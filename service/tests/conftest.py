"""Engine test kit (fixture `world`): a small hand-made site and a telemetry feeder.

Positions are placed by great-circle distance/bearing from a station (or the unload point), so the haversine
distance to that point is exact up to float noise.
"""

from __future__ import annotations

import math

import pytest

from vqservice.config import EngineConfig
from vqservice.engine import Engine, Output
from vqservice.geo import EARTH_RADIUS_M
from vqservice.site import Point, Site
from vqservice.timefmt import parse_iso

T0 = parse_iso("2026-09-15T12:00:00Z")
WEST = 270.0                  # units approach S1 from the west, the unload point is 5 km west of S1


def dest(lat: float, lon: float, dist_m: float, bearing_deg: float) -> tuple[float, float]:
    d = dist_m / EARTH_RADIUS_M
    th = math.radians(bearing_deg)
    p1, l1 = math.radians(lat), math.radians(lon)
    p2 = math.asin(math.sin(p1) * math.cos(d) + math.cos(p1) * math.sin(d) * math.cos(th))
    l2 = l1 + math.atan2(math.sin(th) * math.sin(d) * math.cos(p1), math.cos(d) - math.sin(p1) * math.sin(p2))
    return math.degrees(p2), math.degrees(l2)


def example_stations(p_dist: float = 1500.0, s2_dist: float = 2400.0,
                     s4: tuple[float, float] = (5000.0, 126.87)) -> dict[str, tuple[float, float]]:
    """Task example geometry: P lies `p_dist` west of S1, S2 is `s2_dist` south of P, S3 5000 m from P (NE),
    S4 at (distance, bearing) from P (default 5000 m SE)."""
    s1 = (49.0, 142.0)
    p = dest(*s1, p_dist, WEST)
    return {"S1": s1, "S2": dest(*p, s2_dist, 180.0), "S3": dest(*p, 5000.0, 53.13), "S4": dest(*p, *s4)}


class World:
    T0 = T0
    WEST = WEST
    dest = staticmethod(dest)
    example_stations = staticmethod(example_stations)

    def __init__(self, stations: dict[str, tuple[float, float]] | None = None,
                 units: tuple[str, ...] = ("A", "B", "C", "T1", "T2", "T3", "T4", "X"),
                 assignment: dict[str, str] | None = None, unload_m: float = 5000.0, **cfg) -> None:
        stations = stations or example_stations()
        unload = dest(*stations["S1"], unload_m, WEST)
        self.site = Site(Point("U1", *unload), tuple(Point(k, *v) for k, v in stations.items()),
                         assignment or {u: "S1" for u in units})
        self.cfg = EngineConfig(**cfg)
        self.engine = Engine(self.site, self.cfg)
        self.log: list[Output] = []

    def pos(self, dist: float, bearing: float = WEST, ref: str = "S1") -> tuple[float, float]:
        p = self.site.unload if ref == "U1" else self.site.station(ref)
        return dest(p.lat, p.lon, dist, bearing)

    def send(self, unit: str, ts: int, pos: tuple[float, float], speed: float = 36.0) -> list[Output]:
        out = self.engine.process({"unit_uuid": unit, "ts": ts, "lat": pos[0], "lon": pos[1], "speed_kmh": speed})
        self.log.extend(out)
        return out

    def move(self, unit: str, ts: int, dist: float, bearing: float = WEST, ref: str = "S1") -> list[Output]:
        return self.send(unit, ts, self.pos(dist, bearing, ref), 36.0)

    def stand(self, unit: str, ts: int, dist: float = 15.0, bearing: float = WEST, ref: str = "S1") -> list[Output]:
        return self.send(unit, ts, self.pos(dist, bearing, ref), 0.0)

    def drive(self, unit: str, ts: int, d_from: float, d_to: float, step: float = 10.0,
              bearing: float = WEST, ref: str = "S1") -> list[Output]:
        """One message per second from d_from to d_to (inclusive) toward/away from `ref`; returns all outputs."""
        out = []
        n = round(abs(d_to - d_from) / step)
        sign = -1.0 if d_to < d_from else 1.0
        for k in range(n + 1):
            out += self.move(unit, ts + k, d_from + sign * step * k, bearing, ref)
        return out

    def q(self, station: str) -> list[tuple]:
        """Current queue as (unit, eta|None, service_start, free_at, wait) in epoch seconds."""
        return [(i["unit_uuid"], None if i["eta"] is None else parse_iso(i["eta"]), parse_iso(i["service_start"]),
                 parse_iso(i["free_at"]), i["wait_seconds"]) for i in self.engine.queues()[station]]

    def decisions(self) -> list[Output]:
        return [o for o in self.log if o.kind == "decision"]

    def published(self, station: str) -> list[Output]:
        return [o for o in self.log if o.kind == "queue" and o.key == station]


@pytest.fixture
def world() -> type[World]:
    return World
