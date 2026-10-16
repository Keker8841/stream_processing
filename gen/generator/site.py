"""Seeded site layout generation, validation and YAML persistence."""

from __future__ import annotations

import itertools
import math
import random
from collections.abc import Sequence
from pathlib import Path

import yaml

from .constants import DECISION_RADIUS_M, STATION_RADIUS_M
from .geo import Projection, projection_for
from .model import Site, Station, UnloadPoint

MIN_STATIONS = 4
COORD_DECIMALS = 6           # precision of coordinates stored in site files
MAX_PLACEMENT_ATTEMPTS = 10_000
MAX_POINT_ATTEMPTS = 200       # samples per station inside one pit before the pit is rejected


def generate_site(seed: int, *, center: tuple[float, float] = (49.1288, 142.6601),
                  split: Sequence[int] = (10, 10, 10, 10),
                  unload_dist_m: tuple[float, float] = (3000.0, 10000.0),
                  min_station_gap_m: float = 800.0,
                  pit_radius_m: float = 1500.0,
                  max_station_gap_m: float = 3500.0,
                  min_unload_spread_m: float = 1000.0) -> Site:
    """Unload point at the center; stations S1..Sn are excavators in one pit; units assigned in blocks.

    The pit center lies at a random bearing and distance from the unload point. Every station is placed uniformly
    within `pit_radius_m` of it such that its distance to the unload point is within `unload_dist_m`, stations are
    pairwise >= `min_station_gap_m` and <= `max_station_gap_m` apart, and the distances to the unload point differ by
    at least `min_unload_spread_m` between the nearest and the farthest station. Rejection sampling, seeded.
    """
    lo, hi = unload_dist_m
    if not 0 <= lo <= hi:
        raise ValueError(f"bad unload_dist_m {unload_dist_m}")
    if len(split) < MIN_STATIONS or any(n < 0 for n in split):
        raise ValueError(f"split needs >= {MIN_STATIONS} non-negative entries, got {tuple(split)}")
    if hi <= DECISION_RADIUS_M:
        raise ValueError(f"unload_dist_m {unload_dist_m}: stations must be > {DECISION_RADIUS_M:.0f} m from unload")
    if not 0 < pit_radius_m:
        raise ValueError(f"pit_radius_m must be > 0, got {pit_radius_m}")
    if min_station_gap_m > min(max_station_gap_m, 2 * pit_radius_m):
        raise ValueError(f"min_station_gap_m {min_station_gap_m} exceeds max_station_gap_m {max_station_gap_m} "
                         f"or the pit diameter {2 * pit_radius_m}")
    if min_unload_spread_m > min(hi - lo, 2 * pit_radius_m):
        raise ValueError(f"min_unload_spread_m {min_unload_spread_m} exceeds the unload_dist_m range "
                         f"or the pit diameter")

    rng = random.Random(f"{seed}:site")
    lat0, lon0 = round(center[0], COORD_DECIMALS), round(center[1], COORD_DECIMALS)
    proj = Projection(lat0, lon0)
    unload = UnloadPoint("U1", lat0, lon0)
    station_ids = [f"S{i}" for i in range(1, len(split) + 1)]
    assignment = _block_assignment(split, station_ids)
    lo_ok = max(lo, DECISION_RADIUS_M + 1.0)

    for _ in range(MAX_PLACEMENT_ATTEMPTS):
        bearing = rng.uniform(0.0, 2 * math.pi)
        dist = rng.uniform(lo, hi)
        pcx, pcy = dist * math.sin(bearing), dist * math.cos(bearing)
        points: list[tuple[float, float]] = []
        for _ in station_ids:
            for _ in range(MAX_POINT_ATTEMPTS):
                r = pit_radius_m * math.sqrt(rng.random())
                a = rng.uniform(0.0, 2 * math.pi)
                x, y = pcx + r * math.sin(a), pcy + r * math.cos(a)
                if not lo_ok <= math.hypot(x, y) <= hi:
                    continue
                if all(min_station_gap_m <= math.hypot(x - px, y - py) <= max_station_gap_m for px, py in points):
                    points.append((x, y))
                    break
            else:
                break
        if len(points) < len(station_ids):
            continue
        stations = []
        for sid, (x, y) in zip(station_ids, points):
            lat, lon = proj.to_latlon(x, y)
            stations.append(Station(sid, round(lat, COORD_DECIMALS), round(lon, COORD_DECIMALS)))
        site = Site(lat0, lon0, unload, tuple(stations), assignment)
        if validate_site(site, min_station_gap_m=min_station_gap_m, max_station_gap_m=max_station_gap_m):
            continue
        to_unload = unload_distances(site)
        if max(to_unload.values()) - min(to_unload.values()) >= min_unload_spread_m:
            return site
    raise ValueError(f"cannot place {len(split)} stations with unload_dist_m={unload_dist_m}, "
                     f"pit_radius_m={pit_radius_m}, station gap [{min_station_gap_m}, {max_station_gap_m}], "
                     f"min_unload_spread_m={min_unload_spread_m} after {MAX_PLACEMENT_ATTEMPTS} attempts")


def unload_distances(site: Site) -> dict[str, float]:
    """Straight-line distance (m) of every station center to the unload point."""
    proj = projection_for(site)
    u = site.unload
    return {s.uuid: proj.distance_m(s.lat, s.lon, u.lat, u.lon) for s in site.stations}


def pairwise_distances(site: Site) -> dict[tuple[str, str], float]:
    """Straight-line distance (m) between every pair of station centers."""
    proj = projection_for(site)
    return {(a.uuid, b.uuid): proj.distance_m(a.lat, a.lon, b.lat, b.lon)
            for a, b in itertools.combinations(site.stations, 2)}


def _block_assignment(split: Sequence[int], station_ids: Sequence[str]) -> dict[str, str]:
    """T-01.. assigned in consecutive blocks: first split[0] units -> S1, next split[1] -> S2, ..."""
    total = sum(split)
    width = max(2, len(str(total)))
    unit_ids = (f"T-{i:0{width}d}" for i in range(1, total + 1))
    return {uid: sid for sid, n in zip(station_ids, split) for uid in itertools.islice(unit_ids, n)}


def validate_site(site: Site, *, min_station_gap_m: float = 0.0, max_station_gap_m: float | None = None) -> list[str]:
    """Human-readable problems, empty list = ok. Stations are always required to be > 2 radii apart."""
    problems: list[str] = []
    ids = [s.uuid for s in site.stations]
    if len(ids) < MIN_STATIONS:
        problems.append(f"need at least {MIN_STATIONS} stations, got {len(ids)}")
    dups = sorted({i for i in ids if ids.count(i) > 1})
    if dups:
        problems.append(f"duplicate station ids: {', '.join(dups)}")

    proj = projection_for(site)
    u = site.unload
    for s in site.stations:
        d = proj.distance_m(s.lat, s.lon, u.lat, u.lon)
        if d <= DECISION_RADIUS_M:
            problems.append(f"station {s.uuid} is {d:.1f} m from unload {u.uuid}, must be > {DECISION_RADIUS_M:.0f} m")

    min_sep = 2 * STATION_RADIUS_M
    for a, b in itertools.combinations(site.stations, 2):
        d = proj.distance_m(a.lat, a.lon, b.lat, b.lon)
        if d <= min_sep or d < min_station_gap_m:
            problems.append(f"stations {a.uuid} and {b.uuid} are {d:.1f} m apart, must be "
                            f"> {min_sep:.0f} m and >= {min_station_gap_m:.0f} m")
        elif max_station_gap_m is not None and d > max_station_gap_m:
            problems.append(f"stations {a.uuid} and {b.uuid} are {d:.1f} m apart, must be "
                            f"<= {max_station_gap_m:.0f} m")

    if not site.assignment:
        problems.append("no units assigned")
    known = set(ids)
    for unit, sid in site.assignment.items():
        if sid not in known:
            problems.append(f"unit {unit} assigned to unknown station {sid}")
    return problems


class _Flow(dict):
    """Marker: dump this mapping in YAML flow style ({k: v, ...})."""


class _SiteDumper(yaml.SafeDumper):
    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
        super().increase_indent(flow, False)  # indent list items under their key


_SiteDumper.add_representer(
    _Flow, lambda dumper, data: dumper.represent_mapping("tag:yaml.org,2002:map", data.items(), flow_style=True))


def _c(v: float) -> float:
    return round(float(v), COORD_DECIMALS)


def save_site(site: Site, path: str | Path) -> None:
    doc = {
        "center": _Flow(lat=_c(site.center_lat), lon=_c(site.center_lon)),
        "unload": _Flow(uuid=site.unload.uuid, lat=_c(site.unload.lat), lon=_c(site.unload.lon)),
        "stations": [_Flow(uuid=s.uuid, lat=_c(s.lat), lon=_c(s.lon)) for s in site.stations],
        "assignment": dict(site.assignment),
    }
    text = yaml.dump(doc, Dumper=_SiteDumper, sort_keys=False, default_flow_style=False, width=1000)
    Path(path).write_text(text, encoding="utf-8")


def load_site(path: str | Path) -> Site:
    doc = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    try:
        u = doc["unload"]
        return Site(
            center_lat=float(doc["center"]["lat"]),
            center_lon=float(doc["center"]["lon"]),
            unload=UnloadPoint(str(u["uuid"]), float(u["lat"]), float(u["lon"])),
            stations=tuple(Station(str(s["uuid"]), float(s["lat"]), float(s["lon"])) for s in doc["stations"]),
            assignment={str(k): str(v) for k, v in doc["assignment"].items()},
        )
    except (KeyError, TypeError, AttributeError) as e:
        raise ValueError(f"malformed site file {path}: {e!r}") from e
