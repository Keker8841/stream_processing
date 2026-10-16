"""Immutable domain types shared by all generator modules."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Literal, get_args


class FrozenMap(Mapping[str, str]):
    """Immutable, hashable str -> str mapping with keys kept in sorted order."""

    __slots__ = ("_d",)

    def __init__(self, items: Mapping[str, str] | Iterable[tuple[str, str]] = ()) -> None:
        self._d = dict(sorted(dict(items).items()))

    def __getitem__(self, key: str) -> str:
        return self._d[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._d)

    def __len__(self) -> int:
        return len(self._d)

    def __contains__(self, key: object) -> bool:
        return key in self._d

    def __hash__(self) -> int:
        return hash(tuple(self._d.items()))

    def __repr__(self) -> str:
        return f"FrozenMap({self._d!r})"


@dataclass(frozen=True)
class Station:
    uuid: str
    lat: float
    lon: float


@dataclass(frozen=True)
class UnloadPoint:
    uuid: str
    lat: float
    lon: float


@dataclass(frozen=True)
class Site:
    center_lat: float            # projection origin
    center_lon: float
    unload: UnloadPoint
    stations: tuple[Station, ...]            # ordered by uuid
    assignment: Mapping[str, str]            # unit_uuid -> station_uuid

    def __post_init__(self) -> None:
        # Normalize so that equal sites compare and hash equal regardless of input order.
        object.__setattr__(self, "stations", tuple(sorted(self.stations, key=lambda s: s.uuid)))
        object.__setattr__(self, "assignment", FrozenMap(self.assignment))

    def station(self, uuid: str) -> Station:
        for s in self.stations:
            if s.uuid == uuid:
                return s
        raise KeyError(f"unknown station {uuid!r}")

    def station_of(self, unit_uuid: str) -> Station:
        """Own (assigned) station of a unit."""
        return self.station(self.assignment[unit_uuid])

    @property
    def station_ids(self) -> list[str]:
        return [s.uuid for s in self.stations]

    @property
    def unit_ids(self) -> list[str]:
        return sorted(self.assignment)


@dataclass(frozen=True)
class Telemetry:
    unit_uuid: str
    ts: int
    lat: float
    lon: float
    speed_kmh: float

    def to_dict(self) -> dict:
        return {"unit_uuid": self.unit_uuid, "ts": self.ts, "lat": self.lat, "lon": self.lon,
                "speed_kmh": self.speed_kmh}

    @staticmethod
    def from_dict(d: dict) -> Telemetry:
        return Telemetry(unit_uuid=str(d["unit_uuid"]), ts=int(d["ts"]), lat=float(d["lat"]),
                         lon=float(d["lon"]), speed_kmh=float(d["speed_kmh"]))


@dataclass(frozen=True)
class Emission:
    emit_at: int                 # simulation second when the record is handed to the sink
    key: str                     # unit_uuid
    value: dict                  # Telemetry.to_dict()


TRUTH_EVENTS = (
    "radius_enter",      # first tick with distance to station center < 50 m (station_uuid)
    "service_start",     # first standing tick of service (station_uuid)
    "depart",            # tick t+service_s, unit moved away (station_uuid)
    "radius_exit",       # first tick with distance >= 50 m after depart (station_uuid)
    "unload",            # unload arrival tick
    "redirect_applied",  # detail: {"from_station", "to_station"}
    "redirect_ignored",  # detail: {"to_station", "reason"}
)


@dataclass(frozen=True)
class TruthEvent:
    ts: int
    unit_uuid: str
    event: str                   # see TRUTH_EVENTS
    station_uuid: str | None = None
    detail: Mapping[str, Any] | None = None

    def to_dict(self) -> dict:
        d: dict[str, Any] = {"ts": self.ts, "unit_uuid": self.unit_uuid, "event": self.event}
        if self.station_uuid is not None:
            d["station_uuid"] = self.station_uuid
        if self.detail is not None:
            d["detail"] = dict(self.detail)
        return d

    @staticmethod
    def from_dict(d: dict) -> TruthEvent:
        return TruthEvent(ts=int(d["ts"]), unit_uuid=str(d["unit_uuid"]), event=str(d["event"]),
                          station_uuid=d.get("station_uuid"), detail=d.get("detail"))


@dataclass(frozen=True)
class ScriptedDelay:
    """Scenario fault: records of `unit_uuid` with from_ts <= ts <= to_ts are handed out at `emit_at`, after the
    records of later ticks that are already out (out-of-order delivery). emit_at - from_ts <= MAX_LATENESS_S."""
    unit_uuid: str
    from_ts: int
    to_ts: int
    emit_at: int

    def covers(self, unit_uuid: str, ts: int) -> bool:
        return unit_uuid == self.unit_uuid and self.from_ts <= ts <= self.to_ts


@dataclass(frozen=True)
class Redirect:
    unit_uuid: str
    to_station: str
    at: int                      # decision event time, epoch seconds


UnitPhase = Literal["to_station", "waiting", "in_service", "to_unload"]
UNIT_PHASES: tuple[str, ...] = get_args(UnitPhase)


@dataclass(frozen=True)
class UnitInit:
    """Initial state of one unit (scenario mode). Missing units start at the unload point heading to their station."""
    unit_uuid: str
    phase: UnitPhase
    target_station: str | None = None   # to_station/waiting/in_service: station; None = own station
    lat: float | None = None            # to_station/to_unload: current position (required for those phases)
    lon: float | None = None
    service_elapsed_s: int = 0          # in_service: seconds already served at start
    radius_enter_ts: int | None = None  # waiting: FIFO key; default = start ts
