"""Site layout in the generator's site.yaml format: unload point, stations, unit -> own station."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml

_KEYS = frozenset({"center", "unload", "stations", "assignment"})   # center is the generator's projection origin


@dataclass(frozen=True)
class Point:
    uuid: str
    lat: float
    lon: float


@dataclass(frozen=True)
class Site:
    unload: Point
    stations: tuple[Point, ...]          # sorted by uuid
    assignment: Mapping[str, str]        # unit -> own station

    def __post_init__(self) -> None:
        stations = tuple(sorted(self.stations, key=lambda s: s.uuid))
        ids = {s.uuid for s in stations}
        if not stations or len(ids) != len(stations):
            raise ValueError("station uuids must be non-empty and unique")
        bad = sorted(u for u, s in self.assignment.items() if s not in ids)
        if bad:
            raise ValueError(f"units assigned to unknown stations: {', '.join(bad)}")
        object.__setattr__(self, "stations", stations)
        object.__setattr__(self, "assignment", MappingProxyType(dict(sorted(self.assignment.items()))))

    def station(self, uuid: str) -> Point:
        for s in self.stations:
            if s.uuid == uuid:
                return s
        raise KeyError(uuid)


def _point(d: Any) -> Point:
    return Point(str(d["uuid"]), float(d["lat"]), float(d["lon"]))


def site_from_dict(d: Mapping[str, Any]) -> Site:
    if not isinstance(d, Mapping):
        raise ValueError("site must be a mapping")
    unknown = sorted(map(str, d.keys() - _KEYS))
    if unknown:
        raise ValueError(f"site: unknown key(s) {', '.join(unknown)}")
    try:
        return Site(unload=_point(d["unload"]),
                    stations=tuple(_point(s) for s in d["stations"]),
                    assignment={str(k): str(v) for k, v in d["assignment"].items()})
    except (KeyError, TypeError, AttributeError) as e:
        raise ValueError(f"malformed site: missing or bad {e}") from None


def load_site(path: str | Path) -> Site:
    try:
        return site_from_dict(yaml.safe_load(Path(path).read_text(encoding="utf-8")))
    except ValueError as e:
        raise ValueError(f"{path}: {e}") from None
