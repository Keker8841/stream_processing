"""Scenario files: a site, a start time and explicit initial unit states.

YAML format (unknown keys are rejected to catch typos)::

    name: task_example                   # required
    description: free text               # optional
    start_ts: "2026-09-15T11:59:00Z"     # required: ISO-8601 with a UTC offset, or epoch seconds
    coord_precision: 6                   # optional
    duration_s: 960                      # optional
    service_s: 200                       # optional, generator service time for this scenario
    maneuver_s: 30                       # optional, generator maneuver gap for this scenario
    site_file: ../site.yaml              # either a site file (relative to this file) ...
    site:                                # ... or an inline site in the site.yaml format
      center: {lat: 49.1293, lon: 142.6601}
      unload: {uuid: U1, lat: 49.1293, lon: 142.591382}
      stations:
        - {uuid: S1, lat: 49.1293, lon: 142.6601}
      assignment: {T1: S1}
    units:                               # optional; the site assignment defines the unit set,
      - {unit_uuid: T1, phase: in_service, service_elapsed_s: 0}       # unlisted units start at the unload
      - {unit_uuid: T2, phase: to_station, position: {from_station: S1, distance_m: 1800, toward: U1}}
      - {unit_uuid: T3, phase: to_unload, position: {lat: 49.12, lon: 142.62}}
      - {unit_uuid: T4, phase: waiting, target_station: S2, radius_enter_ts: "2026-09-15T11:58:30Z"}
    delays:                              # optional scripted out-of-order delivery, on top of the random faults
      - {unit_uuid: T2, from_ts: "2026-09-15T11:59:01Z", to_ts: "2026-09-15T11:59:59Z",
         emit_at: "2026-09-15T12:00:01Z"}  # records of these ticks arrive at emit_at, after the newer ones
    expect: {...}                        # optional free-form mapping, passed through

Fields per phase (anything else is an error):
    to_station  position (required), target_station (default: own station)
    waiting     target_station, radius_enter_ts (default: start_ts, must be <= start_ts)
    in_service  target_station, service_elapsed_s (default 0); at most one unit in service per station
    to_unload   position (required)

A position is ``{lat, lon}`` (also accepted as ``lat``/``lon`` directly on the unit entry) or
``{from_station, distance_m, toward}``: the point ``distance_m`` from the station center on the straight
line toward the unload point / another station (by uuid) / a bearing in degrees (0 = north, 90 = east).
Positions are the state before the first tick (the simulator's ``now == start_ts - 1``): a moving unit's first
record (ts = start_ts) is already 10 m further along its leg.

A delay needs start_ts <= from_ts <= to_ts < emit_at <= from_ts + 60 (lateness at most 60 s).
"""

from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

from .constants import MAX_LATENESS_S, STOP_OFFSET_M
from .geo import Projection, projection_for
from .model import UNIT_PHASES, ScriptedDelay, Site, Station, UnitInit, UnloadPoint
from .site import load_site, validate_site


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    site: Site
    start_ts: int
    initial: tuple[UnitInit, ...]
    coord_precision: int | None = None
    duration_s: int | None = None
    expect: Mapping[str, Any] | None = field(default=None, hash=False)  # free-form expected facts
    service_s: int | None = None      # addition: scenario-specific generator settings (None = runner default)
    maneuver_s: int | None = None
    delays: tuple[ScriptedDelay, ...] = ()


_TOP_KEYS = frozenset({"name", "description", "start_ts", "coord_precision", "duration_s", "service_s",
                       "maneuver_s", "site", "site_file", "units", "delays", "expect"})
_DELAY_KEYS = frozenset({"unit_uuid", "from_ts", "to_ts", "emit_at"})
_SITE_KEYS = frozenset({"center", "unload", "stations", "assignment"})
_UNIT_KEYS = frozenset({"unit_uuid", "phase", "target_station", "position", "lat", "lon",
                        "service_elapsed_s", "radius_enter_ts"})
_PHASE_FIELDS = {
    "to_station": frozenset({"target_station", "position"}),
    "waiting": frozenset({"target_station", "radius_enter_ts"}),
    "in_service": frozenset({"target_station", "service_elapsed_s"}),
    "to_unload": frozenset({"position"}),
}
_NEEDS_POSITION = frozenset({"to_station", "to_unload"})
_LATLON_KEYS = frozenset({"lat", "lon"})
_RELATIVE_KEYS = frozenset({"from_station", "distance_m", "toward"})


def parse_ts(value: Any) -> int:
    """Epoch seconds from an int, a digit string, an ISO-8601 string with a UTC offset or an aware datetime."""
    if isinstance(value, bool):
        raise ValueError(f"bad timestamp {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if re.fullmatch(r"-?[0-9]+", text):
            return int(text)
        try:
            value = datetime.fromisoformat(text)
        except ValueError:
            raise ValueError(f"bad timestamp {text!r}: expected ISO-8601 like 2026-09-15T12:00:00Z "
                             f"or epoch seconds") from None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"timestamp {value.isoformat()} has no timezone, add 'Z' or '+00:00'")
        return math.floor(value.timestamp())
    if isinstance(value, date):
        raise ValueError(f"timestamp {value.isoformat()} is a date without a time")
    raise ValueError(f"bad timestamp {value!r}: expected ISO-8601 string or epoch seconds")


def load_scenario(path: str | Path) -> Scenario:
    path = Path(path)
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ValueError(f"{path}: invalid YAML: {e}") from e
    try:
        return _build(doc, path.parent)
    except ValueError as e:
        raise ValueError(f"{path}: {e}") from None


def _build(doc: Any, base_dir: Path) -> Scenario:
    if not isinstance(doc, dict):
        raise ValueError("scenario must be a YAML mapping")
    _check_keys(doc, _TOP_KEYS, "scenario")
    name = _str(doc.get("name"), "name")
    description = doc.get("description") or ""
    if not isinstance(description, str):
        raise ValueError("description must be a string")
    if "start_ts" not in doc:
        raise ValueError("start_ts is required")
    start_ts = _field(parse_ts, doc["start_ts"], "start_ts")
    coord_precision = _opt_int(doc, "coord_precision", 0, 12)
    duration_s = _opt_int(doc, "duration_s", 1)
    service_s = _opt_int(doc, "service_s", 1)
    maneuver_s = _opt_int(doc, "maneuver_s", 0)

    site = _scenario_site(doc, base_dir)
    problems = validate_site(site)
    if problems:
        raise ValueError("invalid site: " + "; ".join(problems))

    units = doc.get("units")
    if units is None:
        units = []
    if not isinstance(units, list):
        raise ValueError("units must be a list")
    initial = _parse_units(units, site, start_ts, service_s)
    delays = _parse_delays(doc.get("delays"), site, start_ts)

    expect = doc.get("expect")
    if expect is not None and not isinstance(expect, dict):
        raise ValueError("expect must be a mapping")
    return Scenario(name=name, description=description, site=site, start_ts=start_ts, initial=initial,
                    coord_precision=coord_precision, duration_s=duration_s, expect=expect,
                    service_s=service_s, maneuver_s=maneuver_s, delays=delays)


def _scenario_site(doc: dict, base_dir: Path) -> Site:
    has_inline, has_file = "site" in doc, "site_file" in doc
    if has_inline == has_file:
        raise ValueError("exactly one of 'site' (inline) or 'site_file' is required")
    if has_file:
        site_path = base_dir / _str(doc["site_file"], "site_file")
        if not site_path.is_file():
            raise ValueError(f"site_file not found: {site_path}")
        try:
            return load_site(site_path)
        except yaml.YAMLError as e:
            raise ValueError(f"site_file {site_path}: invalid YAML: {e}") from e
    return _inline_site(doc["site"])


def _inline_site(d: Any) -> Site:
    if not isinstance(d, dict):
        raise ValueError("site must be a mapping (center, unload, stations, assignment)")
    _check_keys(d, _SITE_KEYS, "site")
    for key in sorted(_SITE_KEYS):
        if key not in d:
            raise ValueError(f"site.{key} is required")
    center = _latlon(d["center"], "site.center")
    u = d["unload"]
    if not isinstance(u, dict):
        raise ValueError("site.unload must be a mapping {uuid, lat, lon}")
    unload = UnloadPoint(_str(u.get("uuid"), "site.unload.uuid"), *_latlon(u, "site.unload", extra={"uuid"}))
    if not isinstance(d["stations"], list):
        raise ValueError("site.stations must be a list")
    stations = []
    for i, s in enumerate(d["stations"]):
        where = f"site.stations[{i}]"
        if not isinstance(s, dict):
            raise ValueError(f"{where} must be a mapping {{uuid, lat, lon}}")
        stations.append(Station(_str(s.get("uuid"), f"{where}.uuid"), *_latlon(s, where, extra={"uuid"})))
    assignment = d["assignment"]
    if not isinstance(assignment, dict):
        raise ValueError("site.assignment must be a mapping unit_uuid -> station_uuid")
    return Site(center_lat=center[0], center_lon=center[1], unload=unload, stations=tuple(stations),
                assignment={_str(k, "site.assignment key"): _str(v, f"site.assignment.{k}")
                            for k, v in assignment.items()})


def _parse_units(entries: list, site: Site, start_ts: int, service_s: int | None) -> tuple[UnitInit, ...]:
    proj = projection_for(site)
    station_ids = set(site.station_ids)
    seen: set[str] = set()
    result = []
    for i, entry in enumerate(entries):
        where = f"units[{i}]"
        if not isinstance(entry, dict):
            raise ValueError(f"{where} must be a mapping")
        unit = _str(entry.get("unit_uuid"), f"{where}.unit_uuid")
        where = f"units[{i}] ({unit})"
        _check_keys(entry, _UNIT_KEYS, where)
        if unit not in site.assignment:
            raise ValueError(f"{where}: unit is not in the site assignment")
        if unit in seen:
            raise ValueError(f"{where}: unit listed twice")
        seen.add(unit)
        phase = entry.get("phase")
        if phase not in UNIT_PHASES:
            raise ValueError(f"{where}: phase must be one of {', '.join(UNIT_PHASES)}, got {phase!r}")

        fields = dict(entry)
        if _LATLON_KEYS & fields.keys():
            if "position" in fields:
                raise ValueError(f"{where}: give either 'position' or 'lat'/'lon', not both")
            fields["position"] = {k: fields.pop(k) for k in _LATLON_KEYS if k in fields}
        extra = sorted(fields.keys() - {"unit_uuid", "phase"} - _PHASE_FIELDS[phase])
        if extra:
            raise ValueError(f"{where}: {', '.join(extra)} not allowed for phase {phase}")

        target = None
        if "target_station" in fields:
            target = _str(fields["target_station"], f"{where}.target_station")
            if target not in station_ids:
                raise ValueError(f"{where}: unknown target_station {target!r}")
            if target == site.assignment[unit]:
                target = None                     # own station, same meaning as omitted

        lat = lon = None
        if phase in _NEEDS_POSITION:
            if "position" not in fields:
                raise ValueError(f"{where}: position is required for phase {phase}")
            lat, lon = _position(fields["position"], site, proj, f"{where}.position")
            if phase == "to_station":
                st = site.station(target or site.assignment[unit])
                d = proj.distance_m(lat, lon, st.lat, st.lon)
                if d <= STOP_OFFSET_M:
                    raise ValueError(f"{where}: position is {d:.1f} m from {st.uuid}, must be > "
                                     f"{STOP_OFFSET_M:.0f} m for to_station (use waiting/in_service)")

        elapsed = 0
        if "service_elapsed_s" in fields:
            elapsed = fields["service_elapsed_s"]
            if isinstance(elapsed, bool) or not isinstance(elapsed, int) or elapsed < 0:
                raise ValueError(f"{where}: service_elapsed_s must be an integer >= 0, got {elapsed!r}")
            if service_s is not None and elapsed >= service_s:
                raise ValueError(f"{where}: service_elapsed_s {elapsed} must be < service_s {service_s}")

        enter_ts = None
        if "radius_enter_ts" in fields:
            enter_ts = _field(parse_ts, fields["radius_enter_ts"], f"{where}.radius_enter_ts")
            if enter_ts > start_ts:
                raise ValueError(f"{where}: radius_enter_ts {enter_ts} is after start_ts {start_ts}")

        result.append(UnitInit(unit_uuid=unit, phase=phase, target_station=target, lat=lat, lon=lon,
                               service_elapsed_s=elapsed, radius_enter_ts=enter_ts))

    in_service = Counter(u.target_station or site.assignment[u.unit_uuid] for u in result if u.phase == "in_service")
    busy = sorted(s for s, n in in_service.items() if n > 1)
    if busy:
        raise ValueError(f"more than one unit in_service at station(s) {', '.join(busy)}")
    return tuple(result)


def _parse_delays(entries: Any, site: Site, start_ts: int) -> tuple[ScriptedDelay, ...]:
    if entries is None:
        return ()
    if not isinstance(entries, list):
        raise ValueError("delays must be a list")
    out = []
    for i, entry in enumerate(entries):
        where = f"delays[{i}]"
        if not isinstance(entry, dict):
            raise ValueError(f"{where} must be a mapping")
        _check_keys(entry, _DELAY_KEYS, where)
        missing = sorted(_DELAY_KEYS - entry.keys())
        if missing:
            raise ValueError(f"{where}: missing {', '.join(missing)}")
        unit = _str(entry["unit_uuid"], f"{where}.unit_uuid")
        if unit not in site.assignment:
            raise ValueError(f"{where}: unit {unit!r} is not in the site assignment")
        d = ScriptedDelay(unit, *(_field(parse_ts, entry[k], f"{where}.{k}") for k in ("from_ts", "to_ts", "emit_at")))
        if not start_ts <= d.from_ts <= d.to_ts < d.emit_at <= d.from_ts + MAX_LATENESS_S:
            raise ValueError(f"{where}: need start_ts <= from_ts <= to_ts < emit_at <= from_ts + {MAX_LATENESS_S}")
        if any(o.unit_uuid == unit and o.from_ts <= d.to_ts and d.from_ts <= o.to_ts for o in out):
            raise ValueError(f"{where}: overlaps another delay of {unit}")
        out.append(d)
    return tuple(out)


def _position(spec: Any, site: Site, proj: Projection, where: str) -> tuple[float, float]:
    if not isinstance(spec, dict):
        raise ValueError(f"{where} must be a mapping")
    keys = spec.keys()
    if keys == _LATLON_KEYS:
        return _latlon(spec, where)
    if keys != _RELATIVE_KEYS:
        raise ValueError(f"{where} must be {{lat, lon}} or {{from_station, distance_m, toward}}, "
                         f"got keys {sorted(map(str, keys))}")
    sid = _str(spec["from_station"], f"{where}.from_station")
    if sid not in site.station_ids:
        raise ValueError(f"{where}: unknown from_station {sid!r}")
    dist = _num(spec["distance_m"], f"{where}.distance_m")
    if dist < 0:
        raise ValueError(f"{where}.distance_m must be >= 0, got {dist}")
    st = site.station(sid)
    x0, y0 = proj.to_xy(st.lat, st.lon)
    toward = spec["toward"]
    if isinstance(toward, str):
        points = {s.uuid: (s.lat, s.lon) for s in site.stations if s.uuid != sid}
        points.setdefault(site.unload.uuid, (site.unload.lat, site.unload.lon))
        if toward == sid:
            raise ValueError(f"{where}.toward must differ from from_station {sid!r}")
        if toward not in points:
            raise ValueError(f"{where}: unknown toward point {toward!r} (unload, station or bearing in degrees)")
        x1, y1 = proj.to_xy(*points[toward])
        norm = math.hypot(x1 - x0, y1 - y0)
        ux, uy = (x1 - x0) / norm, (y1 - y0) / norm   # validate_site guarantees norm > 100 m
    else:
        bearing = math.radians(_num(toward, f"{where}.toward"))
        ux, uy = math.sin(bearing), math.cos(bearing)
    return proj.to_latlon(x0 + dist * ux, y0 + dist * uy)


def _latlon(d: Any, where: str, extra: set[str] | frozenset[str] = frozenset()) -> tuple[float, float]:
    if not isinstance(d, dict):
        raise ValueError(f"{where} must be a mapping with lat and lon")
    _check_keys(d, _LATLON_KEYS | extra, where)
    if not _LATLON_KEYS <= d.keys():
        raise ValueError(f"{where} needs both lat and lon")
    lat, lon = _num(d["lat"], f"{where}.lat"), _num(d["lon"], f"{where}.lon")
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        raise ValueError(f"{where}: lat/lon out of range: {lat}, {lon}")
    return lat, lon


def _check_keys(d: dict, allowed: frozenset[str] | set[str], where: str) -> None:
    unknown = sorted(str(k) for k in d.keys() - allowed)
    if unknown:
        raise ValueError(f"{where}: unknown key(s) {', '.join(unknown)} (allowed: {', '.join(sorted(allowed))})")


def _str(v: Any, where: str) -> str:
    if not isinstance(v, str) or not v:
        raise ValueError(f"{where} must be a non-empty string, got {v!r}")
    return v


def _num(v: Any, where: str) -> float:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        raise ValueError(f"{where} must be a finite number, got {v!r}")
    return float(v)


def _opt_int(doc: dict, key: str, lo: int, hi: int | None = None) -> int | None:
    v = doc.get(key)
    if v is None:
        return None
    if isinstance(v, bool) or not isinstance(v, int) or v < lo or (hi is not None and v > hi):
        bounds = f">= {lo}" if hi is None else f"in [{lo}, {hi}]"
        raise ValueError(f"{key} must be an integer {bounds}, got {v!r}")
    return v


def _field(fn, value: Any, where: str):
    try:
        return fn(value)
    except ValueError as e:
        raise ValueError(f"{where}: {e}") from None
