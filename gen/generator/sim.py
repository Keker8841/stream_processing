"""Deterministic tick-based simulation core: units drive between the unload point and loading stations."""

from __future__ import annotations

import bisect
import math
from collections.abc import Sequence

from .constants import (DEFAULT_COORD_PRECISION, MANEUVER_S, SERVICE_S, SPEED_KMH, SPEED_MPS, STATION_RADIUS_M,
                        STOP_OFFSET_M)
from .geo import projection_for
from .model import UNIT_PHASES, Site, Telemetry, TruthEvent, UnitInit, UnitPhase

MIN_STATION_UNLOAD_DIST_M = 2 * STATION_RADIUS_M   # keeps radius_exit strictly before the unload arrival
MAX_COORD_PRECISION = 12

TO_STATION: UnitPhase = "to_station"
WAITING: UnitPhase = "waiting"
IN_SERVICE: UnitPhase = "in_service"
TO_UNLOAD: UnitPhase = "to_unload"

# Fixed order of one unit's events within a tick (the causal order inside step()).
EVENT_ORDER = ("redirect_applied", "redirect_ignored", "depart", "radius_exit", "unload", "radius_enter",
               "service_start")
_RANK_REDIRECT, _RANK_DEPART, _RANK_EXIT, _RANK_UNLOAD, _RANK_ENTER, _RANK_SERVICE = 0, 1, 2, 3, 4, 5

REDIRECT_IGNORE_REASONS = ("unknown_unit", "unknown_station", "same_station", "not_to_station",
                           "already_redirected", "inside_radius")


class _Station:
    __slots__ = ("uuid", "cx", "cy", "ax", "ay", "fifo", "busy", "release")

    def __init__(self, uuid: str, cx: float, cy: float, ax: float, ay: float) -> None:
        self.uuid = uuid
        self.cx = cx
        self.cy = cy
        self.ax = ax                                # default approach direction (unload point -> center)
        self.ay = ay
        self.fifo: list[tuple[int, str]] = []       # (radius_enter tick, unit_uuid), kept sorted
        self.busy: str | None = None                # unit in service
        self.release: int | None = None             # tick of the last departure


class _Unit:
    __slots__ = ("uid", "own", "phase", "target", "redirected", "joined", "exit_from",
                 "sx", "sy", "tx", "ty", "ux", "uy", "length", "traveled", "x", "y",
                 "service_start", "shown_x", "shown_y", "lat", "lon")

    def __init__(self, uid: str, own: str) -> None:
        self.uid = uid
        self.own = own
        self.phase: UnitPhase = TO_STATION
        self.target: str | None = own               # station of the current trip; None while to_unload
        self.redirected = False
        self.joined = False                         # in the FIFO of `target` (radius_enter happened)
        self.exit_from: _Station | None = None      # departed station whose radius_exit is pending
        # current leg: start (sx, sy), end (tx, ty), unit direction (ux, uy); position = start + dir * traveled
        self.sx = self.sy = self.tx = self.ty = 0.0
        self.ux = self.uy = 0.0
        self.length = 0.0
        self.traveled = 0.0
        self.x = self.y = 0.0                       # internal position on the plane
        self.service_start = 0
        self.shown_x = self.shown_y = math.nan      # position of the cached rounded lat/lon
        self.lat = self.lon = 0.0


def _check_int(name: str, v: object, lo: int | None = None, hi: int | None = None) -> None:
    if not isinstance(v, int) or isinstance(v, bool):
        raise ValueError(f"{name} must be an int, got {v!r}")
    if (lo is not None and v < lo) or (hi is not None and v > hi):
        raise ValueError(f"{name}={v} out of range [{lo}, {hi}]")


def _finite(v: object) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


class Simulator:
    """Pure, deterministic 1 Hz simulation of all units of a site (no randomness, no wall clock).

    Units move on the local plane of the site projection at exactly SPEED_MPS per tick along straight legs.
    Every step() produces one Telemetry per unit (lat/lon rounded to `coord_precision`, speed 36.0 on ticks
    where the unit moved, 0.0 on ticks where it stands) plus the truth events of that tick.

    Start state: initial positions describe the state at `now == start_ts - 1`. Without `initial` every unit
    stands at the unload point heading to its own station, and the first step() already moves it: the record at
    tick start_ts is 10 m from the unload point with speed 36.0 (no `unload` event at start). Units given as
    `to_station`/`to_unload` likewise move 10 m from their given position on the first tick. `waiting` and
    `in_service` units stand at the stop point of their station approached from the unload point. Initial
    states produce no truth events of their own (no radius_enter for initial waiting units, no service_start
    for an initial in_service unit even with service_elapsed_s == 0); only transitions simulated from
    start_ts on are reported.

    Legs and stations: a leg to a station ends at the stop point 15 m before the station center along the leg
    direction (start -> center). If the leg starts within 15 m of the center, the unit's previous heading is
    used as the approach direction (or the unload -> center direction when there is none). A unit joins the
    station FIFO on the first tick its distance to its target station center is < 50 m (`radius_enter`);
    order = (radius_enter tick, unit_uuid); strict FIFO. Arrival (remaining <= 10 m) clamps the unit to the
    stop point with speed 0 and drops the rest of the step. The head starts service on the first tick it stands
    at the stop point, the station is free and tick >= release + maneuver_s. Service started at t covers
    records t .. t+service_s-1; at t+service_s the unit has moved 10 m toward the unload point (`depart`) and
    the station is released at that tick. `radius_exit` = first tick after depart with distance >= 50 m.

    Special ticks (displacement between consecutive records may exceed 10 m): on the unload arrival tick the
    record shows exactly the unload point with speed 0 while internally the rest of that tick's 10 m is
    already driven on the leg to the own station, so the following record is up to 20 m from the unload point.
    That keeps the cycle free of an extra second. Records on all other ticks are <= 10 m apart.

    Redirects (request_redirect) are queued and evaluated at the beginning of the next step(), before movement,
    in request order. Accepted only for a `to_station` unit heading to its own station, outside that station's
    50 m radius and not yet redirected on this trip; the unit then drives straight from its current position to
    the recommended station's stop point (`redirect_applied`). Otherwise `redirect_ignored` with one of
    REDIRECT_IGNORE_REASONS. After service at any station the unit drives to unload, then to its own station.

    Truth events of a tick are sorted by unit_uuid, then by EVENT_ORDER (redirect outcomes keep request order).
    """

    def __init__(self, site: Site, *, start_ts: int, service_s: int = SERVICE_S, maneuver_s: int = MANEUVER_S,
                 coord_precision: int = DEFAULT_COORD_PRECISION,
                 initial: Sequence[UnitInit] = ()) -> None:
        _check_int("start_ts", start_ts)
        _check_int("service_s", service_s, 1)
        _check_int("maneuver_s", maneuver_s, 0)
        _check_int("coord_precision", coord_precision, 0, MAX_COORD_PRECISION)
        self.site = site
        self.start_ts = start_ts
        self.service_s = service_s
        self.maneuver_s = maneuver_s
        self.coord_precision = coord_precision
        self._proj = projection_for(site)
        self._now = start_ts - 1
        self._redirect_queue: list[tuple[str, str]] = []

        self._check_site(site)
        self._ux0, self._uy0 = self._proj.to_xy(site.unload.lat, site.unload.lon)
        self._unload_latlon = (round(site.unload.lat, coord_precision), round(site.unload.lon, coord_precision))

        self._stations: list[_Station] = []
        for s in site.stations:
            cx, cy = self._proj.to_xy(s.lat, s.lon)
            d = math.hypot(cx - self._ux0, cy - self._uy0)
            self._stations.append(_Station(s.uuid, cx, cy, (cx - self._ux0) / d, (cy - self._uy0) / d))
        self._st_by_id = {st.uuid: st for st in self._stations}

        self._units = [_Unit(uid, site.assignment[uid]) for uid in site.unit_ids]
        self._by_id = {u.uid: u for u in self._units}
        for u in self._units:
            u.x, u.y = self._ux0, self._uy0
            self._leg_to_station(u, self._st_by_id[u.own])
        self._apply_initial(initial)

    # ----------------------------------------------------------------------------------------------- public API

    @property
    def now(self) -> int:
        """ts of the last produced tick; start_ts - 1 before the first step()."""
        return self._now

    @property
    def unit_ids(self) -> list[str]:
        return [u.uid for u in self._units]

    def unit_phase(self, unit_uuid: str) -> UnitPhase:
        return self._by_id[unit_uuid].phase

    def unit_target(self, unit_uuid: str) -> str:
        """Station of the current trip; the unload point uuid while the unit is `to_unload`."""
        u = self._by_id[unit_uuid]
        return u.target if u.target is not None else self.site.unload.uuid

    def station_queue(self, station_uuid: str) -> list[str]:
        """Units in the station FIFO (radius entered, not yet in service), head first."""
        return [uid for _, uid in self._st_by_id[station_uuid].fifo]

    def station_busy(self, station_uuid: str) -> str | None:
        """Unit currently in service at the station, if any."""
        return self._st_by_id[station_uuid].busy

    def request_redirect(self, unit_uuid: str, to_station: str) -> None:
        """Queue a redirect; evaluated at the beginning of the next step() and reported as a truth event."""
        self._redirect_queue.append((unit_uuid, to_station))

    def step(self) -> tuple[list[Telemetry], list[TruthEvent]]:
        t = self._now + 1
        self._now = t
        ev: list[tuple[str, int, TruthEvent]] = []
        if self._redirect_queue:
            queue, self._redirect_queue = self._redirect_queue, []
            for uid, to in queue:
                self._apply_redirect(t, uid, to, ev)

        records: list[Telemetry] = []
        append = records.append
        service_s = self.service_s
        step_m = SPEED_MPS
        radius = STATION_RADIUS_M
        hypot = math.hypot
        to_latlon = self._proj.to_latlon
        prec = self.coord_precision
        st_by_id = self._st_by_id

        for u in self._units:
            ph = u.phase
            if ph == IN_SERVICE:
                if t - u.service_start < service_s:
                    append(Telemetry(u.uid, t, u.lat, u.lon, 0.0))   # standing: cache is valid
                    continue
                self._depart(t, u, ev)
                ph = TO_UNLOAD
            elif ph == WAITING:
                append(Telemetry(u.uid, t, u.lat, u.lon, 0.0))
                continue

            rem = u.length - u.traveled
            if rem > step_m:
                d = u.traveled + step_m
                u.traveled = d
                x = u.x = u.sx + u.ux * d
                y = u.y = u.sy + u.uy * d
                speed = SPEED_KMH
            elif ph == TO_STATION:
                u.traveled = u.length
                x = u.x = u.tx
                y = u.y = u.ty
                u.phase = WAITING
                speed = 0.0
            else:
                # Unload arrival: show the unload point, carry the rest of the step onto the next leg.
                ev.append((u.uid, _RANK_UNLOAD, TruthEvent(t, u.uid, "unload")))
                u.x, u.y = self._ux0, self._uy0
                u.target = u.own
                u.joined = False
                u.phase = TO_STATION
                self._leg_to_station(u, st_by_id[u.own])
                d = min(step_m - rem, u.length)
                u.traveled = d
                u.x = u.sx + u.ux * d
                u.y = u.sy + u.uy * d
                x, y = self._ux0, self._uy0
                u.shown_x, u.shown_y = x, y
                u.lat, u.lon = self._unload_latlon
                speed = 0.0

            ex = u.exit_from
            if ex is not None and hypot(u.x - ex.cx, u.y - ex.cy) >= radius:
                ev.append((u.uid, _RANK_EXIT, TruthEvent(t, u.uid, "radius_exit", ex.uuid)))
                u.exit_from = None
            if not u.joined and u.target is not None:
                st = st_by_id[u.target]
                if hypot(u.x - st.cx, u.y - st.cy) < radius:
                    u.joined = True
                    bisect.insort(st.fifo, (t, u.uid))
                    ev.append((u.uid, _RANK_ENTER, TruthEvent(t, u.uid, "radius_enter", st.uuid)))

            if x != u.shown_x or y != u.shown_y:
                u.shown_x = x
                u.shown_y = y
                lat, lon = to_latlon(x, y)
                u.lat = round(lat, prec)
                u.lon = round(lon, prec)
            append(Telemetry(u.uid, t, u.lat, u.lon, speed))

        maneuver_s = self.maneuver_s
        for st in self._stations:
            if st.busy is None and st.fifo and (st.release is None or t >= st.release + maneuver_s):
                head = self._by_id[st.fifo[0][1]]
                if head.phase == WAITING:
                    del st.fifo[0]
                    st.busy = head.uid
                    head.phase = IN_SERVICE
                    head.service_start = t
                    ev.append((head.uid, _RANK_SERVICE, TruthEvent(t, head.uid, "service_start", st.uuid)))

        if not ev:
            return records, []
        ev.sort(key=lambda e: (e[0], e[1]))   # stable: redirect outcomes keep request order
        return records, [e[2] for e in ev]

    # ------------------------------------------------------------------------------------------------ internals

    def _leg_to(self, u: _Unit, tx: float, ty: float) -> None:
        """Start a new leg from the current internal position. A zero-length leg keeps the previous heading."""
        dx, dy = tx - u.x, ty - u.y
        length = math.hypot(dx, dy)
        u.sx, u.sy, u.tx, u.ty = u.x, u.y, tx, ty
        if length > 0.0:
            u.ux, u.uy = dx / length, dy / length
        u.length = length
        u.traveled = 0.0

    def _stop_point(self, st: _Station, x: float, y: float, hx: float, hy: float) -> tuple[float, float]:
        dx, dy = st.cx - x, st.cy - y
        d = math.hypot(dx, dy)
        if d > STOP_OFFSET_M:
            ax, ay = dx / d, dy / d
        elif hx or hy:
            ax, ay = hx, hy          # degenerate: leg starts near the center, approach along the previous heading
        else:
            ax, ay = st.ax, st.ay
        return st.cx - STOP_OFFSET_M * ax, st.cy - STOP_OFFSET_M * ay

    def _leg_to_station(self, u: _Unit, st: _Station) -> None:
        self._leg_to(u, *self._stop_point(st, u.x, u.y, u.ux, u.uy))

    def _depart(self, t: int, u: _Unit, ev: list[tuple[str, int, TruthEvent]]) -> None:
        st = self._st_by_id[u.target]  # type: ignore[index]
        st.busy = None
        st.release = t
        ev.append((u.uid, _RANK_DEPART, TruthEvent(t, u.uid, "depart", st.uuid)))
        u.exit_from = st
        u.phase = TO_UNLOAD
        u.target = None
        u.redirected = False
        u.joined = False
        self._leg_to(u, self._ux0, self._uy0)

    def _apply_redirect(self, t: int, uid: str, to: str, ev: list[tuple[str, int, TruthEvent]]) -> None:
        u = self._by_id.get(uid)
        st = self._st_by_id.get(to)
        reason = None
        if u is None:
            reason = "unknown_unit"
        elif st is None:
            reason = "unknown_station"
        elif to == u.own:
            reason = "same_station"
        elif u.phase != TO_STATION:
            reason = "not_to_station"
        elif u.redirected or u.target != u.own:
            reason = "already_redirected"
        else:
            own = self._st_by_id[u.own]
            if u.joined or math.hypot(u.x - own.cx, u.y - own.cy) < STATION_RADIUS_M:
                reason = "inside_radius"
        if reason is not None:
            ev.append((uid, _RANK_REDIRECT,
                       TruthEvent(t, uid, "redirect_ignored", detail={"to_station": to, "reason": reason})))
            return
        assert u is not None and st is not None
        ev.append((uid, _RANK_REDIRECT,
                   TruthEvent(t, uid, "redirect_applied", detail={"from_station": u.own, "to_station": to})))
        u.target = to
        u.redirected = True
        u.joined = False
        self._leg_to_station(u, st)

    def _place_at_stop(self, u: _Unit, st: _Station) -> None:
        """Stand at the stop point of `st` approached from the unload point (initial waiting / in_service)."""
        u.x = u.tx = u.sx = st.cx - STOP_OFFSET_M * st.ax
        u.y = u.ty = u.sy = st.cy - STOP_OFFSET_M * st.ay
        u.ux, u.uy = st.ax, st.ay
        u.length = u.traveled = 0.0
        u.shown_x, u.shown_y = u.x, u.y
        lat, lon = self._proj.to_latlon(u.x, u.y)
        u.lat, u.lon = round(lat, self.coord_precision), round(lon, self.coord_precision)

    @staticmethod
    def _check_site(site: Site) -> None:
        problems: list[str] = []
        ids = [s.uuid for s in site.stations]
        if not ids:
            problems.append("no stations")
        if len(set(ids)) != len(ids):
            problems.append("duplicate station ids")
        if not site.assignment:
            problems.append("no units assigned")
        for unit, sid in site.assignment.items():
            if sid not in ids:
                problems.append(f"unit {unit} assigned to unknown station {sid}")
        coords = [(site.center_lat, site.center_lon), (site.unload.lat, site.unload.lon)]
        coords += [(s.lat, s.lon) for s in site.stations]
        if not all(_finite(a) and _finite(b) for a, b in coords):
            problems.append("non-finite coordinates")
        else:
            proj = projection_for(site)
            for s in site.stations:
                d = proj.distance_m(s.lat, s.lon, site.unload.lat, site.unload.lon)
                if d < MIN_STATION_UNLOAD_DIST_M:
                    problems.append(f"station {s.uuid} is {d:.1f} m from the unload point, "
                                    f"must be >= {MIN_STATION_UNLOAD_DIST_M:.0f} m")
        if problems:
            raise ValueError("invalid site: " + "; ".join(problems))

    def _apply_initial(self, initial: Sequence[UnitInit]) -> None:
        seen: set[str] = set()
        for init in initial:
            if not isinstance(init, UnitInit):
                raise ValueError(f"initial entries must be UnitInit, got {init!r}")
            uid = init.unit_uuid
            where = f"initial state of {uid!r}"
            u = self._by_id.get(uid)
            if u is None:
                raise ValueError(f"{where}: unknown unit")
            if uid in seen:
                raise ValueError(f"{where}: duplicate entry")
            seen.add(uid)
            ph = init.phase
            if ph not in UNIT_PHASES:
                raise ValueError(f"{where}: unknown phase {ph!r}")
            if init.target_station is not None and init.target_station not in self._st_by_id:
                raise ValueError(f"{where}: unknown station {init.target_station!r}")
            if ph == TO_UNLOAD and init.target_station is not None:
                raise ValueError(f"{where}: target_station is not allowed for to_unload")
            has_pos = init.lat is not None or init.lon is not None
            if ph in (TO_STATION, TO_UNLOAD):
                if not (_finite(init.lat) and _finite(init.lon)
                        and -90.0 <= init.lat <= 90.0 and -180.0 <= init.lon <= 180.0):  # type: ignore[operator]
                    raise ValueError(f"{where}: {ph} needs a valid lat/lon, got {init.lat!r}, {init.lon!r}")
            elif has_pos:
                raise ValueError(f"{where}: lat/lon not allowed for {ph} (the unit stands at the stop point)")
            _check_int(f"{where}: service_elapsed_s", init.service_elapsed_s)
            if ph == IN_SERVICE:
                if not 0 <= init.service_elapsed_s < self.service_s:
                    raise ValueError(f"{where}: service_elapsed_s={init.service_elapsed_s} must be in "
                                     f"[0, {self.service_s - 1}]")
            elif init.service_elapsed_s != 0:
                raise ValueError(f"{where}: service_elapsed_s is only allowed for in_service")
            if init.radius_enter_ts is not None:
                if ph != WAITING:
                    raise ValueError(f"{where}: radius_enter_ts is only allowed for waiting")
                _check_int(f"{where}: radius_enter_ts", init.radius_enter_ts, None, self.start_ts)

            target = init.target_station or u.own
            if ph == TO_STATION:
                u.x, u.y = self._proj.to_xy(init.lat, init.lon)  # type: ignore[arg-type]
                u.ux = u.uy = 0.0
                u.target = target
                u.redirected = target != u.own
                self._leg_to_station(u, self._st_by_id[target])
            elif ph == TO_UNLOAD:
                u.x, u.y = self._proj.to_xy(init.lat, init.lon)  # type: ignore[arg-type]
                u.ux = u.uy = 0.0
                u.phase = TO_UNLOAD
                u.target = None
                self._leg_to(u, self._ux0, self._uy0)
            else:
                st = self._st_by_id[target]
                u.phase = WAITING if ph == WAITING else IN_SERVICE
                u.target = target
                u.redirected = target != u.own
                u.joined = True
                self._place_at_stop(u, st)
                if ph == WAITING:
                    key = init.radius_enter_ts if init.radius_enter_ts is not None else self.start_ts
                    bisect.insort(st.fifo, (key, uid))
                else:
                    if st.busy is not None:
                        raise ValueError(f"{where}: station {st.uuid} already has {st.busy} in service")
                    st.busy = uid
                    u.service_start = self.start_ts - init.service_elapsed_s
