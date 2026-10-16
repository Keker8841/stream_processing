"""Pure virtual-queue engine: telemetry in, queue.v1 / decision.v1 payloads out.

No I/O, no wall clock, no randomness. "now" is the max ts of accepted messages. One accepted message is handled as:
timer releases (now may have advanced) -> unit update (position, radius entry/exit + exit release, phase, trip,
recommendation trigger) -> occupancy -> recommendation -> all queues recomputed -> outputs (decision first, then the
changed queues in station order). Ignored messages (old/duplicate/unknown/invalid) change nothing.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .config import EngineConfig
from .geo import haversine_m
from .site import Site
from .timefmt import iso, round_half_up

TO_STATION = "to_station"
TO_UNLOAD = "to_unload"
SNAPSHOT_VERSION = 3
STAT_KEYS = ("accepted", "ignored_old", "ignored_unknown", "ignored_invalid", "decisions", "recommended",
             "rejected_no_gain", "rejected_stale", "queue_published", "occupancies", "releases")

# Queue item: (unit_uuid, eta or None for the occupant, service_start, free_at, wait_seconds), epoch seconds.
Item = tuple[str, int | None, int, int, int]


@dataclass(frozen=True)
class Output:
    kind: str                     # "queue" | "decision"
    key: str                      # station_uuid | unit_uuid
    value: dict                   # payload, field order as in the task
    headers: tuple[tuple[str, str], ...] = ()


class _Unit:
    __slots__ = ("uid", "own", "ts", "lat", "lon", "speed", "dist", "d_unload", "phase", "redirect", "decided",
                 "radius", "entry", "stood", "served", "in_unload")

    def __init__(self, uid: str, own: int) -> None:
        self.uid = uid
        self.own = own                          # own station index
        self.ts = 0                             # last accepted message
        self.lat = self.lon = self.speed = 0.0
        self.dist: list[float] = []             # to every station center (derived from lat/lon)
        self.d_unload = 0.0
        self.phase = TO_STATION
        self.redirect: int | None = None        # applied recommendation (station index) until occupancy/trip end
        self.decided = False                    # no more recommendation in this trip
        self.radius: int | None = None          # station whose 50 m radius the unit is in
        self.entry: int | None = None           # radius entry ts
        self.stood = False                      # stood inside that radius (or was first seen inside it)
        self.served = False                     # occupancy ended, has not left the radius yet
        self.in_unload = False                  # inside the unload radius


class _Station:
    __slots__ = ("uuid", "occupant", "occupied_at", "last_release", "published")

    def __init__(self, uuid: str) -> None:
        self.uuid = uuid
        self.occupant: str | None = None
        self.occupied_at = 0
        self.last_release: int | None = None
        self.published: tuple[Item, ...] | None = None   # last published queue (without `at`)


def _num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def _parse(raw: Any) -> tuple[str, int, float, float, float] | None:
    try:
        uid, ts, lat, lon, speed = raw["unit_uuid"], raw["ts"], raw["lat"], raw["lon"], raw["speed_kmh"]
    except (KeyError, TypeError, IndexError):
        return None
    if isinstance(ts, float) and ts.is_integer():
        ts = int(ts)
    if not (isinstance(uid, str) and isinstance(ts, int) and not isinstance(ts, bool)
            and _num(lat) and _num(lon) and _num(speed)):
        return None
    return uid, ts, float(lat), float(lon), float(speed)


class Engine:
    def __init__(self, site: Site, cfg: EngineConfig) -> None:
        self.site = site
        self.cfg = cfg
        self._occ = cfg.occupancy_s
        self._st = [_Station(s.uuid) for s in site.stations]
        self._sidx = {s.uuid: i for i, s in enumerate(site.stations)}
        self._unload_to = [haversine_m(site.unload.lat, site.unload.lon, s.lat, s.lon) for s in site.stations]
        self._units: dict[str, _Unit] = {}
        self._now: int | None = None
        self._stats = dict.fromkeys(STAT_KEYS, 0)

    # ------------------------------------------------------------------------------------------------- public

    @property
    def now(self) -> int | None:
        return self._now

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._stats)

    def process(self, raw: Mapping[str, Any]) -> list[Output]:
        msg = _parse(raw)
        if msg is None:
            self._stats["ignored_invalid"] += 1
            return []
        uid, ts, lat, lon, speed = msg
        own = self.site.assignment.get(uid)
        if own is None:
            self._stats["ignored_unknown"] += 1
            return []
        u = self._units.get(uid)
        if u is not None and ts <= u.ts:
            self._stats["ignored_old"] += 1
            return []
        self._stats["accepted"] += 1
        first = u is None
        if u is None:
            u = self._units[uid] = _Unit(uid, self._sidx[own])
        if self._now is None or ts > self._now:
            self._now = ts
        self._timer_releases()
        trigger = self._update(u, ts, lat, lon, speed, first)
        self._occupy(u)
        out = [self._decide(u)] if trigger else []
        out.extend(self._publish())
        return out

    def queues(self) -> dict[str, list[dict]]:
        if self._now is None:
            return {st.uuid: [] for st in self._st}
        return {st.uuid: [_item(it) for it in items] for st, items in zip(self._st, self._compute())}

    def snapshot(self) -> dict:
        sid = [st.uuid for st in self._st]
        units = {}
        for uid in sorted(self._units):
            u = self._units[uid]
            units[uid] = {
                "ts": u.ts, "lat": u.lat, "lon": u.lon, "speed": u.speed, "phase": u.phase,
                "redirect": None if u.redirect is None else sid[u.redirect], "decided": u.decided,
                "radius": None if u.radius is None else sid[u.radius], "entry": u.entry, "stood": u.stood,
                "served": u.served, "in_unload": u.in_unload,
            }
        stations = {st.uuid: {"occupant": st.occupant, "occupied_at": st.occupied_at,
                              "last_release": st.last_release,
                              "published": None if st.published is None else [list(it) for it in st.published]}
                    for st in self._st}
        return {"version": SNAPSHOT_VERSION, "now": self._now, "stats": dict(self._stats),
                "stations": stations, "units": units}

    @classmethod
    def restore(cls, site: Site, cfg: EngineConfig, snap: Mapping[str, Any]) -> Engine:
        if snap.get("version") != SNAPSHOT_VERSION:
            raise ValueError(f"unsupported engine snapshot version {snap.get('version')!r}")
        e = cls(site, cfg)
        e._now = snap["now"]
        e._stats.update(snap["stats"])
        for st in e._st:
            d = snap["stations"][st.uuid]
            st.occupant, st.occupied_at, st.last_release = d["occupant"], d["occupied_at"], d["last_release"]
            pub = d["published"]
            st.published = None if pub is None else tuple(tuple(it) for it in pub)
        for uid, d in snap["units"].items():
            u = e._units[uid] = _Unit(uid, e._sidx[site.assignment[uid]])
            u.ts, u.lat, u.lon, u.speed, u.phase = d["ts"], d["lat"], d["lon"], d["speed"], d["phase"]
            u.redirect = None if d["redirect"] is None else e._sidx[d["redirect"]]
            u.decided, u.served, u.in_unload, u.entry = d["decided"], d["served"], d["in_unload"], d["entry"]
            u.stood = d["stood"]
            u.radius = None if d["radius"] is None else e._sidx[d["radius"]]
            e._locate(u)
        return e

    # ------------------------------------------------------------------------------------------- unit state

    def _locate(self, u: _Unit) -> None:
        u.dist = [haversine_m(u.lat, u.lon, s.lat, s.lon) for s in self.site.stations]
        un = self.site.unload
        u.d_unload = haversine_m(u.lat, u.lon, un.lat, un.lon)

    def _new_trip(self, u: _Unit, phase: str) -> None:
        u.phase = phase
        u.redirect = None
        u.decided = False

    def _update(self, u: _Unit, ts: int, lat: float, lon: float, speed: float, first: bool) -> bool:
        """Apply the message to the unit; True if it triggers the recommendation of this trip."""
        cfg = self.cfg
        u.ts, u.lat, u.lon, u.speed = ts, lat, lon, speed
        self._locate(u)

        inside = None
        for i, d in enumerate(u.dist):
            if d < cfg.station_radius_m:
                inside = i
                break
        if u.radius is not None and u.radius != inside:      # left the station radius
            st = self._st[u.radius]
            if st.occupant == u.uid:
                self._release(st, ts)
            elif u.stood and not u.served:
                self._served_unseen(u.radius, u, ts)
            if u.served:                                      # after service: to unload, the trip is over
                self._new_trip(u, TO_UNLOAD)
            u.served = u.stood = False
            u.radius = u.entry = None
        if inside is not None and u.radius is None:
            u.radius, u.entry, u.stood = inside, ts, first
        if inside is not None and speed < cfg.standing_kmh:
            u.stood = True

        in_unload = u.d_unload < cfg.unload_radius_m
        if u.in_unload and not in_unload:                     # left the unload point: new trip to the own station
            self._new_trip(u, TO_STATION)
        u.in_unload = in_unload

        if first and inside is not None and speed < cfg.standing_kmh:
            u.decided = True            # first seen "at a station", not "to station": no recommendation this trip
        if u.phase != TO_STATION or u.redirect is not None or u.decided:
            return False
        # the first message of the trip closer than the decision radius; on a cold start that may be the very first
        # message of the unit ("to station" is its default state)
        return u.dist[u.own] < cfg.decision_radius_m

    # ------------------------------------------------------------------------------------------- occupancy

    def _release(self, st: _Station, at: int) -> None:
        self._units[st.occupant].served = True               # type: ignore[index]
        st.occupant = None
        st.last_release = at
        self._stats["releases"] += 1

    def _served_unseen(self, i: int, u: _Unit, ts: int) -> None:
        """`u` stood at station i and leaves it, so it was served there although the engine never saw it occupy
        (cold start: occupant and waiting units look alike; or its telemetry was held). Whoever got station i
        meanwhile was in fact waiting: that occupancy (or its release) is undone, the station frees now."""
        st = self._st[i]
        for v in self._units.values():
            if v.radius == i and v is not u:
                v.served = False
        st.occupant = None
        st.last_release = ts
        u.served = True

    def _timer_releases(self) -> None:
        for st in self._st:
            if st.occupant is not None and self._now >= st.occupied_at + self._occ:   # type: ignore[operator]
                self._release(st, st.occupied_at + self._occ)

    def _occupy(self, u: _Unit) -> None:
        """The FIFO head inside a free station's radius occupies it on its own standing message. A head whose
        message is not later than the current occupancy (late telemetry, same second) stood there first: it
        takes the station."""
        i = u.radius
        if i is None or u.served or u.speed >= self.cfg.standing_kmh:
            return
        st = self._st[i]
        if st.last_release is not None and u.ts < st.last_release:
            return
        if st.occupant is not None and st.occupied_at < u.ts:
            return
        head = min((v.entry, v.uid) for v in self._units.values() if v.radius == i and not v.served)
        if head[1] != u.uid:
            return
        st.occupant, st.occupied_at = u.uid, u.ts
        u.redirect = None                                     # occupancy anywhere ends a redirect
        self._stats["occupancies"] += 1

    # ---------------------------------------------------------------------------------------------- queues

    def _eta(self, u: _Unit, i: int, now: int) -> int:
        return max(u.ts + round_half_up(u.dist[i] / self.cfg.speed_mps), now)

    def _members(self, exclude: str | None = None) -> list[list[tuple[int, str]]]:
        """(eta, unit) per station, sorted; occupants, served, stale and beyond-horizon units left out."""
        cfg, now = self.cfg, self._now
        assert now is not None
        busy = {st.occupant for st in self._st}
        limit = now + cfg.horizon_s
        out: list[list[tuple[int, str]]] = [[] for _ in self._st]
        for u in self._units.values():
            if u.served or u.uid in busy or u.uid == exclude or now - u.ts > cfg.fresh_s:
                continue
            if u.phase == TO_UNLOAD:
                i = u.own
                eta = max(u.ts + round_half_up((u.d_unload + self._unload_to[i]) / cfg.speed_mps), now)
            else:
                i = u.own if u.redirect is None else u.redirect
                eta = u.entry if u.radius == i else self._eta(u, i, now)      # type: ignore[assignment]
            if eta <= limit:
                out[i].append((eta, u.uid))
        for m in out:
            m.sort()
        return out

    def _cascade(self, st: _Station, members: list[tuple[int, str]],
                 before: tuple[int, str] | None = None) -> tuple[list[Item], int | None]:
        """Queue items of a station (members ahead of `before` only, if given) and the resulting release moment."""
        occ = self._occ
        items: list[Item] = []
        if st.occupant is not None:
            prev: int | None = st.occupied_at + occ
            items.append((st.occupant, None, st.occupied_at, prev, 0))   # type: ignore[arg-type]
        else:
            prev = st.last_release
        for eta, uid in members:
            if before is not None and (eta, uid) >= before:
                break
            start = eta if prev is None or eta >= prev else prev
            items.append((uid, eta, start, start + occ, start - eta))
            prev = start + occ
        return items, prev

    def _compute(self) -> list[tuple[Item, ...]]:
        return [tuple(self._cascade(st, m)[0]) for st, m in zip(self._st, self._members())]

    def _publish(self) -> list[Output]:
        out = []
        at = iso(self._now)                                   # type: ignore[arg-type]
        for st, items in zip(self._st, self._compute()):
            if items != st.published:
                st.published = items
                self._stats["queue_published"] += 1
                out.append(Output("queue", st.uuid, {"station_uuid": st.uuid, "at": at,
                                                     "queue": [_item(it) for it in items]}))
        return out

    # -------------------------------------------------------------------------------------- recommendation

    def _decide(self, u: _Unit) -> Output:
        cfg, now, ts = self.cfg, self._now, u.ts
        assert now is not None
        u.decided = True
        self._stats["decisions"] += 1
        headers = (("decision-id", f"{u.uid}:{ts}"),)
        if now - ts > cfg.fresh_s:
            self._stats["rejected_stale"] += 1
            return Output("decision", u.uid, {"unit_uuid": u.uid, "at": iso(now), "result": "rejected",
                                              "reason": "stale_telemetry"}, headers)
        best = None
        values = []
        for i, (st, members) in enumerate(zip(self._st, self._members(exclude=u.uid))):
            eta = self._eta(u, i, now)
            _, release = self._cascade(st, members, (eta, u.uid))
            start = eta if release is None or release <= eta else release
            value = start - eta if cfg.metric == "queue_wait" else start + self._occ
            values.append(value)
            key = (value, i != u.own, u.dist[i], st.uuid)
            if best is None or key < best[0]:
                best = (key, i)
        assert best is not None
        chosen = best[1]
        gain = values[u.own] - values[chosen]
        if chosen == u.own or gain < cfg.min_gain_s:
            self._stats["rejected_no_gain"] += 1
            return Output("decision", u.uid, {"unit_uuid": u.uid, "at": iso(now), "result": "rejected",
                                              "reason": "no_gain"}, headers)
        self._stats["recommended"] += 1
        if cfg.apply_decisions:
            u.redirect = chosen
        return Output("decision", u.uid, {"unit_uuid": u.uid, "at": iso(now), "result": "recommended",
                                          "from_station": self._st[u.own].uuid, "to_station": self._st[chosen].uuid,
                                          "gain_seconds": gain}, headers)


def _item(it: Item) -> dict:
    uid, eta, start, free, wait = it
    return {"unit_uuid": uid, "eta": None if eta is None else iso(eta), "service_start": iso(start),
            "free_at": iso(free), "wait_seconds": wait}
