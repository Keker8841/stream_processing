"""Long-run consistency with the generator: the engine's occupancy timeline vs the generator's ground truth.

The engine (task rules) releases a station when the occupant's telemetry leaves the 50 m radius (generator: depart +
~3 s) or 230 s after occupancy, and the next unit occupies on its next standing message. The generator starts the next
service at depart + maneuver_s. Hence, without faults, engine occupancy - truth service_start is:
  maneuver 30: -28 .. 0  (-27 for a queued unit: exit = depart + 3, start = depart + 30; up to 0 on a free station)
  maneuver 0:    0 .. +4 (queued unit occupies on the previous unit's exit, i.e. depart + 3/4)
and the engine's release is the truth radius_exit (-1 when the rounded telemetry is outside one tick earlier).
With faults (lag <= 60 s), a held exit lets the 230 s timer release instead: with maneuver 0 up to 30 s after the
exit, so the next occupancy may lag the truth by up to ~34 s; with maneuver 30 the timer coincides with the exit.
Units that entered a station radius before the stream started are cold-start visits (who of the standing units is in
service is not visible in telemetry) and are only required not to break the timeline.
"""

from collections import defaultdict

import pytest

from generator.faults import FaultConfig, FaultInjector
from generator.sim import Simulator
from generator.site import generate_site
from vqservice.config import EngineConfig
from vqservice.engine import Engine
from vqservice.site import Point, Site

START = 1789473600
WARMUP_S = 3600


class _Traced(Engine):
    """Engine that records occupancy intervals [unit, start, end]; voided occupancies are dropped."""

    def __init__(self, *a, **kw) -> None:
        super().__init__(*a, **kw)
        self.intervals: dict[str, list[list]] = defaultdict(list)
        self.voided = 0

    def _void(self, sid: str, uid: str) -> None:
        iv = [x for x in self.intervals[sid] if x[0] == uid]
        self.intervals[sid].remove(iv[-1])
        self.voided += 1

    def _occupy(self, u) -> None:
        i = u.radius
        before = None if i is None else self._st[i].occupant
        super()._occupy(u)
        if i is not None and self._st[i].occupant == u.uid and before != u.uid:
            if before is not None:                                  # takeover
                self._void(self._st[i].uuid, before)
            self.intervals[self._st[i].uuid].append([u.uid, u.ts, None])

    def _release(self, st, at: int) -> None:
        [iv] = [x for x in self.intervals[st.uuid] if x[0] == st.occupant and x[2] is None]
        iv[2] = at
        super()._release(st, at)

    def _served_unseen(self, i: int, u, ts: int) -> None:
        st = self._st[i]
        for v in self._units.values():
            if v.radius == i and v is not u and (v.served or st.occupant == v.uid):
                self._void(st.uuid, v.uid)
        super()._served_unseen(i, u, ts)


def _site(g) -> Site:
    return Site(Point(g.unload.uuid, g.unload.lat, g.unload.lon),
                tuple(Point(s.uuid, s.lat, s.lon) for s in g.stations), dict(g.assignment))


def _run(site_seed: int, duration_s: int, maneuver: int, faults: bool, closed: bool = False):
    g = generate_site(site_seed)
    sim = Simulator(g, start_ts=START - WARMUP_S, maneuver_s=maneuver, coord_precision=4)
    truth = []
    for _ in range(WARMUP_S):
        truth += sim.step()[1]
    eng = _Traced(_site(g), EngineConfig(apply_decisions=closed))
    inj = FaultInjector(FaultConfig() if faults else FaultConfig.none(), seed=3)
    recommended, pending, problems, first_ts = [], [], [], {}
    for k in range(duration_s):
        for uid, to in pending:
            sim.request_redirect(uid, to)
        pending = []
        records, events = sim.step()
        truth += events
        inj.push(sim.now, records)
        for em in inj.pop_due(sim.now):
            first_ts.setdefault(em.key, em.value["ts"])
            for o in eng.process(em.value):
                if o.kind == "decision" and o.value["result"] == "recommended":
                    cold = int(dict(o.headers)["decision-id"].rsplit(":", 1)[1]) == first_ts[o.key]
                    recommended.append((sim.now, o.key, o.value["to_station"], cold))
                    if closed:
                        pending.append((o.key, o.value["to_station"]))
        if closed and k % 5 == 0:
            members = [it["unit_uuid"] for q in eng.queues().values() for it in q]
            occupants = [st.occupant for st in eng._st if st.occupant is not None]
            if len(members) != len(set(members)) or len(occupants) != len(set(occupants)):
                problems.append((sim.now, members, occupants))
    return eng, truth, recommended, problems, sim.now


def _visits(truth, end: int):
    """Truth visits with a service: (unit, station, radius_enter, service_start, radius_exit), all inside the run."""
    open_, out = {}, []
    for e in truth:
        key = (e.unit_uuid, e.station_uuid)
        if e.event == "radius_enter":
            open_[key] = {"enter": e.ts}
        elif e.event == "service_start":
            open_.setdefault(key, {"enter": None})["start"] = e.ts
        elif e.event == "radius_exit":
            v = open_.pop(key, {"enter": None})
            if "start" in v and v["start"] >= START and e.ts <= end - 5:
                out.append((e.unit_uuid, e.station_uuid, v["enter"], v["start"], e.ts))
    return out


def _check_timeline(eng: _Traced, truth, end: int, occ_bounds: tuple[int, int], rel_bounds: tuple[int, int],
                    unseen_ok: bool = False) -> dict:
    # no two occupancies overlap at a station, every one is closed in time
    for sid, ivs in eng.intervals.items():
        ivs = sorted(ivs, key=lambda x: x[1])
        for a, b in zip(ivs, ivs[1:]):
            assert a[2] is not None and a[2] <= b[1], (sid, a, b)
    by_unit = defaultdict(list)
    for sid, ivs in eng.intervals.items():
        for uid, start, stop in ivs:
            by_unit[(uid, sid)].append((start, stop))
    d_occ, d_rel, cold, unseen = [], [], 0, 0
    for uid, sid, enter, start, exit_ in _visits(truth, end):
        mine = [(a, b) for a, b in by_unit[(uid, sid)] if (enter or 0) <= a <= exit_]
        assert len(mine) <= 1, ("occupied twice in one visit", uid, sid, mine)
        if enter is None or enter < START:
            cold += 1
            continue
        if not mine and unseen_ok:
            unseen += 1                       # FIFO order differed (same-second radius entries); resynced on exit
            continue
        assert len(mine) == 1, ("service without an engine occupancy", uid, sid, enter, start, exit_)
        (a, b), = mine
        d_occ.append(a - start)
        d_rel.append(b - exit_)
    assert d_occ and occ_bounds[0] <= min(d_occ) and max(d_occ) <= occ_bounds[1], (min(d_occ), max(d_occ))
    assert rel_bounds[0] <= min(d_rel) and max(d_rel) <= rel_bounds[1], (min(d_rel), max(d_rel))
    return {"matched": len(d_occ), "cold": cold, "unseen": unseen}


@pytest.mark.parametrize("maneuver, occ_bounds", [(30, (-28, 0)), (0, (0, 4))])
def test_open_loop_without_faults_matches_truth(maneuver, occ_bounds):
    """Site 4 starts with long queues: the engine's cold-start guess of the occupant is wrong at S1 and must not
    shift the timeline for the rest of the run (it used to: one unit 'served' too early, every later one 257 s off)."""
    eng, truth, _, _, end = _run(4, 3000, maneuver, faults=False)
    r = _check_timeline(eng, truth, end, occ_bounds, (-1, 0))
    assert r["matched"] >= 20 and eng.voided > 0


@pytest.mark.parametrize("maneuver, occ_bounds, rel_bounds", [(30, (-28, 0), (-1, 1)), (0, (0, 35), (-1, 31))])
def test_open_loop_with_faults_matches_truth(maneuver, occ_bounds, rel_bounds):
    eng, truth, _, _, end = _run(4, 3000, maneuver, faults=True)
    r = _check_timeline(eng, truth, end, occ_bounds, rel_bounds)
    assert r["matched"] >= 20 and eng.stats["ignored_old"] > 0


def test_closed_loop_redirects_are_served_where_recommended():
    eng, truth, recommended, problems, end = _run(42, 3600, 30, faults=True, closed=True)
    assert problems == []                     # no unit in two queues, no unit occupying two stations
    served = defaultdict(list)
    applied = {(e.unit_uuid, e.ts) for e in truth if e.event == "redirect_applied"}
    ignored = {(e.unit_uuid, e.ts) for e in truth if e.event == "redirect_ignored"}
    for e in truth:
        if e.event == "service_start":
            served[e.unit_uuid].append((e.ts, e.station_uuid))
    done = 0
    for at, uid, to, cold in recommended:
        if cold and (uid, at + 1) in ignored:
            continue                          # cold start: the unit was not driving to its station
        assert (uid, at + 1) in applied       # applied by the generator on the next tick
        nxt = [sid for ts, sid in served[uid] if ts > at]
        if nxt:
            assert nxt[0] == to
            done += 1
    assert done >= 5
    # redirected units approach from other directions: same-second radius entries may be ordered differently
    _check_timeline(eng, truth, end, (-28, 0), (-5, 1), unseen_ok=True)
