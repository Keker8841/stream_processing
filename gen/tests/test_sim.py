import decimal
import math
from collections import defaultdict
from dataclasses import dataclass, field

import pytest

from generator.constants import SERVICE_S, STATION_RADIUS_M, STOP_OFFSET_M
from generator.geo import Projection, dist_xy, projection_for
from generator.model import Site, Station, Telemetry, TruthEvent, UnitInit, UnloadPoint
from generator.sim import EVENT_ORDER, Simulator
from generator.site import generate_site

LAT0, LON0 = 49.0, 142.0
START = 1_789_473_600
PROJ = Projection(LAT0, LON0)
EPS = 0.2           # meters of slack for 6-decimal rounding (~0.06 m per coordinate)


def polar(dist: float, bearing_deg: float, origin: tuple[float, float] = (0.0, 0.0)) -> tuple[float, float]:
    b = math.radians(bearing_deg)
    return origin[0] + dist * math.sin(b), origin[1] + dist * math.cos(b)


def ll(p: tuple[float, float]) -> tuple[float, float]:
    return PROJ.to_latlon(*p)


def make_site(stations: dict[str, tuple[float, float]], assignment: dict[str, str]) -> Site:
    """Unload point at the projection center; stations given in local meters."""
    return Site(LAT0, LON0, UnloadPoint("U1", LAT0, LON0),
                tuple(Station(sid, *ll(p)) for sid, p in stations.items()), assignment)


def xy(t: Telemetry, proj: Projection = PROJ) -> tuple[float, float]:
    return proj.to_xy(t.lat, t.lon)


def legs(dist: float) -> int:
    """Ticks needed to cover `dist` meters at 10 m per tick (the last one is the standing arrival tick)."""
    return math.ceil(dist / 10)


@dataclass
class Trace:
    start: int
    ticks: list[list[Telemetry]] = field(default_factory=list)
    events: list[TruthEvent] = field(default_factory=list)
    tick_events: list[list[TruthEvent]] = field(default_factory=list)
    phases: list[dict[str, tuple[str, str]]] = field(default_factory=list)

    def rec(self, uid: str, ts: int) -> Telemetry:
        r = next(r for r in self.ticks[ts - self.start] if r.unit_uuid == uid)
        assert r.ts == ts
        return r

    def series(self, uid: str) -> list[Telemetry]:
        return [r for tick in self.ticks for r in tick if r.unit_uuid == uid]

    def ts(self, uid: str, event: str, station: str | None = None) -> list[int]:
        return [e.ts for e in self.events
                if e.unit_uuid == uid and e.event == event and (station is None or e.station_uuid == station)]

    def of(self, uid: str) -> list[tuple[int, str, str | None]]:
        return [(e.ts, e.event, e.station_uuid) for e in self.events if e.unit_uuid == uid]

    def phase(self, uid: str, ts: int) -> tuple[str, str]:
        return self.phases[ts - self.start][uid]


def run(sim: Simulator, n: int, requests: dict[int, list[tuple[str, str]]] | None = None,
        phases: bool = False) -> Trace:
    """Run n steps. requests[ts] are issued right before the step that produces tick ts."""
    tr = Trace(sim.now + 1)
    for _ in range(n):
        for uid, to in (requests or {}).get(sim.now + 1, ()):
            sim.request_redirect(uid, to)
        recs, evs = sim.step()
        assert [r.ts for r in recs] == [sim.now] * len(recs)
        assert all(e.ts == sim.now for e in evs)
        tr.ticks.append(recs)
        tr.tick_events.append(evs)
        tr.events.extend(evs)
        if phases:
            tr.phases.append({u: (sim.unit_phase(u), sim.unit_target(u)) for u in sim.unit_ids})
    return tr


# ----------------------------------------------------------------------------------------------- single unit cycle

D1 = 998.0                  # L/10 = 98.3: carry-over saves a second per cycle (2*ceil(98.3) = 198 vs ceil(196.6))
S1_XY = polar(D1, 37.0)


def one_unit_site(d: float = D1, bearing: float = 37.0) -> Site:
    return make_site({"S1": polar(d, bearing)}, {"T-01": "S1"})


def test_single_unit_full_cycle():
    site = one_unit_site()
    sim = Simulator(site, start_ts=START, coord_precision=6)
    assert sim.now == START - 1
    assert (sim.unit_phase("T-01"), sim.unit_target("T-01")) == ("to_station", "S1")
    tr = run(sim, 900, phases=True)

    L = D1 - STOP_OFFSET_M
    enter = START + math.floor((D1 - STATION_RADIUS_M) / 10)       # first tick with distance < 50 m
    arrive = START + legs(L) - 1
    depart = arrive + SERVICE_S
    exit_ = depart + math.ceil((STATION_RADIUS_M - STOP_OFFSET_M) / 10) - 1
    unload = depart + legs(L) - 1
    arrive2 = depart + legs(2 * L) - 1
    assert tr.of("T-01")[:7] == [
        (enter, "radius_enter", "S1"), (arrive, "service_start", "S1"), (depart, "depart", "S1"),
        (exit_, "radius_exit", "S1"), (unload, "unload", None), (arrive2 - 3, "radius_enter", "S1"),
        (arrive2, "service_start", "S1")]
    assert arrive2 - arrive == SERVICE_S + legs(2 * L) - 1 == 396

    # first tick: already 10 m from the unload point
    first = tr.rec("T-01", START)
    assert first.speed_kmh == 36.0
    assert dist_xy(xy(first), (0, 0)) == pytest.approx(10.0, abs=EPS)

    for t in range(START, arrive2 + 1):
        r = tr.rec("T-01", t)
        standing = arrive <= t < depart or t in (unload, arrive2)
        assert r.speed_kmh == (0.0 if standing else 36.0), t
    stop = tr.rec("T-01", arrive)
    assert dist_xy(xy(stop), S1_XY) == pytest.approx(STOP_OFFSET_M, abs=EPS)
    assert dist_xy(xy(stop), (0, 0)) == pytest.approx(L, abs=EPS)
    assert all(tr.rec("T-01", t) == Telemetry("T-01", t, stop.lat, stop.lon, 0.0) for t in range(arrive, depart))
    assert dist_xy(xy(tr.rec("T-01", depart)), xy(stop)) == pytest.approx(10.0, abs=EPS)
    assert dist_xy(xy(tr.rec("T-01", exit_ - 1)), S1_XY) < STATION_RADIUS_M
    assert dist_xy(xy(tr.rec("T-01", exit_)), S1_XY) >= STATION_RADIUS_M
    assert dist_xy(xy(tr.rec("T-01", enter - 1)), S1_XY) >= STATION_RADIUS_M
    assert dist_xy(xy(tr.rec("T-01", enter)), S1_XY) < STATION_RADIUS_M
    u = tr.rec("T-01", unload)
    assert (u.lat, u.lon, u.speed_kmh) == (LAT0, LON0, 0.0)
    assert tr.rec("T-01", arrive2) == Telemetry("T-01", arrive2, stop.lat, stop.lon, 0.0)

    # displacement between records: <= 10 m except the tick after unload (carry-over, <= 20 m)
    series = tr.series("T-01")
    unloads = tr.ts("T-01", "unload")
    assert len(unloads) == 2
    for a, b in zip(series, series[1:]):
        step = dist_xy(xy(a), xy(b))
        if a.ts in unloads:
            assert 10.0 - EPS <= step <= 20.0 + EPS
        else:
            assert step <= 10.0 + EPS, b.ts

    assert tr.phase("T-01", enter) == ("to_station", "S1")
    assert tr.phase("T-01", arrive) == ("in_service", "S1")
    assert tr.phase("T-01", depart) == ("to_unload", "U1")
    assert tr.phase("T-01", unload) == ("to_station", "S1")


@pytest.mark.parametrize("d", [998.0, 1003.0, 1500.5, 2347.3])
def test_carry_over_no_extra_second_per_cycle(d):
    sim = Simulator(one_unit_site(d, 211.0), start_ts=START, coord_precision=6)
    L = d - STOP_OFFSET_M
    cycle = SERVICE_S + legs(2 * L) - 1
    n_cycles = 5
    first = START + legs(L) - 1
    tr = run(sim, first - START + n_cycles * cycle + 1)
    starts = tr.ts("T-01", "service_start", "S1")
    assert starts == [first + k * cycle for k in range(n_cycles + 1)]
    unloads = tr.ts("T-01", "unload")
    assert unloads == [first + SERVICE_S + legs(L) - 1 + k * cycle for k in range(n_cycles)]
    for t in unloads:
        assert tr.rec("T-01", t) == Telemetry("T-01", t, LAT0, LON0, 0.0)
        assert tr.rec("T-01", t + 1).speed_kmh == 36.0
    if d == 998.0:
        assert cycle < SERVICE_S + 2 * legs(L)   # carry-over actually matters for this geometry


# --------------------------------------------------------------------------------------------------- station FIFO

@pytest.mark.parametrize("maneuver", [0, 30])
def test_fifo_lockstep_ties_by_unit_uuid(maneuver):
    site = make_site({"S1": S1_XY, "S2": polar(1500, 200)},
                     {"T-03": "S1", "T-02": "S1", "T-01": "S1", "T-04": "S2"})
    sim = Simulator(site, start_ts=START, maneuver_s=maneuver, coord_precision=6)
    L = D1 - STOP_OFFSET_M
    arrive = START + legs(L) - 1
    tr = run(sim, arrive - START + 3 * (SERVICE_S + maneuver) + 5, phases=True)

    enter = START + math.floor((D1 - STATION_RADIUS_M) / 10)
    enter_events = [e for e in tr.tick_events[enter - START] if e.event == "radius_enter"]
    assert [(e.unit_uuid, e.station_uuid) for e in enter_events] == [("T-01", "S1"), ("T-02", "S1"), ("T-03", "S1")]
    s1 = arrive
    s2 = s1 + SERVICE_S + maneuver
    s3 = s2 + SERVICE_S + maneuver
    assert [tr.ts(u, "service_start")[0] for u in ("T-01", "T-02", "T-03")] == [s1, s2, s3]
    assert [tr.ts(u, "depart")[0] for u in ("T-01", "T-02")] == [s1 + SERVICE_S, s2 + SERVICE_S]

    stop = tr.rec("T-01", arrive)
    for uid, until in (("T-02", s2), ("T-03", s3)):
        for t in range(arrive, until + SERVICE_S):
            r = tr.rec(uid, t)
            assert (r.lat, r.lon, r.speed_kmh) == (stop.lat, stop.lon, 0.0), (uid, t)
        assert tr.phase(uid, until - 1) == ("waiting", "S1")
        assert tr.phase(uid, until) == ("in_service", "S1")
    assert tr.phase("T-03", arrive) == ("waiting", "S1")
    if maneuver == 0:
        assert [(e.unit_uuid, e.event) for e in tr.tick_events[s2 - START]] == \
            [("T-01", "depart"), ("T-02", "service_start")]
    else:
        assert tr.ts("T-01", "depart")[0] == s2 - maneuver


def test_station_free_during_maneuver():
    site = make_site({"S1": S1_XY}, {"T-01": "S1", "T-02": "S1"})
    sim = Simulator(site, start_ts=START, maneuver_s=30)
    arrive = START + legs(D1 - STOP_OFFSET_M) - 1
    run(sim, arrive - START + SERVICE_S)            # last tick of T-01 service
    assert (sim.station_busy("S1"), sim.station_queue("S1")) == ("T-01", ["T-02"])
    run(sim, 1)                                     # release tick
    assert (sim.station_busy("S1"), sim.station_queue("S1")) == (None, ["T-02"])
    assert sim.unit_phase("T-02") == "waiting"
    run(sim, 29)
    assert sim.station_busy("S1") is None
    tr = run(sim, 1)
    assert sim.station_busy("S1") == "T-02" and sim.station_queue("S1") == []
    assert [(e.unit_uuid, e.event) for e in tr.events] == [("T-02", "service_start")]


def test_fifo_by_radius_entry_not_by_uuid():
    site = make_site({"S1": S1_XY}, {"T-01": "S1", "T-02": "S1"})
    near = polar(D1 - 300.0, 37.0)     # 300 m before S1 on the unload -> S1 line
    sim = Simulator(site, start_ts=START, coord_precision=6,
                    initial=[UnitInit("T-02", "to_station", lat=ll(near)[0], lon=ll(near)[1])])
    tr = run(sim, 700)
    e1, e2 = tr.ts("T-01", "radius_enter")[0], tr.ts("T-02", "radius_enter")[0]
    assert e2 < e1
    a2 = START + legs(300 - STOP_OFFSET_M) - 1
    assert tr.ts("T-02", "service_start")[0] == a2
    assert tr.ts("T-01", "service_start")[0] == a2 + SERVICE_S


def test_strict_fifo_head_still_driving_blocks_standing_unit():
    site = make_site({"S1": S1_XY}, {"T-01": "S1", "T-02": "S1"})
    behind = polar(47.0, 37.0, S1_XY)          # far side of S1, inside the radius
    sim = Simulator(site, start_ts=START, coord_precision=6, initial=[
        UnitInit("T-01", "to_station", lat=ll(behind)[0], lon=ll(behind)[1]),
        UnitInit("T-02", "waiting"),
    ])
    assert sim.station_queue("S1") == ["T-02"]
    tr = run(sim, 260, phases=True)
    assert tr.ts("T-01", "radius_enter") == [START]
    # T-01 entered at START and sorts before T-02 (same key tick); it needs 32 m -> arrives at START+3
    assert tr.ts("T-01", "service_start") == [START + 3]
    assert tr.ts("T-02", "service_start") == [START + 3 + SERVICE_S]
    assert tr.phase("T-02", START + 2) == ("waiting", "S1")
    a = tr.rec("T-01", START + 3)
    assert dist_xy(xy(a), S1_XY) == pytest.approx(STOP_OFFSET_M, abs=EPS)
    assert dist_xy(xy(a), behind) == pytest.approx(32.0, abs=EPS)     # approached from its own side


# ------------------------------------------------------------------------------------------------------- redirects

S2_XY = (1203.0, 0.0)


def redirect_site() -> Site:
    return make_site({"S1": (0.0, D1), "S2": S2_XY}, {"T-01": "S1", "T-02": "S1"})


def test_redirect_accepted_then_unload_then_own_station():
    sim = Simulator(redirect_site(), start_ts=START, coord_precision=6)
    req = START + 20
    tr = run(sim, 1600, requests={req: [("T-01", "S2")]}, phases=True)

    applied = [e for e in tr.events if e.event == "redirect_applied"]
    assert applied == [TruthEvent(req, "T-01", "redirect_applied", detail={"from_station": "S1", "to_station": "S2"})]
    assert tr.phase("T-01", req) == ("to_station", "S2")
    p = (0.0, 200.0)                            # position at the end of tick req-1
    d = dist_xy(p, S2_XY)
    stop = (S2_XY[0] - STOP_OFFSET_M * (S2_XY[0] - p[0]) / d, S2_XY[1] - STOP_OFFSET_M * (S2_XY[1] - p[1]) / d)
    arrive = req + legs(d - STOP_OFFSET_M) - 1
    depart = arrive + SERVICE_S
    unload = depart + legs(dist_xy(stop, (0, 0))) - 1
    events = tr.of("T-01")
    assert [ev for ev in events if ev[1] != "radius_enter" and ev[1] != "radius_exit"][:6] == [
        (req, "redirect_applied", None), (arrive, "service_start", "S2"), (depart, "depart", "S2"),
        (unload, "unload", None), (tr.ts("T-01", "service_start", "S1")[0], "service_start", "S1"),
        (tr.ts("T-01", "depart", "S1")[0], "depart", "S1")]
    assert tr.ts("T-01", "radius_enter")[:2] == [tr.ts("T-01", "radius_enter", "S2")[0],
                                                tr.ts("T-01", "radius_enter", "S1")[0]]
    assert tr.ts("T-01", "radius_exit", "S2")[0] < unload
    assert dist_xy(xy(tr.rec("T-01", arrive)), stop) < EPS
    assert dist_xy(xy(tr.rec("T-01", req)), p) == pytest.approx(10.0, abs=EPS)
    assert tr.phase("T-01", depart) == ("to_unload", "U1")
    assert tr.phase("T-01", unload) == ("to_station", "S1")
    # back on its own station after unload, approached from the unload point
    arrive_s1 = depart + legs(dist_xy(stop, (0, 0)) + D1 - STOP_OFFSET_M) - 1   # carry-over at unload
    assert tr.rec("T-01", arrive_s1 - 1).speed_kmh == 36.0
    assert tr.rec("T-01", arrive_s1).speed_kmh == 0.0
    assert dist_xy(xy(tr.rec("T-01", arrive_s1)), (0.0, D1 - STOP_OFFSET_M)) < EPS
    s1_start = tr.ts("T-01", "service_start", "S1")[0]
    t02_departs = tr.ts("T-02", "depart", "S1")
    assert s1_start == max([arrive_s1] + [t for t in t02_departs if t <= s1_start])
    assert s1_start > arrive_s1          # T-02 (second cycle) was ahead in the S1 queue
    # T-02 was never redirected and is served at S1 first (T-01 left the S1 queue)
    assert tr.ts("T-02", "service_start", "S1")[0] == START + legs(D1 - STOP_OFFSET_M) - 1


def test_redirect_flag_resets_after_service():
    first = run(Simulator(redirect_site(), start_ts=START), 800, requests={START: [("T-01", "S2")]})
    unload = first.ts("T-01", "unload")[0]
    # second redirect on the next outbound trip (far from S1) is accepted again
    sim = Simulator(redirect_site(), start_ts=START)
    tr = run(sim, unload + 60 - START, requests={START: [("T-01", "S2")], unload + 50: [("T-01", "S2")]})
    assert [(e.ts, e.event) for e in tr.events if e.unit_uuid == "T-01" and e.event.startswith("redirect")] == \
        [(START, "redirect_applied"), (unload + 50, "redirect_applied")]
    assert (sim.unit_phase("T-01"), sim.unit_target("T-01")) == ("to_station", "S2")


def test_redirect_ignore_reasons():
    site = make_site({"S1": (0.0, D1), "S2": S2_XY, "S3": (-1500.0, -200.0)},
                     {"T-01": "S1", "T-02": "S1", "T-03": "S1"})
    sim = Simulator(site, start_ts=START, coord_precision=6, initial=[
        UnitInit("T-03", "to_station", target_station="S3", lat=LAT0, lon=LON0)])
    enter = START + math.floor((D1 - STATION_RADIUS_M) / 10)
    reqs = {
        START: [("T-99", "S2"), ("T-01", "S9"), ("T-01", "S1"), ("T-01", "S2"), ("T-01", "S3"),
                ("T-03", "S2")],
        enter + 1: [("T-02", "S2")],          # inside S1's radius, still driving
        enter + 10: [("T-02", "S2")],         # in service at S1
        START + 300: [("T-01", "S1")],        # same station wins over phase
    }
    baseline = run(Simulator(site, start_ts=START, coord_precision=6, initial=[
        UnitInit("T-03", "to_station", target_station="S3", lat=LAT0, lon=LON0)]), 400,
        requests={START: [("T-01", "S2")]})
    tr = run(sim, 400, requests=reqs, phases=True)

    at_start = [(e.unit_uuid, e.event, (e.detail or {}).get("reason")) for e in tr.tick_events[0]
                if e.event.startswith("redirect")]
    assert at_start == [
        ("T-01", "redirect_ignored", "unknown_station"), ("T-01", "redirect_ignored", "same_station"),
        ("T-01", "redirect_applied", None), ("T-01", "redirect_ignored", "already_redirected"),
        ("T-03", "redirect_ignored", "already_redirected"), ("T-99", "redirect_ignored", "unknown_unit")]
    assert tr.tick_events[0][-1].detail == {"to_station": "S2", "reason": "unknown_unit"}

    def reason(ts, uid):
        return [e.detail["reason"] for e in tr.tick_events[ts - START]
                if e.unit_uuid == uid and e.event == "redirect_ignored"]

    assert tr.phase("T-02", enter + 1)[0] == "to_station"
    assert reason(enter + 1, "T-02") == ["inside_radius"]
    assert tr.phase("T-02", enter + 10)[0] == "in_service"
    assert reason(enter + 10, "T-02") == ["not_to_station"]
    assert reason(START + 300, "T-01") == ["same_station"]
    # ignored redirects do not disturb the unit
    assert tr.series("T-02") == baseline.series("T-02")


def test_redirect_not_to_station_in_unload_leg_and_waiting():
    site = make_site({"S1": (0.0, D1), "S2": S2_XY}, {"T-01": "S1", "T-02": "S1"})
    sim = Simulator(site, start_ts=START, initial=[UnitInit("T-01", "in_service", service_elapsed_s=199),
                                                   UnitInit("T-02", "waiting")])
    tr = run(sim, 3, requests={START: [("T-02", "S2")], START + 2: [("T-01", "S2")]}, phases=True)
    assert tr.phase("T-01", START + 1) == ("to_unload", "U1")
    reasons = [(e.ts, e.unit_uuid, e.detail["reason"]) for e in tr.events if e.event == "redirect_ignored"]
    assert reasons == [(START, "T-02", "not_to_station"), (START + 2, "T-01", "not_to_station")]


def test_redirect_degenerate_start_near_target_center():
    # S2 lies on T-01's route; redirect while 10 m short of its center -> approach along the previous heading
    site = make_site({"S1": (0.0, D1), "S2": (0.0, 500.0)}, {"T-01": "S1"})
    sim = Simulator(site, start_ts=START, coord_precision=6)
    t = START + 49                          # position (0, 490) at the end of tick t-1
    tr = run(sim, 60, requests={t: [("T-01", "S2")]})
    assert [(e.event, e.station_uuid) for e in tr.tick_events[t - START]] == [
        ("redirect_applied", None), ("radius_enter", "S2"), ("service_start", "S2")]
    r = tr.rec("T-01", t)
    assert r.speed_kmh == 0.0
    assert dist_xy(xy(r), (0.0, 485.0)) < EPS


# ---------------------------------------------------------------------------------------------- initial states

def init_site() -> Site:
    return make_site({"S1": (0.0, D1), "S2": S2_XY},
                     {"T-01": "S1", "T-02": "S1", "T-03": "S1", "T-04": "S1", "T-05": "S2", "T-06": "S2",
                      "T-07": "S1", "T-08": "S1"})


def test_initial_states():
    p5 = (S2_XY[0] + 500.0, 0.0)
    p6 = (0.0, -304.0)
    p7 = (0.0, 300.0)
    initial = [
        UnitInit("T-01", "in_service", service_elapsed_s=150),
        UnitInit("T-02", "waiting", radius_enter_ts=START - 10),
        UnitInit("T-03", "waiting"),
        UnitInit("T-04", "waiting", target_station="S1", radius_enter_ts=START - 20),
        UnitInit("T-05", "to_station", lat=ll(p5)[0], lon=ll(p5)[1]),
        UnitInit("T-06", "to_unload", lat=ll(p6)[0], lon=ll(p6)[1]),
        UnitInit("T-07", "to_station", target_station="S2", lat=ll(p7)[0], lon=ll(p7)[1]),
    ]
    sim = Simulator(init_site(), start_ts=START, coord_precision=6, initial=initial)
    assert sim.now == START - 1
    assert [sim.unit_phase(u) for u in sim.unit_ids] == [
        "in_service", "waiting", "waiting", "waiting", "to_station", "to_unload", "to_station", "to_station"]
    assert [sim.unit_target(u) for u in sim.unit_ids] == ["S1", "S1", "S1", "S1", "S2", "U1", "S2", "S1"]
    assert sim.station_busy("S1") == "T-01"
    assert sim.station_queue("S1") == ["T-04", "T-02", "T-03"]
    tr = run(sim, 1200, requests={START + 5: [("T-07", "S2")]})

    stop1 = (0.0, D1 - STOP_OFFSET_M)
    for t in range(START, START + 50):
        r = tr.rec("T-01", t)
        assert r.speed_kmh == 0.0 and dist_xy(xy(r), stop1) < EPS
    assert tr.rec("T-01", START + 50).speed_kmh == 36.0
    assert tr.of("T-01")[0] == (START + 50, "depart", "S1")
    # S1 order: initial waiting units by radius_enter_ts (then uuid), then T-08 (entered at START+94 on its first
    # trip), then T-01 (re-entered after its first unload)
    assert tr.ts("T-08", "radius_enter")[0] == START + 94
    assert START + 94 < tr.ts("T-01", "radius_enter")[0] < START + 450
    firsts = sorted((tr.ts(u, "service_start", "S1")[0], u) for u in ("T-01", "T-02", "T-03", "T-04", "T-08"))
    assert firsts == [(START + 50 + k * SERVICE_S, u) for k, u in enumerate(["T-04", "T-02", "T-03", "T-08", "T-01"])]
    for uid in ("T-02", "T-03", "T-04"):
        assert tr.ts(uid, "radius_enter")[0] > tr.ts(uid, "depart")[0]    # no event for the initial state
        assert dist_xy(xy(tr.rec(uid, START)), stop1) < EPS and tr.rec(uid, START).speed_kmh == 0.0
    # to_station from a given position
    a5 = START + legs(500.0 - STOP_OFFSET_M) - 1
    assert dist_xy(xy(tr.rec("T-05", START)), p5) == pytest.approx(10.0, abs=EPS)
    assert tr.ts("T-05", "service_start")[0] == a5
    assert dist_xy(xy(tr.rec("T-05", a5)), (S2_XY[0] + STOP_OFFSET_M, 0.0)) < EPS
    # to_unload from a given position, then own station
    assert tr.ts("T-06", "unload")[0] == START + legs(304.0) - 1
    assert tr.ts("T-06", "radius_enter")[0] > START + 30
    assert tr.rec("T-06", START + 30) == Telemetry("T-06", START + 30, LAT0, LON0, 0.0)
    # to_station with a non-own target counts as redirected: served at S2 after T-05, then unload, then S1
    assert [e.detail["reason"] for e in tr.events if e.unit_uuid == "T-07" and e.event == "redirect_ignored"] == \
        ["already_redirected"]
    a7 = START + legs(dist_xy(p7, S2_XY) - STOP_OFFSET_M) - 1
    assert a7 < a5 + SERVICE_S
    assert tr.ts("T-07", "service_start")[0] == a5 + SERVICE_S
    assert tr.ts("T-07", "service_start", "S2") == [a5 + SERVICE_S]
    assert tr.ts("T-07", "unload")[0] < tr.ts("T-07", "radius_enter", "S1")[0]
    # T-08 started at the unload point (default)
    assert dist_xy(xy(tr.rec("T-08", START)), (0, 0)) == pytest.approx(10.0, abs=EPS)


@pytest.mark.parametrize("elapsed, depart", [(0, START + SERVICE_S), (SERVICE_S - 1, START + 1)])
def test_initial_in_service_bounds(elapsed, depart):
    sim = Simulator(init_site(), start_ts=START, initial=[UnitInit("T-01", "in_service",
                                                                   service_elapsed_s=elapsed)])
    tr = run(sim, SERVICE_S + 2)
    assert tr.ts("T-01", "depart") == [depart]
    assert tr.ts("T-01", "service_start") == []          # initial states emit no events of their own
    assert tr.rec("T-01", depart - 1).speed_kmh == 0.0 and tr.rec("T-01", depart).speed_kmh == 36.0


def test_initial_waiting_at_free_station_starts_service_at_start():
    sim = Simulator(init_site(), start_ts=START, maneuver_s=30,
                    initial=[UnitInit("T-02", "waiting"), UnitInit("T-01", "waiting")])
    tr = run(sim, 1)
    assert [(e.unit_uuid, e.event) for e in tr.events] == [("T-01", "service_start")]
    assert sim.station_queue("S1") == ["T-02"]


@pytest.mark.parametrize("initial, match", [
    ([UnitInit("T-99", "waiting")], "unknown unit"),
    ([UnitInit("T-01", "waiting"), UnitInit("T-01", "waiting")], "duplicate"),
    ([UnitInit("T-01", "waiting", target_station="S9")], "unknown station"),
    ([UnitInit("T-01", "parked")], "unknown phase"),                                  # type: ignore[arg-type]
    ([UnitInit("T-01", "to_station")], "lat/lon"),
    ([UnitInit("T-01", "to_unload", lat=LAT0)], "lat/lon"),
    ([UnitInit("T-01", "to_station", lat=float("nan"), lon=LON0)], "lat/lon"),
    ([UnitInit("T-01", "to_station", lat=91.0, lon=LON0)], "lat/lon"),
    ([UnitInit("T-01", "waiting", lat=LAT0, lon=LON0)], "not allowed"),
    ([UnitInit("T-01", "to_unload", target_station="S1", lat=LAT0, lon=LON0)], "target_station"),
    ([UnitInit("T-01", "in_service"), UnitInit("T-02", "in_service")], "already has T-01"),
    ([UnitInit("T-01", "in_service", service_elapsed_s=SERVICE_S)], "service_elapsed_s"),
    ([UnitInit("T-01", "in_service", service_elapsed_s=-1)], "service_elapsed_s"),
    ([UnitInit("T-01", "waiting", service_elapsed_s=5)], "service_elapsed_s"),
    ([UnitInit("T-01", "in_service", radius_enter_ts=START)], "radius_enter_ts"),
    ([UnitInit("T-01", "waiting", radius_enter_ts=START + 1)], "radius_enter_ts"),
    (["T-01"], "UnitInit"),
])
def test_initial_invalid(initial, match):
    with pytest.raises(ValueError, match=match):
        Simulator(init_site(), start_ts=START, initial=initial)


def test_in_service_at_different_stations_is_fine():
    sim = Simulator(init_site(), start_ts=START, initial=[
        UnitInit("T-01", "in_service"), UnitInit("T-02", "in_service", target_station="S2")])
    assert (sim.station_busy("S1"), sim.station_busy("S2")) == ("T-01", "T-02")


@pytest.mark.parametrize("kwargs, match", [
    ({"service_s": 0}, "service_s"), ({"maneuver_s": -1}, "maneuver_s"), ({"coord_precision": -1}, "coord"),
    ({"coord_precision": 13}, "coord"), ({"start_ts": 1.5}, "start_ts"), ({"start_ts": True}, "start_ts"),
])
def test_invalid_parameters(kwargs, match):
    args = {"start_ts": START} | kwargs
    with pytest.raises(ValueError, match=match):
        Simulator(init_site(), **args)


@pytest.mark.parametrize("site, match", [
    (make_site({"S1": (0.0, 60.0)}, {"T-01": "S1"}), "from the unload point"),
    (make_site({"S1": (0.0, D1)}, {"T-01": "S7"}), "unknown station"),
    (make_site({"S1": (0.0, D1)}, {}), "no units"),
    (make_site({}, {"T-01": "S1"}), "no stations"),
])
def test_invalid_site(site, match):
    with pytest.raises(ValueError, match=match):
        Simulator(site, start_ts=START)


# ------------------------------------------------------------------------------- long run: determinism, invariants

def decimals(v: float) -> int:
    return max(0, -decimal.Decimal(repr(v)).as_tuple().exponent)


def redirect_pattern(site: Site, n: int, every: int = 37) -> dict[int, list[tuple[str, str]]]:
    units, stations = site.unit_ids, site.station_ids
    return {START + k: [(units[(k * 7) % len(units)], stations[(k // every) % len(stations)])]
            for k in range(0, n, every)}


def test_deterministic():
    site = generate_site(seed=7)
    a = run(Simulator(site, start_ts=START), 3600, requests=redirect_pattern(site, 3600))
    b = run(Simulator(site, start_ts=START), 3600, requests=redirect_pattern(site, 3600))
    assert a.ticks == b.ticks and a.tick_events == b.tick_events
    assert a.events


def test_long_run_invariants():
    site = generate_site(seed=7)
    proj = projection_for(site)
    centers = {s.uuid: proj.to_xy(s.lat, s.lon) for s in site.stations}
    n = 4 * 3600
    sim = Simulator(site, start_ts=START, coord_precision=6)
    tr = run(sim, n, requests=redirect_pattern(site, n))

    kinds = defaultdict(int)
    for e in tr.events:
        kinds[e.event] += 1
    assert kinds["redirect_applied"] > 5 and kinds["redirect_ignored"] > 5
    assert kinds["service_start"] > 100

    # per tick: one record per unit, sorted; events sorted by unit, then fixed order
    rank = {name: i for i, name in enumerate(EVENT_ORDER)}
    rank["redirect_ignored"] = rank["redirect_applied"]
    for i, (recs, evs) in enumerate(zip(tr.ticks, tr.tick_events)):
        assert [r.unit_uuid for r in recs] == site.unit_ids
        keys = [(e.unit_uuid, rank[e.event]) for e in evs]
        assert keys == sorted(keys), START + i

    # services: one at a time per station, exactly service_s long, standing still
    busy: dict[str, tuple[str, int]] = {}
    services = []
    by_tick = [sorted(evs, key=lambda e: e.event != "depart") for evs in tr.tick_events]
    for e in (e for evs in by_tick for e in evs):
        if e.event == "service_start":
            assert e.station_uuid not in busy, e
            busy[e.station_uuid] = (e.unit_uuid, e.ts)
        elif e.event == "depart":
            uid, t0 = busy.pop(e.station_uuid)
            assert uid == e.unit_uuid and e.ts - t0 == SERVICE_S
            services.append((uid, e.station_uuid, t0))
    assert len(services) > 100
    for uid, sid, t0 in services:
        first = tr.rec(uid, t0)
        assert dist_xy(xy(first, proj), centers[sid]) == pytest.approx(STOP_OFFSET_M, abs=EPS)
        assert all(tr.rec(uid, t) == Telemetry(uid, t, first.lat, first.lon, 0.0) for t in range(t0, t0 + SERVICE_S))
        assert tr.rec(uid, t0 + SERVICE_S).speed_kmh == 36.0

    # FIFO: service order at each station == radius-entry order (tick, unit_uuid)
    entered = defaultdict(list)
    started = defaultdict(list)
    for e in tr.events:
        if e.event == "radius_enter":
            entered[e.station_uuid].append((e.ts, e.unit_uuid))
        elif e.event == "service_start":
            started[e.station_uuid].append(e.unit_uuid)
    for sid in site.station_ids:
        order = [u for _, u in sorted(entered[sid])]
        assert started[sid] == order[:len(started[sid])], sid
        assert len(order) - len(started[sid]) <= len(site.unit_ids)

    # records: speed values, rounding, displacement, where units stand
    unloads = {(e.unit_uuid, e.ts) for e in tr.events if e.event == "unload"}
    for uid in site.unit_ids:
        series = tr.series(uid)
        for a, b in zip(series, series[1:]):
            step = dist_xy(xy(a, proj), xy(b, proj))
            assert step <= (20.0 if (uid, a.ts) in unloads else 10.0) + EPS, (uid, b.ts)
        for r in series:
            assert decimals(r.lat) <= 6 and decimals(r.lon) <= 6
            assert r.speed_kmh in (0.0, 36.0)
            if r.speed_kmh == 0.0:
                p = xy(r, proj)
                at_stop = min(abs(dist_xy(p, c) - STOP_OFFSET_M) for c in centers.values()) < EPS
                assert at_stop or (uid, r.ts) in unloads, (uid, r.ts)
                if (uid, r.ts) in unloads:
                    assert (r.lat, r.lon) == (site.unload.lat, site.unload.lon)


# ------------------------------------------------------------------------------------------------------- rounding

@pytest.mark.parametrize("precision", [4, 6])
def test_coord_rounding(precision):
    site = generate_site(seed=7)
    sim = Simulator(site, start_ts=START, coord_precision=precision)
    tr = run(sim, 1500)
    values = [v for recs in tr.ticks for r in recs for v in (r.lat, r.lon)]
    assert max(decimals(v) for v in values) == precision
    assert all(round(v, precision) == v for v in values)
