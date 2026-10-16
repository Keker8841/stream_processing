"""The task's worked example (gen/scenarios/task_example.yaml) run through the generator's simulator and its scripted
delays (T4's 11:59:01..11:59:59 records arrive at 12:00:01), no random faults."""

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from generator.faults import FaultConfig, FaultInjector
from generator.scenario import load_scenario
from generator.sim import Simulator
from vqservice.config import EngineConfig
from vqservice.engine import Engine
from vqservice.geo import haversine_m
from vqservice.site import site_from_dict
from vqservice.timefmt import parse_iso

SCENARIO = Path(__file__).resolve().parents[2] / "gen" / "scenarios" / "task_example.yaml"
T1200 = parse_iso("2026-09-15T12:00:00Z")


def _run(until: int = T1200 + 1, **cfg) -> SimpleNamespace:
    """Feed the scenario's emissions in delivery order up to the `until` tick."""
    sc = load_scenario(SCENARIO)
    site = site_from_dict(yaml.safe_load(SCENARIO.read_text(encoding="utf-8"))["site"])
    sim = Simulator(sc.site, start_ts=sc.start_ts, service_s=sc.service_s, maneuver_s=sc.maneuver_s,
                    coord_precision=sc.coord_precision, initial=sc.initial)
    inj = FaultInjector(FaultConfig.none(), 1, sc.delays)
    eng = Engine(site, EngineConfig(**cfg))
    r = SimpleNamespace(expect=sc.expect, decisions=[], before=None, after=None, t4_outputs=None, positions={},
                        late_outputs=[])
    while sim.now < until:
        records, _ = sim.step()
        inj.push(sim.now, records)
        for em in inj.pop_due(sim.now):
            rec = em.value
            r.positions[(rec["unit_uuid"], rec["ts"])] = (rec["lat"], rec["lon"])
            t4_now = rec["ts"] == T1200 and rec["unit_uuid"] == "T4"
            if t4_now:
                r.before = eng.queues()
            out = eng.process(rec)
            r.decisions += [(rec["ts"], o) for o in out if o.kind == "decision"]
            if t4_now:
                r.t4_outputs = [(o.kind, o.key, o.value) for o in out]
                r.after = eng.queues()
            if em.emit_at > rec["ts"]:
                r.late_outputs += out
    return r


def _expected(queue: list[dict]) -> list[dict]:
    """YAML form -> queue.v1 items (the occupant T1 has eta null in the task's format)."""
    return [{"unit_uuid": q["unit_uuid"], "eta": None if q["unit_uuid"] == "T1" else q["eta"],
             "service_start": q["service_start"], "free_at": q["free_at"], "wait_seconds": q["wait_s"]}
            for q in queue]


def test_geometry_matches_the_scenario_comments():
    r = _run()
    st = {s.uuid: s for s in load_scenario(SCENARIO).site.stations}
    for (u, t, sid), d in {("T4", T1200 - 60, "S1"): 1502.003, ("T4", T1200, "S1"): 902.016,
                           ("T4", T1200, "S2"): 2400.031, ("T2", T1200 - 29, "S1"): 1492.034,
                           ("T2", T1200 - 30, "S1"): 1502.003, ("T3", T1200, "S1"): 3001.968}.items():
        assert haversine_m(*r.positions[(u, t)], st[sid].lat, st[sid].lon) == pytest.approx(d, abs=1e-3)


def test_queue_and_t4_decision_at_1200():
    r = _run()
    e = r.expect["decision"]
    assert r.before == {sid: _expected(q) for sid, q in r.expect["queues"].items()}      # the task's table
    assert r.after == {sid: _expected(q) for sid, q in r.expect["queues_after_decision"].items()}
    [(ts, d)] = [(ts, o) for ts, o in r.decisions if o.key == "T4"]
    assert ts == T1200
    assert d.value == {k: e[k] for k in ("unit_uuid", "at", "result", "from_station", "to_station", "gain_seconds")}
    assert d.headers == (("decision-id", f"T4:{T1200}"),)
    # outputs of T4's 12:00:00 message: the decision, then S2's queue with T4; S1 is unchanged (T4 was not in it)
    assert r.t4_outputs == [("decision", "T4", d.value),
                            ("queue", "S2", {"station_uuid": "S2", "at": e["at"],
                                             "queue": _expected(r.expect["queues_after_decision"]["S2"])})]
    assert r.late_outputs == []                     # the late 11:59:xx records of T4 are ignored


@pytest.mark.parametrize("metric", ["queue_wait", "service_end"])
def test_t2_rejected_no_gain_at_1159_31(metric):
    r = _run(metric=metric)
    [early] = r.expect["earlier_decisions"]
    [(ts, d)] = [(ts, o) for ts, o in r.decisions if o.key == "T2"]
    assert (ts, d.value["at"]) == (parse_iso(early["at"]), early["at"])
    assert (d.value["result"], d.value["reason"]) == (early["result"], early["reason"])


def test_service_end_metric_rejects_t4():
    r = _run(metric="service_end")
    [d] = [o for _, o in r.decisions if o.key == "T4"]
    assert d.value == r.expect["decision_service_end"]
    assert r.after == {sid: _expected(q) for sid, q in r.expect["queues_shadow"].items()}   # T4 stays at S1


def test_shadow_mode_keeps_t4_in_s1():
    r = _run(apply_decisions=False)
    assert [o.value["gain_seconds"] for _, o in r.decisions if o.key == "T4"] == [80]
    assert r.after == {sid: _expected(q) for sid, q in r.expect["queues_shadow"].items()}
