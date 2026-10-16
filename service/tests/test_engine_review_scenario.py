"""Review: the task example (gen/scenarios/task_example.yaml) up to T3's crossing at 12:02:31 (numbers in the YAML
comments): T1 leaves the radius at 12:02:23, before its 230 s timer, and the FIFO head at S1 occupies it right then."""

from pathlib import Path

import pytest
import yaml

from generator.faults import FaultConfig, FaultInjector
from generator.scenario import load_scenario
from generator.sim import Simulator
from vqservice.config import EngineConfig
from vqservice.engine import Engine
from vqservice.site import site_from_dict
from vqservice.timefmt import parse_iso

SCENARIO = Path(__file__).resolve().parents[2] / "gen" / "scenarios" / "task_example.yaml"


def _run_until(until: str, closed: bool, **cfg):
    sc = load_scenario(SCENARIO)
    site = site_from_dict(yaml.safe_load(SCENARIO.read_text(encoding="utf-8"))["site"])
    sim = Simulator(sc.site, start_ts=sc.start_ts, service_s=sc.service_s, maneuver_s=sc.maneuver_s,
                    coord_precision=sc.coord_precision, initial=sc.initial)
    inj = FaultInjector(FaultConfig.none(), 1, sc.delays)
    eng = Engine(site, EngineConfig(apply_decisions=closed, **cfg))
    decisions, pending = {}, []
    while sim.now < parse_iso(until):
        for p in pending:
            sim.request_redirect(*p)
        pending = []
        records, _ = sim.step()
        inj.push(sim.now, records)
        for em in inj.pop_due(sim.now):
            for o in eng.process(em.value):
                if o.kind == "decision":
                    decisions[o.key] = o.value
                    if closed and o.value["result"] == "recommended":
                        pending.append((o.key, o.value["to_station"]))
    return eng, decisions


@pytest.mark.parametrize("closed, head", [(True, "T2"), (False, "T4")])
def test_t1_exit_frees_s1_before_its_timer(closed, head):
    eng, _ = _run_until("2026-09-15T12:02:30Z", closed=closed)
    s1 = eng.queues()["S1"]
    assert s1[0] == {"unit_uuid": head, "eta": None, "service_start": "2026-09-15T12:02:23Z",
                     "free_at": "2026-09-15T12:06:13Z", "wait_seconds": 0}


@pytest.mark.parametrize("closed, metric, expected", [
    (True, "queue_wait", ("recommended", "S3", 73)),
    (True, "service_end", ("recommended", "S2", 205)),     # T4 was not redirected with this metric
    (False, "queue_wait", ("recommended", "S2", 303)),
    (False, "service_end", ("recommended", "S2", 205)),
])
def test_t3_decision_at_12_02_31(closed, metric, expected):
    _, decisions = _run_until("2026-09-15T12:02:31Z", closed=closed, metric=metric)
    d = decisions["T3"]
    assert d["at"] == "2026-09-15T12:02:31Z"
    assert (d["result"], d.get("to_station"), d.get("gain_seconds")) == expected
