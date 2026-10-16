"""Snapshot/restore mid-stream continues byte-identically (generator in-process, faults on)."""

import json

import pytest

from generator.faults import FaultConfig, FaultInjector
from generator.sim import Simulator
from generator.site import generate_site
from vqservice.config import EngineConfig
from vqservice.engine import Engine
from vqservice.site import Point, Site

START = 1789473600
WARMUP_S = 3600
DURATION_S = 1200


def _site(g) -> Site:
    return Site(Point(g.unload.uuid, g.unload.lat, g.unload.lon),
                tuple(Point(s.uuid, s.lat, s.lon) for s in g.stations), dict(g.assignment))


def _run(cfg: EngineConfig, checkpoints: frozenset[int], closed: bool):
    """Feed a faulty generator stream; at every checkpoint (message index) replace the engine by
    restore(json(snapshot)). Closed loop: recommendations are fed back to the simulator."""
    g = generate_site(42)
    site = _site(g)
    sim = Simulator(g, start_ts=START - WARMUP_S, maneuver_s=30, coord_precision=4)
    for _ in range(WARMUP_S):
        sim.step()
    faults = FaultInjector(FaultConfig(), seed=5)
    eng = Engine(site, cfg)
    outs: list[tuple] = []
    applied = 0
    n = 0

    def feed(emissions):
        nonlocal eng, n
        for em in emissions:
            if n in checkpoints:
                snap = json.loads(json.dumps(eng.snapshot()))
                eng = Engine.restore(site, cfg, snap)
                assert eng.snapshot() == snap
            n += 1
            for o in eng.process(em.value):
                outs.append((o.kind, o.key, json.dumps(o.value), o.headers))
                if closed and o.kind == "decision" and o.value["result"] == "recommended":
                    sim.request_redirect(o.key, o.value["to_station"])

    for _ in range(DURATION_S):
        records, events = sim.step()
        applied += sum(e.event == "redirect_applied" for e in events)
        faults.push(sim.now, records)
        feed(faults.pop_due(sim.now))
    feed(faults.drain())
    return outs, eng.stats, applied


@pytest.mark.parametrize("closed", [False, True], ids=["open_loop", "closed_loop"])
def test_restore_mid_stream_continues_identically(closed):
    cfg = EngineConfig()
    ref, ref_stats, ref_applied = _run(cfg, frozenset(), closed)
    got, got_stats, got_applied = _run(cfg, frozenset({0, 1, 777, 9_999, 23_456, 40_000, 47_000}), closed)
    assert got == ref
    assert got_stats == ref_stats
    assert got_applied == ref_applied
    # the stream exercised the interesting paths
    assert ref_stats["ignored_old"] > 0 and ref_stats["occupancies"] > 20 and ref_stats["releases"] > 20
    assert ref_stats["recommended"] > 0 and ref_stats["rejected_no_gain"] > 0
    if closed:
        assert ref_applied > 0
