"""Review: restore(snapshot()) at ANY point continues exactly like the uninterrupted run."""

import json

from generator.faults import FaultConfig, FaultInjector
from generator.sim import Simulator
from generator.site import generate_site
from vqservice.config import EngineConfig
from vqservice.engine import Engine
from vqservice.site import Point, Site

FAR = 30_000.0
EAST = 90.0


def _outputs(site, cfg, messages, restore_at=()):
    eng = Engine(site, cfg)
    out = []
    for k, m in enumerate(messages):
        if k in restore_at:
            snap = json.loads(json.dumps(eng.snapshot()))
            eng = Engine.restore(site, cfg, snap)
            assert eng.snapshot() == snap
        out += [(o.kind, o.key, o.value, o.headers) for o in eng.process(m)]
    return out, eng.stats


def _scripted(world):
    """A stream that passes through every special state: redirect, cold-start decision, occupant with a pending
    timer, a timer-released unit still inside the radius (late backlog), a same-second takeover, an unseen service."""
    w = world()
    T0 = w.T0
    w.stand("T1", T0 - 60)                        # occupant, timer at T0 + 170
    w.move("T4", T0 - 1, 1506.0)
    w.move("T2", T0, 1200.0)
    w.move("T3", T0, 3000.0)
    w.move("T4", T0, 1496.0)                      # recommended S2: redirected
    w.move("A", T0 + 1, 1001.0, EAST)             # first message inside 1500 m: decision at once
    w.move("A", T0 + 2, 991.0, EAST)
    w.move("A", T0 + 3, 980.0, EAST)
    w.stand("T1", T0 + 100)                       # T1's telemetry is then held
    for ts in range(T0 + 160, T0 + 181, 5):
        w.move("X", ts, FAR, EAST)                # now passes T0 + 170: timer release, T1 still inside
    w.stand("T1", T0 + 175)                       # late backlog: served, does not occupy again
    w.move("T1", T0 + 178, 57.0)                  # leaves -> to_unload
    w.stand("C", T0 + 180, 15.0, EAST)            # first seen standing: occupies
    w.stand("B", T0 + 180, 15.0, EAST)            # same second, ahead by unit_uuid: takes over
    w.stand("C", T0 + 190, 15.0, EAST)
    w.stand("B", T0 + 190, 15.0, EAST)
    w.move("C", T0 + 200, 57.0, EAST)             # C leaves: it was the one served, B's occupancy is undone
    w.stand("B", T0 + 201, 15.0, EAST)            # B occupies again
    w.stand("T4", T0 + 240, 15.0, 0.0, ref="S2")  # redirected T4 arrives at S2 and occupies it
    w.move("T4", T0 + 450, 60.0, 0.0, ref="S2")
    return w


def test_restore_at_every_message_of_a_scripted_stream(world):
    messages = []
    orig = Engine.process

    def record(self, raw):
        messages.append(dict(raw))
        return orig(self, raw)

    Engine.process = record
    try:
        w = _scripted(world)
    finally:
        Engine.process = orig
    s = w.engine.stats
    assert (s["decisions"], s["releases"]) == (3, 3)  # T2 and A (first messages inside 1500 m), T4
    assert s["occupancies"] == 5                  # T1, C, B (takeover), B (again), T4
    ref, ref_stats = _outputs(w.site, w.cfg, messages)
    for k in range(len(messages) + 1):
        got, got_stats = _outputs(w.site, w.cfg, messages, {k})
        assert (got, got_stats) == (ref, ref_stats), k


def test_restore_before_every_message_of_a_generator_stream(monkeypatch):
    """Site 4 at start: wrong cold-start occupant guesses get undone at S1; faults on, closed loop."""
    g = generate_site(4)
    site = Site(Point(g.unload.uuid, g.unload.lat, g.unload.lon),
                tuple(Point(s.uuid, s.lat, s.lon) for s in g.stations), dict(g.assignment))
    cfg = EngineConfig()
    unseen = []
    orig = Engine._served_unseen
    monkeypatch.setattr(Engine, "_served_unseen", lambda self, i, u, ts: (unseen.append(ts), orig(self, i, u, ts)))

    def run(every: bool):
        sim = Simulator(g, start_ts=1789473600 - 3600, maneuver_s=30, coord_precision=4)
        for _ in range(3600):
            sim.step()
        inj = FaultInjector(FaultConfig(), seed=9)
        eng, out, pending = Engine(site, cfg), [], []
        for _ in range(420):
            for uid, to in pending:
                sim.request_redirect(uid, to)
            pending = []
            records = sim.step()[0]
            inj.push(sim.now, records)
            for em in inj.pop_due(sim.now):
                if every:
                    eng = Engine.restore(site, cfg, json.loads(json.dumps(eng.snapshot())))
                for o in eng.process(em.value):
                    out.append((o.kind, o.key, o.value, o.headers))
                    if o.kind == "decision" and o.value["result"] == "recommended":
                        pending.append((o.key, o.value["to_station"]))
        return out, eng.stats

    ref = run(False)
    n_unseen = len(unseen)
    assert run(True) == ref
    assert n_unseen > 0 and ref[1]["recommended"] > 0 and ref[1]["ignored_old"] > 0
