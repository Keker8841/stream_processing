import dataclasses
import threading
from pathlib import Path

import pytest

from generator.constants import MAX_LATENESS_S
from generator.faults import FaultConfig
from generator.model import Emission, Redirect
from generator.recording import JsonlWriter, read_record
from generator.runner import KafkaSink, RecordSink, RunConfig, run
from generator.scenario import load_scenario, parse_ts
from generator.site import load_site

ROOT = Path(__file__).resolve().parent.parent
SCENARIO = ROOT / "scenarios" / "task_example.yaml"
T_1200 = 1789473600


def ts(text: str) -> int:
    return parse_ts(text)


class ListSink:
    def __init__(self, name="list", on_send=None):
        self.name = name
        self.items: list[Emission] = []
        self.ticks = 0
        self.closed = False
        self.on_send = on_send

    def send(self, em):
        self.items.append(em)
        if self.on_send:
            self.on_send(em)

    def tick(self):
        self.ticks += 1

    def close(self):
        self.closed = True
        return {"items": len(self.items)}


class ListTruth:
    def __init__(self):
        self.events: list[dict] = []
        self.closed = False

    def write(self, obj):
        self.events.append(obj)

    def flush(self):
        pass

    def close(self):
        self.closed = True


class FakeFollower:
    """Returns whatever was queued since the last poll (a stand-in for DecisionFollower)."""

    def __init__(self, queued=()):
        self.queue = list(queued)
        self.polls = 0
        self.closed = False
        self.malformed = self.ignored = self.stale = self.errors = 0

    def poll(self):
        self.polls += 1
        out, self.queue = self.queue, []
        return out

    def close(self):
        self.closed = True


class FakePacer:
    speed = 0.0

    def __init__(self, raise_at=None):
        self.calls: list[float] = []
        self.raise_at = raise_at

    def wait_until(self, off):
        self.calls.append(off)
        if self.raise_at is not None and off >= self.raise_at:
            raise KeyboardInterrupt

    def behind_s(self, off):
        return 0.0


def scenario_cfg(**kw) -> RunConfig:
    sc = load_scenario(SCENARIO)
    base = dict(site=sc.site, start_ts=sc.start_ts, duration_s=sc.duration_s, initial=sc.initial,
                service_s=sc.service_s, maneuver_s=sc.maneuver_s, coord_precision=sc.coord_precision,
                faults=FaultConfig.none(), delays=sc.delays, speed=0)
    base.update(kw)
    return RunConfig(**base)


def site_cfg(**kw) -> RunConfig:
    base = dict(site=load_site(ROOT.parent / "site.yaml"), start_ts=T_1200, duration_s=600, warmup_s=1800, seed=7, speed=0)
    base.update(kw)
    return RunConfig(**base)


def events_of(truth: ListTruth, unit: str) -> list[tuple[str, int, str | None]]:
    return [(e["event"], e["ts"], e.get("station_uuid")) for e in truth.events if e["unit_uuid"] == unit]


def first_stop(items: list[Emission], unit: str, after: int) -> int:
    """ts of the first standing record of `unit` at or after `after` (arrival at a stop point)."""
    return min(em.value["ts"] for em in items
               if em.key == unit and em.value["ts"] >= after and em.value["speed_kmh"] == 0.0)


def test_scenario_open_loop_matches_expected_truth():
    sc = load_scenario(SCENARIO)
    sink, truth = ListSink(), ListTruth()
    summary = run(scenario_cfg(), sinks=[sink], truth=truth)
    assert summary.ticks == sc.duration_s and not summary.interrupted
    expect = {u: {k: ts(v) for k, v in d.items()} for u, d in sc.expect["generator_truth_open_loop"].items()}
    for unit, exp in expect.items():
        ev = events_of(truth, unit)
        if exp["service_start"] > sc.start_ts:     # T1 is in service from the initial state (no event)
            assert ("service_start", exp["service_start"], "S1") in ev
        assert ("depart", exp["depart"], "S1") in ev
        if "arrive" in exp:
            assert first_stop(sink.items, unit, sc.start_ts) == exp["arrive"]
    # T1 starts in service (initial state, no service_start event of its own)
    assert not any(e == "service_start" for e, _, _ in events_of(truth, "T1"))
    assert summary.redirects_applied == summary.redirects_ignored == 0
    # every unit emits exactly once per tick, in tick order (no random faults), except T4's 11:59:01..11:59:59
    # records: they come at 12:00:01, after T4's 12:00:00 record and before the 12:00:01 records
    (d,) = sc.delays
    held = [(d.emit_at, "T4", t) for t in range(d.from_ts, d.to_ts + 1)]
    expected = []
    for t in range(sc.start_ts, sc.start_ts + sc.duration_s):
        if t == d.emit_at:
            expected += held
        expected += [(t, u, t) for u in ("T1", "T2", "T3", "T4") if not (u == "T4" and d.from_ts <= t <= d.to_ts)]
    assert [(em.emit_at, em.key, em.value["ts"]) for em in sink.items] == expected
    assert summary.faults["scripted"] == 59 and summary.faults["max_lateness"] == 60


class ServiceStub:
    """Plays the external service: when it sees T4's 12:00:00 record it publishes the T4 -> S2 decision."""

    def __init__(self, follower: FakeFollower):
        self.follower = follower
        self.name = "service"
        self.sent = False

    def send(self, em):
        if not self.sent and em.key == "T4" and em.value["ts"] == T_1200:
            self.follower.queue.append(Redirect("T4", "S2", T_1200))
            self.sent = True

    def tick(self):
        pass

    def close(self):
        return {}


def test_scenario_closed_loop_redirect():
    follower = FakeFollower()
    sink, truth = ListSink(), ListTruth()
    summary = run(scenario_cfg(), sinks=[sink, ServiceStub(follower)], truth=truth, follower=follower)
    t4 = events_of(truth, "T4")
    applied = [e for e in truth.events if e["event"] == "redirect_applied"]
    assert applied == [{"ts": ts("2026-09-15T12:00:01Z"), "unit_uuid": "T4", "event": "redirect_applied",
                        "detail": {"from_station": "S1", "to_station": "S2"}}]
    # T4 drives 2400 - 15 m straight to the S2 stop point: 238.5 s from 12:00:00 -> standing at 12:03:59
    assert ("service_start", ts("2026-09-15T12:03:59Z"), "S2") in t4
    assert ("depart", ts("2026-09-15T12:07:19Z"), "S2") in t4
    assert first_stop(sink.items, "T4", T_1200) == ts("2026-09-15T12:03:59Z")
    assert not any(st == "S1" and e in ("radius_enter", "service_start") for e, _, st in t4)
    # S1 without T4: T3 starts right after T2's departure (12:06:10) + 30 s maneuver
    assert ("service_start", ts("2026-09-15T12:06:40Z"), "S1") in events_of(truth, "T3")
    assert ("depart", ts("2026-09-15T12:10:00Z"), "S1") in events_of(truth, "T3")
    assert (summary.redirects_received, summary.redirects_applied, summary.redirects_ignored) == (1, 1, 0)
    assert summary.follower == {"malformed": 0, "ignored": 0, "stale": 0, "errors": 0}
    assert follower.closed and follower.polls == summary.ticks


def test_early_redirect_waits_for_its_event_time():
    # Delivered before the run starts: still applied on the tick after `at`, never earlier.
    follower = FakeFollower([Redirect("T4", "S2", T_1200)])
    truth = ListTruth()
    summary = run(scenario_cfg(), sinks=[ListSink()], truth=truth, follower=follower)
    applied = [e["ts"] for e in truth.events if e["event"] == "redirect_applied"]
    assert applied == [T_1200 + 1]
    assert summary.redirects_pending == 0


def test_redirect_ignored_and_future_pending():
    follower = FakeFollower([Redirect("T1", "S2", 0), Redirect("T2", "S1", 0), Redirect("T2", "S9", 10**10)])
    truth = ListTruth()
    summary = run(scenario_cfg(duration_s=5), truth=truth, follower=follower)
    ignored = [(e["unit_uuid"], e["detail"]["reason"]) for e in truth.events if e["event"] == "redirect_ignored"]
    assert ignored == [("T1", "not_to_station"), ("T2", "same_station")]
    assert (summary.redirects_received, summary.redirects_ignored, summary.redirects_pending) == (3, 2, 1)


def test_determinism_byte_identical_records(tmp_path):
    paths = []
    for i, seed in enumerate((7, 7, 8)):
        rec, tr = tmp_path / f"r{i}.jsonl", tmp_path / f"t{i}.jsonl"
        run(site_cfg(seed=seed), sinks=[RecordSink(str(rec))], truth=JsonlWriter(tr))
        paths.append((rec.read_bytes(), tr.read_bytes()))
    assert paths[0] == paths[1]
    assert paths[0][0] != paths[2][0]
    assert paths[0][1] == paths[2][1]          # faults never change the physics


def test_lateness_order_and_summary_consistency(tmp_path):
    rec = tmp_path / "rec.jsonl"
    sink, truth = ListSink(), ListTruth()
    summary = run(site_cfg(), sinks=[sink, RecordSink(str(rec))], truth=truth)
    ems = list(read_record(rec))
    assert ems == sink.items
    units = len(site_cfg().site.assignment)
    assert summary.ticks == 600 and summary.warmup_ticks == 1800
    assert summary.records == 600 * units == summary.faults["records_in"]
    assert summary.emissions == len(ems) == summary.records + summary.faults["duplicates"]
    assert summary.faults["emitted"] == summary.emissions and summary.faults["pending"] == 0
    assert summary.sinks == {"list": {"items": len(ems)}, f"record:{rec}": {"lines": len(ems)}}
    assert summary.truth_events == len(truth.events) > 0
    assert summary.end_ts == T_1200 + 599 and summary.ok
    lateness = [em.emit_at - em.value["ts"] for em in ems]
    assert 0 <= min(lateness) and max(lateness) <= MAX_LATENESS_S
    assert summary.faults["max_lateness"] == max(lateness)
    emit_at = [em.emit_at for em in ems]
    assert emit_at == sorted(emit_at)
    # warmup is silent: nothing before start_ts, and the drained tail are the only emit_at > last tick
    assert min(em.value["ts"] for em in ems) == T_1200
    assert sum(1 for em in ems if em.emit_at > summary.end_ts) == summary.drained
    assert all(t["ts"] >= T_1200 for t in truth.events)


def test_pacer_called_per_tick_and_sinks_ticked():
    pacer = FakePacer()
    pacer.speed = 2.0
    sink = ListSink()
    summary = run(scenario_cfg(duration_s=10), sinks=[sink], pacer=pacer)
    assert pacer.calls == list(range(1, 11))
    assert sink.ticks == 10 and summary.ticks == 10


def test_keyboard_interrupt_drains_and_closes():
    sink, truth = ListSink(), ListTruth()
    follower = FakeFollower()
    summary = run(site_cfg(duration_s=None, faults=FaultConfig(late_prob=0.5)), sinks=[sink], truth=truth,
                  follower=follower, pacer=FakePacer(raise_at=50))
    assert summary.interrupted and summary.ticks == 50
    assert sink.closed and truth.closed and follower.closed
    assert summary.faults["pending"] == 0 and summary.drained > 0
    assert len(sink.items) == summary.emissions == summary.records + summary.faults["duplicates"]


def test_stop_event_ends_run():
    stop = threading.Event()
    sink = ListSink(on_send=lambda em: stop.set() if em.value["ts"] >= T_1200 + 20 else None)
    summary = run(site_cfg(duration_s=None), sinks=[sink], stop=stop)
    assert summary.interrupted and summary.ticks == 21 and sink.closed


def test_close_errors_do_not_stop_cleanup():
    class Broken(ListSink):
        def close(self):
            raise OSError("disk full")
    a, b, truth = Broken("broken"), ListSink("ok"), ListTruth()
    summary = run(scenario_cfg(duration_s=3), sinks=[a, b], truth=truth)
    assert b.closed and truth.closed
    assert summary.close_errors and "disk full" in summary.close_errors[0] and not summary.ok
    assert "close error" in summary.format()


def test_kafka_sink_stats():
    class FakeProducer:
        topic = "telemetry.v1"
        sent = delivered = errors = 0
        last_error = None

        def send(self, em):
            self.sent += 1
            self.delivered += 1

        def poll(self, timeout=0):
            pass

        def flush(self, timeout=30):
            return 0
    sink = KafkaSink(FakeProducer())
    summary = run(scenario_cfg(duration_s=2), sinks=[sink])
    assert summary.sinks["kafka:telemetry.v1"] == {"sent": 8, "delivered": 8, "errors": 0, "unflushed": 0,
                                                   "last_error": None}
    assert summary.ok
    summary.sinks["kafka:telemetry.v1"]["errors"] = 1
    assert not summary.ok


@pytest.mark.parametrize("kw, message", [
    ({"warmup_s": 60}, "warmup must be 0"), ({"duration_s": -1}, "duration_s"), ({"service_s": 0}, "service_s"),
    ({"maneuver_s": -1}, "maneuver_s"), ({"coord_precision": 13}, "coord_precision")])
def test_run_config_validation(kw, message):
    with pytest.raises(ValueError, match=message):
        dataclasses.replace(scenario_cfg(), **kw)


def test_progress_callback():
    seen = []
    run(scenario_cfg(duration_s=10), progress=lambda s, now: seen.append((s.ticks, now)), progress_every=4)
    start = load_scenario(SCENARIO).start_ts
    assert seen == [(4, start + 3), (8, start + 7)]


def test_closed_output_stops_run_and_drains_others():
    class Gone(ListSink):
        gone = False

        def send(self, em):
            super().send(em)
            if len(self.items) >= 100:
                self.gone = True
    gone, other = Gone("stdout"), ListSink("kafka")
    summary = run(site_cfg(duration_s=None, faults=FaultConfig(late_prob=0.5)), sinks=[gone, other])
    assert summary.output_closed == ["stdout"] and not summary.interrupted
    assert 3 <= summary.ticks <= 10 and "STOPPED: reader closed stdout" in summary.format()
    assert other.closed and gone.closed and summary.faults["pending"] == 0
    assert len(other.items) == summary.emissions == summary.records + summary.faults["duplicates"]


def test_pipe_safe_writer_marks_gone(tmp_path):
    from generator.runner import PipeSafeWriter

    class Broken:
        path, lines = str(tmp_path / "fifo"), 0

        def write(self, obj):
            self.lines += 1
            if self.lines > 2:
                raise BrokenPipeError

        def flush(self):
            pass

        def close(self):
            raise BrokenPipeError
    w = PipeSafeWriter(Broken())
    for i in range(5):
        w.write({"i": i})
    assert w.gone and w.lines == 3
    w.close()                                  # swallowed
    truth = PipeSafeWriter(Broken())
    summary = run(scenario_cfg(), truth=truth)  # truth reader gone after a few events -> run stops
    assert summary.output_closed == ["truth"] and summary.ticks < 960
