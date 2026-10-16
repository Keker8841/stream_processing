import pytest

from generator.faults import FaultConfig, FaultInjector
from generator.model import Emission, Telemetry
from generator.pacing import Pacer
from generator.recording import JsonlWriter, emission_to_line, read_record
from generator.replay import first_emit_at, replay, shift_emission


class FakeClock:
    def __init__(self, start: float = 500.0) -> None:
        self.now = start
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, d: float) -> None:
        self.sleeps.append(d)
        self.now += d


def em(emit_at: int, unit: str, ts: int) -> Emission:
    return Emission(emit_at, unit, Telemetry(unit, ts, 49.0, 142.0, 36.0).to_dict())


# emit_at is non-decreasing (emission order); value ts is not (late record, duplicate)
EMS = [em(100, "T-02", 100), em(100, "T-01", 100), em(101, "T-01", 101), em(103, "T-02", 97),
       em(103, "T-01", 103), em(103, "T-01", 101), em(110, "T-03", 110)]


@pytest.fixture
def record(tmp_path):
    path = tmp_path / "rec.jsonl"
    with JsonlWriter(path) as w:
        for e in EMS:
            w.write(emission_to_line(e))
    return path


def run_replay(path, speed, shift_ts=0):
    fc = FakeClock()
    made: list[float] = []

    def factory(s):
        made.append(s)
        return Pacer(s, clock=fc.clock, sleep=fc.sleep)

    t0 = fc.now
    sent: list[tuple[float, Emission]] = []
    n = replay(path, lambda e: sent.append((fc.now - t0, e)), speed=speed, shift_ts=shift_ts, pacer_factory=factory)
    return n, sent, fc, made


@pytest.mark.parametrize("speed, scale", [(1, 1.0), (1.0, 1.0), (4, 0.25)])
def test_paced_by_emit_at_relative_to_first(record, speed, scale):
    n, sent, fc, made = run_replay(record, speed)
    assert n == len(EMS) and made == [speed]
    assert [t for t, _ in sent] == pytest.approx([(e.emit_at - 100) * scale for e in EMS])
    assert [e for _, e in sent] == EMS                         # file order, never re-sorted by ts


@pytest.mark.parametrize("speed", [0, -1])
def test_unpaced_never_sleeps(record, speed):
    n, sent, fc, _ = run_replay(record, speed)
    assert n == len(EMS) and fc.sleeps == []
    assert [e for _, e in sent] == EMS


def test_shift_applies_to_emit_at_and_ts_only(record):
    n, sent, _, _ = run_replay(record, 1, shift_ts=1_000)
    out = [e for _, e in sent]
    assert [e.emit_at for e in out] == [e.emit_at + 1_000 for e in EMS]
    assert [e.value["ts"] for e in out] == [e.value["ts"] + 1_000 for e in EMS]
    for a, b in zip(out, EMS):
        assert a.key == b.key
        assert {k: v for k, v in a.value.items() if k != "ts"} == {k: v for k, v in b.value.items() if k != "ts"}
    assert [t for t, _ in sent] == pytest.approx([e.emit_at - 100 for e in EMS])   # pacing unaffected
    assert list(read_record(record)) == EMS                    # record file untouched


def test_shift_emission_copies_value():
    e = EMS[3]
    before = dict(e.value)
    s = shift_emission(e, -50)
    assert s.value is not e.value and e.value == before
    assert (s.emit_at, s.value["ts"]) == (53, 47)
    assert shift_emission(e, 0) == e


def test_empty_record(tmp_path):
    path = tmp_path / "empty.jsonl"
    path.write_text("")
    n, sent, fc, made = run_replay(path, 1)
    assert n == 0 and sent == [] and made == []
    assert first_emit_at(path) is None


def test_first_emit_at(record):
    assert first_emit_at(record) == 100


def test_faults_record_replay_round_trip(tmp_path):
    cfg = FaultConfig(dup_prob=0.1, late_prob=0.1, lag_rate_per_hour=360.0, lag_min_s=5, lag_max_s=20,
                      shuffle_within_tick=True)
    inj = FaultInjector(cfg, seed=3)
    path = tmp_path / "run" / "rec.jsonl"
    emitted: list[Emission] = []
    with JsonlWriter(path) as w:
        for now in range(1000, 1200):
            inj.push(now, [Telemetry(f"T-0{i}", now, 49.0, 142.0, 0.0) for i in range(1, 6)])
            for e in inj.pop_due(now):
                w.write(emission_to_line(e))
                emitted.append(e)
        for e in inj.drain():
            w.write(emission_to_line(e))
            emitted.append(e)
    sent: list[Emission] = []
    assert replay(path, sent.append) == len(emitted) == inj.stats["emitted"]
    assert sent == emitted
