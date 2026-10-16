import pytest

from generator.pacing import Pacer


class FakeClock:
    """Monotonic fake clock; sleep() advances it and may oversleep by a fixed jitter."""

    def __init__(self, start: float = 1000.0, oversleep: float = 0.0) -> None:
        self.now = start
        self.oversleep = oversleep
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, d: float) -> None:
        self.sleeps.append(d)
        self.now += d + self.oversleep

    def work(self, d: float) -> None:
        self.now += d


def make(speed: float, **kw) -> tuple[Pacer, FakeClock]:
    fc = FakeClock(**kw)
    return Pacer(speed, clock=fc.clock, sleep=fc.sleep), fc


@pytest.mark.parametrize("speed", [0, 0.0, -1.0])
def test_non_positive_speed_never_sleeps(speed):
    pacer, fc = make(speed)
    for k in range(1, 100):
        fc.work(0.01)
        pacer.wait_until(k)
    assert fc.sleeps == []
    assert pacer.behind_s(1) == 0.0


def test_real_time_deadlines_are_absolute():
    pacer, fc = make(1.0)
    t0 = fc.now
    for k in range(1, 6):
        fc.work(0.2)
        pacer.wait_until(k)
        assert fc.now == pytest.approx(t0 + k)
    assert fc.sleeps == pytest.approx([0.8] * 5)


def test_accelerated_speed():
    pacer, fc = make(10.0)
    t0 = fc.now
    for k in range(1, 21):
        pacer.wait_until(k)
        assert fc.now == pytest.approx(t0 + k / 10)


def test_no_drift_with_work_and_oversleep():
    # Each tick costs 50 ms of work and every sleep overshoots by 3 ms: relative sleeping would drift by
    # ~53 s over 1000 ticks; absolute deadlines keep the error bounded by a single overshoot.
    pacer, fc = make(1.0, oversleep=0.003)
    t0 = fc.now
    n = 1000
    for k in range(1, n + 1):
        fc.work(0.05)
        pacer.wait_until(k)
        assert fc.now - (t0 + k) == pytest.approx(0.003, abs=1e-6)
    assert fc.now == pytest.approx(t0 + n + 0.003, abs=1e-6)


def test_behind_schedule_never_sleeps_negative_and_catches_up():
    pacer, fc = make(1.0)
    t0 = fc.now
    pacer.wait_until(1)
    fc.work(5.5)                  # stall: now at t0 + 6.5
    assert pacer.behind_s(2) == pytest.approx(4.5)
    for k in range(2, 7):         # deadlines t0+2 .. t0+6 already passed: burst without sleeping
        pacer.wait_until(k)
    assert fc.now == pytest.approx(t0 + 6.5)
    pacer.wait_until(7)           # back on schedule
    assert fc.now == pytest.approx(t0 + 7)
    assert all(d > 0 for d in fc.sleeps)


def test_t0_captured_at_construction():
    fc = FakeClock(start=50.0)
    pacer = Pacer(2.0, clock=fc.clock, sleep=fc.sleep)
    fc.work(3.0)                  # construction-to-first-call delay counts against the schedule
    pacer.wait_until(10)          # deadline 50 + 10/2 = 55
    assert fc.now == pytest.approx(55.0)
    assert fc.sleeps == pytest.approx([2.0])
    pacer.wait_until(0)           # past deadline: no sleep
    assert len(fc.sleeps) == 1
