"""Review: occupancy when the engine could not see who got the station first.

Cold start: the unit in service and the waiting ones all stand at the stop point, the engine's guess may be wrong; a
unit that stood at the station and leaves it was served there, whoever the engine had let occupy it was waiting.
Lag: a unit's held telemetry shows it standing at the station, first in line, before the current occupancy."""

FAR = 30_000.0
EAST = 90.0


def _cold_start(w, units=("A", "B", "C"), until: int = 99):
    """All units stand at S1 when the stream starts; the engine picks A (first message) as the occupant."""
    T0 = w.T0
    for ts in range(T0, T0 + until + 1):
        for u in units:
            w.stand(u, ts)


def test_leaving_unit_was_served_and_frees_the_station(world):
    w = world()
    T0 = w.T0
    _cold_start(w)
    assert w.q("S1")[0] == ("A", None, T0, T0 + 230, 0)
    w.move("B", T0 + 100, 57.0)                   # B was the real occupant: served, leaves toward the unload point
    # B is `to_unload` (via the unload point, (4943 + 5000) / 10 = 994 s), not a unit driving to S1 from 57 m
    assert ("B", T0 + 1094) in [(u, eta) for u, eta, *_ in w.q("S1")]
    # A did not get the station while B was there: A and C wait, the station is free since B's exit
    assert [u for u, eta, *_ in w.q("S1") if eta is None] == []
    w.stand("A", T0 + 100)                        # same second, after the exit: the head occupies now
    assert w.q("S1")[:2] == [("A", None, T0 + 100, T0 + 330, 0), ("C", T0, T0 + 330, T0 + 560, 330)]
    assert w.engine.stats["occupancies"] == 2


def test_timer_released_unit_still_waiting_is_not_lost(world):
    """The real occupant leaves in the same second the wrongly chosen occupant's 230 s timer fires."""
    w = world()
    T0 = w.T0
    _cold_start(w, until=229)
    w.stand("A", T0 + 230)                        # timer: A released (it has in fact been waiting all along)
    w.move("B", T0 + 230, 57.0)                   # B, the real occupant, leaves
    w.stand("C", T0 + 230)
    w.stand("A", T0 + 231)
    q = w.q("S1")
    assert [u for u, *_ in q[:2]] == ["A", "C"]   # nobody vanished: A occupies, C still waits behind it
    assert q[0] == ("A", None, T0 + 231, T0 + 461, 0)


def test_lagged_head_takes_the_station_it_stood_at_first(world):
    """A arrives at the free S1 while its telemetry is held; B arrives later and takes the seemingly free station.
    A's backlog shows it standing there, first in line, before B's occupancy: A occupied the station (event time)."""
    w = world()
    T0 = w.T0
    w.move("A", T0 - 1, 60.0)
    w.drive("B", T0, 200.0, 60.0)                 # B: 200 m .. 60 m at T0 .. T0 + 14
    w.move("B", T0 + 15, 45.0)
    w.stand("B", T0 + 16)                         # the station looks free: B occupies
    assert w.q("S1")[0] == ("B", None, T0 + 16, T0 + 246, 0)
    w.move("A", T0, 45.0)                         # A's held backlog: entered at T0, stands from T0 + 1
    w.stand("A", T0 + 1)
    assert w.q("S1")[:2] == [("A", None, T0 + 1, T0 + 231, 0), ("B", T0 + 15, T0 + 231, T0 + 461, 216)]
    for ts in range(T0 + 2, T0 + 40):
        w.stand("A", ts)
    for ts in range(T0 + 17, T0 + 200):
        w.stand("B", ts)
        w.stand("A", ts + 22)
    w.move("A", T0 + 222, 57.0)                   # A leaves after its service: an ordinary exit release
    w.stand("B", T0 + 222)
    assert w.q("S1")[0] == ("B", None, T0 + 222, T0 + 452, 0)
    assert w.engine.stats["releases"] == 1


def test_unit_passing_through_does_not_void_the_occupancy(world):
    w = world()
    T0 = w.T0
    w.stand("A", T0)
    for k in range(13):                           # T1 drives through the center at 36 km/h
        w.send("T1", T0 + k, w.pos(60.0 - 10.0 * k, 0.0, ref="S1") if k < 6 else w.pos(10.0 * (k - 6), 180.0),
               36.0)
        w.stand("A", T0 + k)
    assert w.q("S1")[0] == ("A", None, T0, T0 + 230, 0)
    assert w.engine.stats["occupancies"] == 1
