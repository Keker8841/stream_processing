"""Occupancy and release: first standing message, exit / 230 s timer release, served flag, FIFO head."""

FAR = 30_000.0      # unit X far east of S1: beyond the horizon, only advances "now"
EAST = 90.0
NO_REDIRECT = 10**9   # queue mechanics only: units first seen near S1 get a decision, never a redirect


def test_first_standing_message_occupies_free_station(world):
    w = world()
    T0 = w.T0
    w.stand("T1", T0)
    assert w.q("S1") == [("T1", None, T0, T0 + 230, 0)]
    assert w.engine.stats["occupancies"] == 1


def test_moving_inside_radius_waits_until_standing(world):
    w = world()
    T0 = w.T0
    w.move("T1", T0, 40.0)                       # inside 50 m, 36 km/h: arrived, not "at station"
    assert w.q("S1") == [("T1", T0, T0, T0 + 230, 0)]
    w.move("T1", T0 + 1, 30.0)
    w.stand("T1", T0 + 2, 15.0)
    assert w.q("S1") == [("T1", None, T0 + 2, T0 + 232, 0)]


def test_exit_release_at_first_message_outside(world):
    w = world(min_gain_s=NO_REDIRECT)
    T0 = w.T0
    w.stand("T1", T0)
    w.move("A", T0 + 5, 45.0, EAST)              # A enters from the east and waits
    for ts in range(T0 + 8, T0 + 101, 10):
        w.stand("A", ts, 15.0, EAST)
    w.move("T1", T0 + 100, 45.0)                 # leaving, still inside
    assert w.q("S1")[0][:2] == ("T1", None)
    w.move("T1", T0 + 101, 57.0)                 # first message outside -> release at its ts
    assert w.engine.stats["releases"] == 1
    # T1 is now `to_unload`: own queue via the unload point (4943 + 5000) / 10 -> 994 s
    assert w.q("S1") == [("A", T0 + 5, T0 + 101, T0 + 331, 96),
                         ("T1", T0 + 1095, T0 + 1095, T0 + 1325, 0)]
    w.stand("A", T0 + 101, 15.0, EAST)
    assert w.q("S1")[0] == ("A", None, T0 + 101, T0 + 331, 0)


def test_timer_release_driven_by_other_units(world):
    w = world()
    T0 = w.T0
    w.stand("T1", T0)                            # T1 then goes silent (connection loss)
    w.move("X", T0 + 229, FAR, EAST)
    assert w.q("S1") == [("T1", None, T0, T0 + 230, 0)]   # occupant: never excluded as stale
    out = w.move("X", T0 + 230, FAR - 10, EAST)
    assert w.q("S1") == []
    assert w.engine.stats["releases"] == 1
    assert [o.value["queue"] for o in out if o.key == "S1"] == [[]]


def test_served_flag_blocks_reoccupation_by_late_backlog(world):
    w = world()
    T0 = w.T0
    w.stand("T1", T0)
    w.move("X", T0 + 240, FAR, EAST)             # timer release at T0 + 230
    assert w.q("S1") == []
    for ts in range(T0 + 1, T0 + 245):           # T1's backlog: standing messages, newer than its last ts
        w.stand("T1", ts)
    assert w.q("S1") == []                       # served: not an occupant again, not a queue member
    assert w.engine.stats["occupancies"] == 1
    w.move("T1", T0 + 245, 57.0)                 # leaves the radius -> to_unload, back in the own queue
    assert w.q("S1") == [("T1", T0 + 1239, T0 + 1239, T0 + 1469, 0)]


def test_fifo_head_occupies_even_if_other_message_comes_first(world):
    w = world(min_gain_s=NO_REDIRECT)
    T0 = w.T0
    w.stand("C", T0)
    w.move("A", T0 + 5, 45.0)
    w.move("B", T0 + 6, 45.0)
    for ts in range(T0 + 9, T0 + 230, 10):
        w.stand("A", ts - 1)
        w.stand("B", ts)                         # same stop point as A
    assert w.q("S1") == [("C", None, T0, T0 + 230, 0),
                         ("A", T0 + 5, T0 + 230, T0 + 460, 225),
                         ("B", T0 + 6, T0 + 460, T0 + 690, 454)]
    w.stand("B", T0 + 230)                       # releases C (timer), but B is not the head
    assert w.engine.stats["occupancies"] == 1
    assert w.q("S1")[:1] == [("A", T0 + 5, T0 + 230, T0 + 460, 225)]
    w.stand("A", T0 + 230)
    assert w.q("S1") == [("A", None, T0 + 230, T0 + 460, 0), ("B", T0 + 6, T0 + 460, T0 + 690, 454)]


def test_cold_start_ties_by_unit_uuid(world):
    w = world()
    T0 = w.T0
    w.stand("C", T0)                             # first seen: occupies the free station
    assert w.q("S1")[0] == ("C", None, T0, T0 + 230, 0)
    w.stand("B", T0)                             # first seen at the same ts: entry = first seen ts, tie
    w.stand("A", T0)                             # the head by (entry, unit_uuid) stood there at that same second
    assert w.q("S1") == [("A", None, T0, T0 + 230, 0),
                         ("B", T0, T0 + 230, T0 + 460, 230),
                         ("C", T0, T0 + 460, T0 + 690, 460)]
    w.stand("C", T0 + 230)
    w.stand("B", T0 + 230)
    assert w.q("S1")[0] == ("B", None, T0 + 230, T0 + 460, 0)


def test_head_does_not_occupy_on_backlog_older_than_release(world):
    w = world(min_gain_s=NO_REDIRECT)
    T0 = w.T0
    w.stand("C", T0)
    w.move("A", T0 + 5, 45.0)
    for ts in range(T0 + 8, T0 + 201):
        w.stand("A", ts)
    w.move("X", T0 + 240, FAR, EAST)             # C released by the timer at T0 + 230 while A lags
    for ts in range(T0 + 201, T0 + 230):
        w.stand("A", ts)
    assert w.q("S1") == [("A", T0 + 5, T0 + 230, T0 + 460, 225)]
    w.stand("A", T0 + 230)
    assert w.q("S1") == [("A", None, T0 + 230, T0 + 460, 0)]


def test_occupancy_at_any_station_ends_redirect(world):
    w = world()
    T0 = w.T0
    w.stand("T1", T0 - 60)
    w.move("T4", T0 - 1, 1506.0)
    w.move("T2", T0, 1200.0)
    w.move("T3", T0, 3000.0)
    w.move("T4", T0, 1496.0)
    assert w.decisions()[-1].value["to_station"] == "S2"
    assert [u for u, *_ in w.q("S2")] == ["T4"]
    # open loop: T4 ignores the recommendation and drives to its own S1
    for k, d in enumerate(range(1486, 44, -10)):
        w.move("T4", T0 + 1 + k, float(d))
    ts = T0 + 1 + k
    w.stand("T4", ts + 1)                        # T1 still occupies S1 (until T0 + 170)
    assert [u for u, *_ in w.q("S2")] == ["T4"]
    w.stand("T4", T0 + 170)                      # timer release of T1, T4 is the head -> occupies S1
    assert w.q("S1")[0] == ("T4", None, T0 + 170, T0 + 400, 0)
    assert w.q("S2") == []
    w.move("T4", T0 + 371, 57.0)                 # served at S1, leaves -> to_unload, own queue again
    assert "T4" in [u for u, *_ in w.q("S1")]
    assert w.q("S2") == []
