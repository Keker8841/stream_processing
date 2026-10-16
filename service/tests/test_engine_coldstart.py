"""Cold start: the first message of a unit is already inside the 1500 m decision radius. Its default state is
"to station" (unless that message is "at the station"), so that message is its first one closer than 1500 m."""

EAST = 90.0


def test_first_message_inside_the_radius_decides_at_once(world):
    w = world()
    T0 = w.T0
    out = w.move("T4", T0, 1000.0)
    assert out[0].kind == "decision" and out[0].headers == (("decision-id", f"T4:{T0}"),)
    w.drive("T4", T0 + 1, 990.0, 500.0)
    assert len(w.decisions()) == 1


def test_default_state_is_to_station_whatever_the_motion(world):
    w = world()
    T0 = w.T0
    w.stand("T3", T0, 800.0)                      # standing, but not within 50 m of a station
    w.drive("T4", T0, 800.0, 1100.0)              # driving away from its station
    assert [(d.key, d.headers) for d in w.decisions()] == [("T3", (("decision-id", f"T3:{T0}"),)),
                                                           ("T4", (("decision-id", f"T4:{T0}"),))]


def test_first_seen_standing_at_a_station_gets_no_decision(world):
    w = world()
    T0 = w.T0
    w.stand("T4", T0, 15.0)                       # "at the station": not heading to it in this trip
    w.stand("T4", T0 + 100, 15.0)
    w.move("T4", T0 + 200, 60.0)                  # served, leaves -> to unload
    w.move("T4", T0 + 300, 1000.0)
    assert w.decisions() == []


def test_first_seen_rolling_inside_the_station_radius_decides(world):
    w = world()
    T0 = w.T0
    out = w.move("T4", T0, 49.0)                  # moving: not "at the station" yet, so "to station"
    assert out[0].kind == "decision"
    w.stand("T4", T0 + 4, 15.0)
    assert len(w.decisions()) == 1


def test_first_observation_outside_uses_the_crossing(world):
    w = world()
    w.drive("T4", w.T0, 1535.0, 1485.0)
    [d] = w.decisions()
    assert d.headers == (("decision-id", f"T4:{w.T0 + 4}"),)    # 1505 m at T0 + 3 -> 1495 m at T0 + 4


def test_new_trip_after_unload_starts_outside(world):
    w = world()
    T0 = w.T0
    w.move("T4", T0, 1000.0)                      # cold start decision of this trip
    w.stand("T4", T0 + 100, 15.0)
    w.move("T4", T0 + 300, 60.0)                  # served, leaves
    w.stand("T4", T0 + 800, 0.0, ref="U1")
    w.move("T4", T0 + 805, 60.0, EAST, ref="U1")  # new trip from the unload point (5 km west)
    w.move("T4", T0 + 1150, 1504.0)
    assert len(w.decisions()) == 1
    w.move("T4", T0 + 1151, 1494.0)
    assert len(w.decisions()) == 2
