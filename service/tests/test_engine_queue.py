"""Queue composition: the task example table, ETA rules, horizon, staleness, ordering, publication."""

FAR = 30_000.0
EAST = 90.0
NO_REDIRECT = 10**9   # queue mechanics only: units first seen near S1 get a decision, never a redirect


def test_task_example_table_from_telemetry(world):
    w = world()
    T0 = w.T0
    w.stand("T1", T0 - 60)                       # 11:59:00, 15 m from S1
    w.stand("T1", T0)
    w.move("T2", T0, 1200.0)
    out = w.move("T3", T0, 3000.0)
    assert [o.key for o in out] == ["S1"]
    value = out[0].value
    assert list(value) == ["station_uuid", "at", "queue"]
    assert value == {"station_uuid": "S1", "at": "2026-09-15T12:00:00Z", "queue": [
        {"unit_uuid": "T1", "eta": None, "service_start": "2026-09-15T11:59:00Z",
         "free_at": "2026-09-15T12:02:50Z", "wait_seconds": 0},
        {"unit_uuid": "T2", "eta": "2026-09-15T12:02:00Z", "service_start": "2026-09-15T12:02:50Z",
         "free_at": "2026-09-15T12:06:40Z", "wait_seconds": 50},
        {"unit_uuid": "T3", "eta": "2026-09-15T12:05:00Z", "service_start": "2026-09-15T12:06:40Z",
         "free_at": "2026-09-15T12:10:30Z", "wait_seconds": 100}]}
    assert all(list(item) == ["unit_uuid", "eta", "service_start", "free_at", "wait_seconds"]
               for item in value["queue"])


def test_eta_is_clamped_to_now(world):
    w = world()
    T0 = w.T0
    w.move("A", T0, 100.0, EAST)                 # ts + 10 s
    assert w.q("S1") == [("A", T0 + 10, T0 + 10, T0 + 240, 0)]
    w.move("X", T0 + 20, FAR, EAST)              # A's position is 20 s old (still fresh)
    assert w.q("S1") == [("A", T0 + 20, T0 + 20, T0 + 250, 0)]


def test_to_unload_eta_goes_via_unload_point(world):
    w = world()
    T0 = w.T0
    w.stand("T1", T0)
    w.move("T1", T0 + 5, 60.0)                   # served, left the radius toward the unload point (5 km west)
    # (4940 m to unload + 5000 m unload -> S1) / 10 m/s = 994 s
    assert w.q("S1") == [("T1", T0 + 999, T0 + 999, T0 + 1229, 0)]
    w.move("T1", T0 + 6, 70.0)
    assert w.q("S1") == [("T1", T0 + 999, T0 + 999, T0 + 1229, 0)]


def test_waiting_unit_eta_is_radius_entry(world):
    w = world(min_gain_s=NO_REDIRECT)
    T0 = w.T0
    w.stand("C", T0)
    w.move("A", T0 + 5, 45.0)
    for ts in range(T0 + 8, T0 + 21):
        w.stand("A", ts)
    assert w.engine.now == T0 + 20
    assert w.q("S1") == [("C", None, T0, T0 + 230, 0), ("A", T0 + 5, T0 + 230, T0 + 460, 225)]


def test_first_member_after_release_starts_at_last_release(world):
    w = world(min_gain_s=NO_REDIRECT)
    T0 = w.T0
    w.stand("C", T0)
    w.move("A", T0 + 5, 45.0, EAST)
    for ts in range(T0 + 8, T0 + 100):
        w.stand("A", ts, 15.0, EAST)
    w.move("C", T0 + 100, 60.0)                  # C leaves early: release at T0 + 100
    assert w.q("S1")[0] == ("A", T0 + 5, T0 + 100, T0 + 330, 95)
    w.stand("A", T0 + 100, 15.0, EAST)
    assert w.q("S1")[0] == ("A", None, T0 + 100, T0 + 330, 0)


def test_horizon_excludes_eta_beyond_30_min(world):
    w = world()
    T0 = w.T0
    w.move("A", T0, 17_990.0, EAST)              # eta now + 1799
    w.move("B", T0, 18_000.0, EAST + 1)          # eta now + 1800: still inside
    w.move("C", T0, 18_010.0, EAST + 2)          # eta now + 1801: out
    assert [u for u, *_ in w.q("S1")] == ["A", "B"]
    w.move("C", T0 + 1, 18_000.0, EAST + 2)
    assert [u for u, *_ in w.q("S1")] == ["A", "B", "C"]


def test_stale_unit_excluded_until_fresh_message(world):
    w = world(min_gain_s=NO_REDIRECT)
    T0 = w.T0
    w.stand("C", T0)                             # occupant, goes silent
    w.move("A", T0, 1000.0, EAST)
    w.move("X", T0 + 30, FAR, EAST)
    assert [u for u, *_ in w.q("S1")] == ["C", "A"]
    w.move("X", T0 + 31, FAR - 10, EAST)         # A's position is 31 s old
    assert [u for u, *_ in w.q("S1")] == ["C"]
    w.move("A", T0 + 32, 990.0, EAST)
    assert w.q("S1") == [("C", None, T0, T0 + 230, 0), ("A", T0 + 131, T0 + 230, T0 + 460, 99)]


def test_equal_eta_ordered_by_unit_uuid(world):
    w = world()
    T0 = w.T0
    w.move("B", T0, 1000.0, EAST)
    w.move("A", T0, 1000.0, EAST + 10)
    assert w.q("S1") == [("A", T0 + 100, T0 + 100, T0 + 330, 0), ("B", T0 + 100, T0 + 330, T0 + 560, 230)]


def test_publication_only_on_change(world):
    w = world()
    T0 = w.T0
    out = w.move("X", T0, FAR, EAST)             # first evaluation: every station, in station order
    assert [(o.kind, o.key, o.value["queue"]) for o in out] == [("queue", s, []) for s in ("S1", "S2", "S3", "S4")]
    assert w.move("X", T0 + 1, FAR - 10, EAST) == []          # only `at` would differ
    out = w.move("A", T0 + 1, 1600.0, EAST)
    assert [o.key for o in out] == ["S1"] and out[0].value["at"] == "2026-09-15T12:00:01Z"
    assert w.move("A", T0 + 2, 1590.0, EAST) == []            # same eta (T0 + 161), nothing changed
    assert [o.key for o in w.move("A", T0 + 3, 1570.0, EAST)] == ["S1"]  # eta T0 + 161 -> T0 + 160
    assert w.engine.stats["queue_published"] == 6
