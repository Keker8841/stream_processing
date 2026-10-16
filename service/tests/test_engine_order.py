"""Ordering and duplicates: per-unit dedup by ts, late newer messages, now = max accepted ts."""

EAST = 90.0


def test_duplicate_and_older_messages_are_ignored(world):
    w = world()
    T0 = w.T0
    w.move("A", T0 + 10, 1600.0, EAST)
    before = w.engine.snapshot()
    assert w.move("A", T0 + 10, 1600.0, EAST) == []          # duplicate
    assert w.move("A", T0 + 5, 1200.0, EAST) == []           # older than A's last accepted ts
    assert w.engine.now == T0 + 10
    after = w.engine.snapshot()
    assert after["stats"]["ignored_old"] == 2
    after["stats"]["ignored_old"] = 0
    assert after == before                                    # nothing else changed


def test_late_newer_message_is_accepted_without_moving_now(world):
    w = world()
    T0 = w.T0
    w.move("A", T0, 1600.0, EAST)
    w.move("B", T0 + 40, 1700.0, EAST)
    w.move("A", T0 + 20, 1400.0, EAST)                      # 20 s late, but newer than A's last
    assert w.engine.now == T0 + 40
    assert w.engine.stats["accepted"] == 3
    assert w.q("S1")[0] == ("A", T0 + 160, T0 + 160, T0 + 390, 0)   # from the T0 + 20 position
    w.move("A", T0 + 41, 1390.0, EAST)
    assert w.engine.now == T0 + 41                            # now = max accepted ts


def test_unknown_and_invalid_messages_are_counted_and_ignored(world):
    w = world()
    T0 = w.T0
    w.move("A", T0, 1600.0, EAST)
    lat, lon = w.pos(1000.0)
    bad = [
        {"unit_uuid": "T-99", "ts": T0 + 100, "lat": lat, "lon": lon, "speed_kmh": 0.0},
        {"unit_uuid": "A", "ts": T0 + 100, "lat": lat, "speed_kmh": 0.0},
        {"unit_uuid": "A", "ts": "soon", "lat": lat, "lon": lon, "speed_kmh": 0.0},
        {"unit_uuid": "A", "ts": T0 + 100, "lat": lat, "lon": float("nan"), "speed_kmh": 0.0},
        {"unit_uuid": "A", "ts": True, "lat": lat, "lon": lon, "speed_kmh": 0.0},
        ["not", "a", "mapping"],
    ]
    assert all(w.engine.process(m) == [] for m in bad)
    assert w.engine.now == T0
    s = w.engine.stats
    assert (s["accepted"], s["ignored_unknown"], s["ignored_invalid"]) == (1, 1, 5)
