"""Station choice: both metrics, ties, threshold, stale telemetry, once per trip, redirect, shadow mode."""

import pytest

FAR = 30_000.0
EAST = 90.0


def busy_s1(w, t4_before: float = 1506.0, t4_cross: float = 1496.0):
    """T1 occupies S1 since 11:59:00, T2/T3 at 1200/3000 m, T4 crosses 1500 m at 12:00:00 (S1 wait 250 s);
    S2 is empty, 2400 m south of the crossing point."""
    T0 = w.T0
    w.stand("T1", T0 - 60)
    w.move("T4", T0 - 1, t4_before)
    w.move("T2", T0, 1200.0)
    w.move("T3", T0, 3000.0)
    return w.move("T4", T0, t4_cross)


def task_example(w):
    """The task's example: T1 occupies S1 since 11:59:00; T4's last message before 12:00:00 is from 11:59:00 at
    1502 m (its 11:59:01..59 messages are late), so at 12:00:00 it is out of S1's queue; T2/T3 at 1200/3000 m;
    T4's 12:00:00 message at 900 m, S2 2400 m away and empty. Needs `world.example_stations(p_dist=900.0)`."""
    T0 = w.T0
    w.stand("T1", T0 - 60)
    w.move("T4", T0 - 60, 1502.0)
    w.move("T2", T0, 1200.0)
    w.move("T3", T0, 3000.0)
    return w.move("T4", T0, 900.0)


def test_task_example_recommends_s2_by_queue_wait(world):
    w = world(stations=world.example_stations(p_dist=900.0))
    T0 = w.T0
    w.stand("T1", T0 - 60)
    w.move("T4", T0 - 60, 1502.0)
    w.move("T2", T0, 1200.0)
    w.move("T3", T0, 3000.0)
    table = [("T1", None, T0 - 60, T0 + 170, 0), ("T2", T0 + 120, T0 + 170, T0 + 400, 50),
             ("T3", T0 + 300, T0 + 400, T0 + 630, 100)]
    assert w.q("S1") == table                      # T4's position is 60 s old: not in the queue
    out = w.move("T4", T0, 900.0)
    assert [(o.kind, o.key) for o in out] == [("decision", "T4"), ("queue", "S2")]
    d = out[0]
    assert list(d.value) == ["unit_uuid", "at", "result", "from_station", "to_station", "gain_seconds"]
    assert d.value == {"unit_uuid": "T4", "at": "2026-09-15T12:00:00Z", "result": "recommended",
                       "from_station": "S1", "to_station": "S2", "gain_seconds": 80}
    assert d.headers == (("decision-id", "T4:1789473600"),)
    assert w.q("S1") == table                      # the redirect keeps S1 as the task's table
    assert w.q("S2") == [("T4", T0 + 240, T0 + 240, T0 + 470, 0)]
    assert w.engine.stats["recommended"] == 1
    assert w.move("T4", T0 - 30, 1202.0) == []     # a late 11:59:30 message is older than 12:00:00: ignored


@pytest.mark.parametrize("metric, result", [("queue_wait", ("recommended", 80)), ("service_end", ("rejected", None))])
def test_task_example_both_metrics(world, metric, result):
    """Waits 80 vs 0 -> S2 with gain 80; by end of service S1 is better (12:06:40 vs 12:07:50) -> no_gain."""
    w = world(stations=world.example_stations(p_dist=900.0), metric=metric)
    d = task_example(w)[0].value
    assert (d["result"], d.get("gain_seconds")) == result
    if d["result"] == "recommended":
        assert d["to_station"] == "S2"
    else:
        assert d["reason"] == "no_gain"


def test_service_end_metric_compares_end_of_service(world):
    w = world(metric="service_end")
    out = busy_s1(w)
    assert out[0].value["to_station"] == "S2" and out[0].value["gain_seconds"] == 160   # 12:10:30 - 12:07:50


def test_shadow_mode_publishes_decision_but_keeps_queues(world):
    w = world(apply_decisions=False)
    T0 = w.T0
    out = busy_s1(w)
    assert [(o.kind, o.value.get("gain_seconds")) for o in out] == [("decision", 250)]   # queues unchanged
    assert [u for u, *_ in w.q("S1")] == ["T1", "T2", "T4", "T3"]
    assert w.q("S2") == []
    w.move("T4", T0 + 1, 1486.0)
    assert [d.key for d in w.decisions()] == ["T2", "T4"]    # T2: first message inside 1500 m, no_gain


def test_tie_prefers_own_station(world):
    w = world()
    T0 = w.T0
    w.move("T4", T0 - 1, 1506.0)
    out = w.move("T4", T0, 1496.0)                # every station empty: waits all 0
    assert list(out[0].value) == ["unit_uuid", "at", "result", "reason"]
    assert out[0].value == {"unit_uuid": "T4", "at": "2026-09-15T12:00:00Z", "result": "rejected",
                            "reason": "no_gain"}


def test_tie_between_others_prefers_nearest(world):
    w = world(stations=world.example_stations(s4=(2000.0, 0.0)))   # S4 2000 m north of the crossing point
    out = busy_s1(w)
    assert out[0].value["to_station"] == "S4" and out[0].value["gain_seconds"] == 250


@pytest.mark.parametrize("occupied_at, result", [(-20, ("recommended", 60)), (-21, ("rejected", None))])
def test_min_gain_threshold(world, occupied_at, result):
    w = world()
    T0 = w.T0
    w.stand("T1", T0 + occupied_at)               # free at T0 + 210 (wait 60) / T0 + 209 (wait 59)
    w.move("T4", T0 - 1, 1506.0)
    d = w.move("T4", T0, 1496.0)[0].value          # eta S1 = T0 + 150
    assert (d["result"], d.get("gain_seconds")) == result


@pytest.mark.parametrize("now_offset, reason", [(40, "stale_telemetry"), (35, "no_gain")])
def test_stale_triggering_message(world, now_offset, reason):
    w = world()
    T0 = w.T0
    w.move("T4", T0 - 1, 1506.0)
    w.move("X", T0 + now_offset, FAR, EAST)
    out = w.move("T4", T0 + 5, 1496.0)             # late but newer than T4's last message: accepted
    assert out[0].value["reason"] == reason
    assert out[0].value["at"] == f"2026-09-15T12:00:{now_offset}Z"
    assert out[0].headers == (("decision-id", f"T4:{T0 + 5}"),)
    w.move("T4", T0 + 6, 1486.0)
    assert len(w.decisions()) == 1               # the trip's decision is consumed either way


def test_once_per_trip_next_after_station_visit(world):
    w = world()
    T0 = w.T0
    w.move("T4", T0 - 1, 1506.0)
    w.move("T4", T0, 1496.0)
    w.move("T4", T0 + 1, 1510.0)
    w.move("T4", T0 + 2, 1490.0)                  # crosses again in the same trip: no decision
    assert len(w.decisions()) == 1
    w.stand("T4", T0 + 150)                       # at S1: occupies
    w.move("T4", T0 + 200, 60.0)                  # served, leaves -> to_unload
    w.move("T4", T0 + 300, 1490.0)                # passes 1500 m on the way out: not to_station
    w.stand("T4", T0 + 700, 0.0, ref="U1")        # unload
    w.move("T4", T0 + 705, 60.0, EAST, ref="U1")  # left the unload radius: new trip to S1
    assert len(w.decisions()) == 1
    w.move("T4", T0 + 1050, 1506.0)
    out = w.move("T4", T0 + 1051, 1496.0)
    assert out[0].kind == "decision" and len(w.decisions()) == 2


def test_redirected_unit_served_at_target_returns_to_own_queue(world):
    w = world()
    T0 = w.T0
    busy_s1(w)
    assert [u for u, *_ in w.q("S2")] == ["T4"]
    w.stand("T4", T0 + 240, 15.0, 0.0, ref="S2")  # closed loop: T4 arrives at S2 and occupies it
    assert w.q("S2") == [("T4", None, T0 + 240, T0 + 470, 0)]
    assert "T4" not in [u for u, *_ in w.q("S1")]
    w.move("T4", T0 + 450, 60.0, 0.0, ref="S2")   # served, leaves S2 -> to_unload, own station S1 again
    assert w.q("S2") == []
    assert "T4" in [u for u, *_ in w.q("S1")]
