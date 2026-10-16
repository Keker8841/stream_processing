import math
from collections import Counter, defaultdict

import pytest

from generator.constants import MAX_LATENESS_S
from generator.faults import FaultConfig, FaultInjector
from generator.model import Emission, ScriptedDelay, Telemetry

UNITS = [f"T-{i:02d}" for i in range(1, 41)]
START = 1_789_473_600


def tick_records(now: int, units=UNITS) -> list[Telemetry]:
    return [Telemetry(u, now, round(49.1 + int(u[2:]) * 1e-4, 4), round(142.6 + (now % 97) * 1e-4, 4),
                      36.0 if (now + int(u[2:])) % 3 else 0.0) for u in units]


def run(cfg: FaultConfig, seed=1, ticks=200, units=UNITS, records_fn=tick_records):
    """Push/pop tick by tick like the runner; returns (injector, [(pop_tick, emission)], drained)."""
    inj = FaultInjector(cfg, seed)
    out: list[tuple[int, Emission]] = []
    for now in range(START, START + ticks):
        inj.push(now, records_fn(now, units))
        out.extend((now, em) for em in inj.pop_due(now))
    return inj, out, inj.drain()


def flat(out, tail=()):
    return [em for _, em in out] + list(tail)


def lateness(em: Emission) -> int:
    return em.emit_at - em.value["ts"]


# --- config ----------------------------------------------------------------------------------------------------

def test_default_config_is_valid_and_none_is_inactive():
    assert FaultConfig().active
    none = FaultConfig.none()
    assert not none.active
    assert none.dup_prob == 0 and none.late_prob == 0 and none.lag_rate_per_hour == 0
    assert not none.shuffle_within_tick
    assert none.problems() == []


@pytest.mark.parametrize("kw", [
    {"dup_prob": -0.1}, {"dup_prob": 1.5}, {"late_prob": math.nan}, {"late_prob": True},
    {"dup_max_delay_s": -1}, {"dup_max_delay_s": MAX_LATENESS_S + 1},
    {"late_max_s": 0}, {"late_max_s": MAX_LATENESS_S + 1}, {"late_max_s": 1.5},
    {"lag_min_s": 0}, {"lag_max_s": MAX_LATENESS_S + 1}, {"lag_min_s": 30, "lag_max_s": 20},
    {"lag_rate_per_hour": -1.0}, {"lag_rate_per_hour": 3601.0}, {"lag_rate_per_hour": math.inf},
    {"shuffle_within_tick": "yes"},
])
def test_invalid_config_rejected(kw):
    with pytest.raises(ValueError):
        FaultConfig(**kw)


def test_boundary_config_accepted():
    FaultConfig(dup_prob=1, dup_max_delay_s=0, late_prob=1.0, late_max_s=1, lag_rate_per_hour=3600,
                lag_min_s=MAX_LATENESS_S, lag_max_s=MAX_LATENESS_S)


# --- passthrough / input handling ---------------------------------------------------------------------------------

def test_none_is_passthrough_in_input_order():
    def records(now, units):
        # Some units drop out on some ticks; input order is deliberately not sorted.
        return [r for r in reversed(tick_records(now, units)) if (now + int(r.unit_uuid[2:])) % 7]

    inj, out, tail = run(FaultConfig.none(), ticks=50, records_fn=records)
    expected = [Emission(r.ts, r.unit_uuid, r.to_dict()) for now in range(START, START + 50)
                for r in records(now, UNITS)]
    assert flat(out) == expected
    assert all(now == em.emit_at for now, em in out)
    assert tail == []
    st = inj.stats
    assert st["records_in"] == st["emitted"] == len(expected)
    assert st["duplicates"] == st["delayed"] == st["lag_episodes"] == st["held_in_lag"] == st["max_lateness"] == 0


def test_push_validation():
    inj = FaultInjector(FaultConfig.none(), 1)
    with pytest.raises(ValueError):
        inj.push(START, [Telemetry("T-01", START - 1, 0.0, 0.0, 0.0)])
    inj.push(START + 5, [])
    with pytest.raises(ValueError):
        inj.push(START + 4, [])


def test_unit_faults_do_not_depend_on_other_units():
    cfg = FaultConfig(dup_prob=0.1, late_prob=0.1, lag_rate_per_hour=120.0, lag_min_s=5, lag_max_s=20)
    _, out_all, tail_all = run(cfg, seed=5, ticks=300)
    _, out_one, tail_one = run(cfg, seed=5, ticks=300, units=["T-07"])
    only = [(em.emit_at, em.value) for em in flat(out_all, tail_all) if em.key == "T-07"]
    assert only == [(em.emit_at, em.value) for em in flat(out_one, tail_one)]


# --- invariants -----------------------------------------------------------------------------------------------

EXTREME = [
    FaultConfig(dup_prob=1, dup_max_delay_s=60, late_prob=1, late_max_s=60, lag_rate_per_hour=3600,
                lag_min_s=60, lag_max_s=60),
    FaultConfig(dup_prob=1, dup_max_delay_s=60, late_prob=1, late_max_s=60, lag_rate_per_hour=0),
    FaultConfig(dup_prob=0.5, dup_max_delay_s=60, late_prob=0.5, late_max_s=60, lag_rate_per_hour=600,
                lag_min_s=1, lag_max_s=60, shuffle_within_tick=True),
]


@pytest.mark.parametrize("cfg", EXTREME)
def test_lateness_cap_with_extreme_configs(cfg):
    inj, out, tail = run(cfg, seed=3, ticks=300, units=UNITS[:10])
    last_tick = START + 299
    assert all(now == em.emit_at for now, em in out)          # popped exactly on its tick, never early or late
    assert all(em.emit_at > last_tick for em in tail)
    lat = [lateness(em) for em in flat(out, tail)]
    assert min(lat) >= 0
    assert max(lat) == MAX_LATENESS_S                          # the cap is reached but never exceeded
    assert inj.stats["max_lateness"] == MAX_LATENESS_S


def test_determinism_and_seed_sensitivity():
    cfg = FaultConfig(dup_prob=0.05, late_prob=0.05, lag_rate_per_hour=60.0, shuffle_within_tick=True)
    a = run(cfg, seed=7, ticks=150)
    b = run(cfg, seed=7, ticks=150)
    c = run(cfg, seed=8, ticks=150)
    assert a[1] == b[1] and a[2] == b[2]
    assert a[0].stats == b[0].stats
    assert flat(a[1], a[2]) != flat(c[1], c[2])


def test_stats_consistency_and_drain_order():
    cfg = FaultConfig(dup_prob=0.05, late_prob=0.05, lag_rate_per_hour=60.0, lag_min_s=10, lag_max_s=40)
    inj, out, tail = run(cfg, seed=11, ticks=400)
    st = inj.stats
    ems = flat(out, tail)
    assert st["records_in"] == 400 * len(UNITS)
    assert st["pending"] == inj.pending == 0
    assert st["emitted"] == len(ems) == st["records_in"] + st["duplicates"]
    copies = Counter((em.key, em.value["ts"]) for em in ems)
    assert len(copies) == st["records_in"]
    assert set(copies.values()) <= {1, 2}
    assert sum(1 for c in copies.values() if c == 2) == st["duplicates"]
    assert st["delayed"] > 0 and st["lag_episodes"] > 0 and st["held_in_lag"] >= st["lag_episodes"]
    assert st["max_lateness"] == max(lateness(em) for em in ems)
    keys = [em.emit_at for em in ems]
    assert keys == sorted(keys)


# --- individual faults ----------------------------------------------------------------------------------------

def test_duplicates_rate_and_shape():
    cfg = FaultConfig(dup_prob=0.1, dup_max_delay_s=10, late_prob=0.0, lag_rate_per_hour=0.0)
    inj, out, tail = run(cfg, seed=2, ticks=500)
    n = inj.stats["records_in"]
    dups = inj.stats["duplicates"]
    assert n == 20_000
    assert 1_700 <= dups <= 2_300                              # expected 2000, sd ~42
    seen: dict[tuple[str, int], Emission] = {}
    dup_delays = []
    for em in flat(out, tail):
        k = (em.key, em.value["ts"])
        if k in seen:                                          # the copy always comes after the original
            assert em.value == seen[k].value
            assert seen[k].emit_at == em.value["ts"]           # original on time
            dup_delays.append(em.emit_at - seen[k].emit_at)
        else:
            seen[k] = em
    assert len(dup_delays) == dups
    assert min(dup_delays) == 0 and max(dup_delays) == 10


def test_delayed_records_keep_emit_at_ts_plus_d():
    cfg = FaultConfig(dup_prob=0.0, late_prob=0.2, late_max_s=30, lag_rate_per_hour=0.0)
    inj, out, tail = run(cfg, seed=4, ticks=300, units=UNITS[:20])
    assert all(now == em.emit_at for now, em in out)
    delays = [lateness(em) for em in flat(out, tail)]
    late = [d for d in delays if d > 0]
    assert len(late) == inj.stats["delayed"]
    assert 1_000 <= len(late) <= 1_400                         # 6000 records * 0.2
    assert min(late) == 1 and max(late) == 30


def test_late_prob_one_delays_everything_by_exactly_late_max():
    cfg = FaultConfig(dup_prob=0.0, late_prob=1.0, late_max_s=1, lag_rate_per_hour=0.0)
    inj, out, tail = run(cfg, ticks=20, units=UNITS[:3])
    assert all(lateness(em) == 1 for em in flat(out, tail))
    assert [em.value["ts"] for _, em in out[:3]] == [START] * 3
    assert len(tail) == 3 and all(em.value["ts"] == START + 19 for em in tail)


def test_lag_episode_holds_and_releases_at_end_tick():
    # Rate 3600/h -> an episode starts on every tick that is not inside one; fixed duration 5 s.
    cfg = FaultConfig(dup_prob=0.0, late_prob=0.0, lag_rate_per_hour=3600.0, lag_min_s=5, lag_max_s=5)
    inj = FaultInjector(cfg, seed=1)
    released: dict[int, list[int]] = {}
    for now in range(START, START + 20):
        inj.push(now, tick_records(now, ["T-01"]))
        due = inj.pop_due(now)
        if due:
            released[now] = [em.value["ts"] for em in due]
            assert all(em.emit_at == now for em in due)
    assert released == {START + 5: list(range(START, START + 5)),
                        START + 10: list(range(START + 5, START + 10)),
                        START + 15: list(range(START + 10, START + 15))}
    assert [em.value["ts"] for em in inj.drain()] == list(range(START + 15, START + 20))
    st = inj.stats
    assert st["lag_episodes"] == 4 and st["held_in_lag"] == 20 and st["max_lateness"] == 5


def test_random_lag_episodes_are_contiguous_and_released_in_ts_order():
    cfg = FaultConfig(dup_prob=0.0, late_prob=0.0, lag_rate_per_hour=360.0, lag_min_s=5, lag_max_s=15)
    units = UNITS[:5]
    ticks = 400
    inj, out, tail = run(cfg, seed=9, ticks=ticks, units=units)
    last_tick = START + ticks - 1
    assert all(now == em.emit_at for now, em in out)
    per_unit: dict[str, list[int]] = defaultdict(list)
    groups: dict[tuple[str, int], list[int]] = defaultdict(list)
    for em in flat(out, tail):
        per_unit[em.key].append(em.value["ts"])
        if em.emit_at > em.value["ts"]:
            groups[(em.key, em.emit_at)].append(em.value["ts"])
    for unit in units:                                         # lag never reorders a unit's own records
        assert per_unit[unit] == list(range(START, START + ticks))
    for (unit, release), held in groups.items():
        start = held[0]
        assert 5 <= release - start <= 15
        if release <= last_tick:
            assert held == list(range(start, release))         # whole episode [start, release)
        else:
            assert held == list(range(start, last_tick + 1))   # cut by the end of the run
    st = inj.stats
    assert len(groups) == st["lag_episodes"]
    assert sum(map(len, groups.values())) == st["held_in_lag"]
    assert 40 <= st["lag_episodes"] <= 160                     # ~20 per unit (10 s wait + 10 s episode)


def test_backlog_precedes_fresh_record_on_release_tick():
    cfg = FaultConfig(dup_prob=0.0, late_prob=0.0, lag_rate_per_hour=360.0, lag_min_s=5, lag_max_s=15)
    inj, out, _ = run(cfg, seed=9, ticks=400, units=UNITS[:5])
    by_tick: dict[tuple[int, str], list[int]] = defaultdict(list)
    for now, em in out:
        by_tick[(now, em.key)].append(em.value["ts"])
    multi = [v for v in by_tick.values() if len(v) > 1]
    assert multi
    assert all(v == sorted(v) for v in multi)


def test_scripted_delay_delivers_after_newer_records():
    d = ScriptedDelay("T-02", START + 1, START + 3, START + 5)
    inj = FaultInjector(FaultConfig.none(), 1, [d])
    out = []
    for now in range(START, START + 7):
        inj.push(now, tick_records(now, UNITS[:3]))
        out += [(now, em.key, em.value["ts"]) for em in inj.pop_due(now)]
    t02 = [(now, ts) for now, key, ts in out if key == "T-02"]
    assert t02 == [(START, START), (START + 4, START + 4),
                   (START + 5, START + 1), (START + 5, START + 2), (START + 5, START + 3), (START + 5, START + 5),
                   (START + 6, START + 6)]
    # on the release tick the held records come first, before every record of that tick
    assert [k for now, k, ts in out if now == START + 5][:3] == ["T-02"] * 3
    assert inj.stats["scripted"] == 3 and inj.stats["max_lateness"] == 4


def test_scripted_delay_keeps_random_draws_and_caps():
    cfg = FaultConfig(dup_prob=0.3, late_prob=0.3, lag_rate_per_hour=0.0)
    d = ScriptedDelay("T-01", START + 10, START + 20, START + 70)
    _, plain, _ = run(cfg, seed=4, ticks=100, units=UNITS[:2])
    inj = FaultInjector(cfg, 4, [d])
    scripted = []
    for now in range(START, START + 100):
        inj.push(now, tick_records(now, UNITS[:2]))
        scripted += inj.pop_due(now)
    other = lambda ems: [(em.emit_at, em.value["ts"]) for em in ems if em.key == "T-02"]   # noqa: E731
    assert other(scripted) == other(flat(plain))         # the other unit's faults are untouched
    assert all(lateness(em) <= MAX_LATENESS_S for em in scripted)
    for bad in (ScriptedDelay("T-01", 10, 9, 20), ScriptedDelay("T-01", 10, 20, 20), ScriptedDelay("T-01", 10, 20, 71)):
        with pytest.raises(ValueError, match="scripted delay"):
            FaultInjector(cfg, 1, [bad])


def test_shuffle_within_tick_changes_order_not_content_and_is_seeded():
    cfg = FaultConfig(dup_prob=0.0, late_prob=0.0, lag_rate_per_hour=0.0, shuffle_within_tick=True)

    def order(seed):
        inj = FaultInjector(cfg, seed)
        ticks = []
        for now in range(START, START + 20):
            recs = tick_records(now)
            inj.push(now, recs)
            due = inj.pop_due(now)
            assert sorted(due, key=lambda e: e.key) == [Emission(r.ts, r.unit_uuid, r.to_dict()) for r in recs]
            ticks.append([em.key for em in due])
        return ticks

    a, b, c = order(1), order(1), order(2)
    assert a == b
    assert a != c
    assert sum(t != UNITS for t in a) >= 19


def test_changing_one_probability_keeps_other_faults():
    base = dict(dup_max_delay_s=10, late_max_s=30, lag_min_s=5, lag_max_s=20)

    def dup_keys(**kw):
        inj, out, tail = run(FaultConfig(**base, **kw), seed=6, ticks=300, units=UNITS[:10])
        c = Counter((em.key, em.value["ts"]) for em in flat(out, tail))
        return {k for k, n in c.items() if n == 2}

    def delays(late_prob):
        cfg = FaultConfig(**base, dup_prob=0.0, late_prob=late_prob, lag_rate_per_hour=0.0)
        _, out, tail = run(cfg, seed=6, ticks=300, units=UNITS[:10])
        return {(em.key, em.value["ts"]): lateness(em) for em in flat(out, tail) if lateness(em) > 0}

    a = dup_keys(dup_prob=0.05, late_prob=0.02, lag_rate_per_hour=2.0)
    b = dup_keys(dup_prob=0.05, late_prob=0.3, lag_rate_per_hour=120.0)
    assert a and a == b
    low, high = delays(0.02), delays(0.3)
    assert low and len(high) > len(low)
    assert all(high[k] == d for k, d in low.items())          # raising the probability keeps earlier faults
