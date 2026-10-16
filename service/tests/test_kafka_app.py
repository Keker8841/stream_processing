"""ServiceApp against the in-memory fake broker (no real Kafka): transactions, cadence, restore, abort."""

import json
import threading
from pathlib import Path

import pytest
from confluent_kafka import OFFSET_INVALID, KafkaError, KafkaException

from fakes import FakeBroker, StubFactory, kerr, stop_after_idle
from vqservice.config import EngineConfig, KafkaConfig, ServiceConfig
from vqservice.kafka_app import STATE_TAIL, ServiceApp, StartupError, decode_telemetry

TEL, QUEUE, DEC, STATE = "telemetry.v1", "queue.v1", "decision.v1", "vq.state.v1"


def make_cfg(**kafka) -> ServiceConfig:
    return ServiceConfig(site_file=Path("unused.yaml"), engine=EngineConfig(), kafka=KafkaConfig(**kafka))


def make_app(broker: FakeBroker, factory: StubFactory | None = None, logs: list | None = None,
             stats_interval_s=None, **kafka) -> ServiceApp:
    return ServiceApp(make_cfg(**kafka), engine_factory=factory or StubFactory(),
                      log=(logs.append if logs is not None else (lambda m: None)),
                      stats_interval_s=stats_interval_s, **broker.factories())


def tel(unit: str, ts: int, **kw) -> dict:
    return {"unit_uuid": unit, "ts": ts, "lat": 49.1, "lon": 142.6, "speed_kmh": 10.0, **kw}


def feed(broker: FakeBroker, n: int, start_ts: int = 100, unit: str = "T-01") -> None:
    for i in range(n):
        broker.append(TEL, tel(unit, start_ts + i), key=unit)


def run_until_idle(app: ServiceApp, broker: FakeBroker, idle: int = 2) -> dict:
    stop = threading.Event()
    broker.on_idle = stop_after_idle(stop, idle)
    return app.run(stop)


def txn_calls(broker: FakeBroker) -> list:
    """Producer calls grouped: produce calls collapsed to their topic, idle polls dropped."""
    return [c[1] if c[0] == "produce" else c[0] for c in broker.calls if c[0] != "idle"]


def main_consumer(broker: FakeBroker, index: int = -1):
    """The app's group consumer (state readers are separate, short-lived consumers)."""
    return [c for c in broker.consumers if not c.eof][index]


def committed_offsets(broker: FakeBroker) -> list[int]:
    return [c[1][(TEL, 0)] for c in broker.calls if c[0] == "send_offsets_to_transaction"]


# ------------------------------------------------------------------------------------------------- startup

def test_topics_created_once_with_compaction():
    broker = FakeBroker()
    run_until_idle(make_app(broker), broker)
    assert broker.topics == {
        TEL: {"partitions": 1, "config": {}},
        QUEUE: {"partitions": 1, "config": {"cleanup.policy": "compact"}},
        DEC: {"partitions": 1, "config": {}},
        STATE: {"partitions": 1, "config": {"cleanup.policy": "compact", "segment.bytes": "16777216"}},
    }
    logs: list[str] = []
    run_until_idle(make_app(broker, logs=logs), broker)        # existing topics are fine
    assert not any("created topics" in m for m in logs)


def test_client_configs():
    app = make_app(FakeBroker(), transactional_id="tx-1", group_id="g-1", bootstrap="h:1")
    p, c, s = app.producer_config(), app.consumer_config(), app.state_reader_config()
    assert p["transactional.id"] == "tx-1" and p["enable.idempotence"] is True and p["bootstrap.servers"] == "h:1"
    assert c["group.id"] == "g-1" and c["isolation.level"] == "read_committed"
    assert c["enable.auto.commit"] is False and c["auto.offset.reset"] == "earliest"
    assert c["group.instance.id"] == "tx-1"                    # static membership: instant takeover on restart
    assert s["isolation.level"] == "read_committed" and s["enable.partition.eof"] is True
    assert s["group.id"] != "g-1"


def test_broker_unreachable_is_a_startup_error():
    broker = FakeBroker(down=True)
    with pytest.raises(StartupError, match="cannot create/check topic"):
        make_app(broker).run(threading.Event())
    assert broker.producers == []


def test_init_transactions_failure_is_a_startup_error():
    broker = FakeBroker()
    broker.fail("init_transactions", kerr(KafkaError._TIMED_OUT, retriable=True))
    with pytest.raises(StartupError, match="init_transactions"):
        make_app(broker).run(threading.Event())


# -------------------------------------------------------------------------------------------- transactions

def test_transaction_sequence_per_batch():
    broker = FakeBroker()
    feed(broker, 3)
    factory = StubFactory()
    stats = run_until_idle(make_app(broker, factory), broker)
    assert txn_calls(broker) == ["init_transactions", "begin_transaction", QUEUE, QUEUE, QUEUE, STATE,
                                 "send_offsets_to_transaction", "commit_transaction"]
    # the commit followed the first idle poll (idle with pending work)
    names = [c[0] for c in broker.calls]
    assert names.index("idle") < names.index("send_offsets_to_transaction")
    assert committed_offsets(broker) == [3]
    assert broker.group_offsets == {("vq-service", TEL, 0): 3}
    assert [v["n"] for v in broker.values(QUEUE)] == [1, 2, 3]
    (snap,) = broker.log(STATE)
    assert snap.key == b"vq-service"
    assert snap.json() == {"version": 1, "engine": factory.engines[0].snapshot(), "offsets": {f"{TEL}:0": 3}}
    assert stats["consumed"] == 3 and stats["commits"] == 1 and stats["aborts"] == 0
    assert stats["engine"] == {"accepted": 3, "ignored_old": 0}
    assert main_consumer(broker).closed and broker.producers[0].flushed


def test_no_transaction_without_input():
    broker = FakeBroker()
    run_until_idle(make_app(broker), broker, idle=3)
    assert txn_calls(broker) == ["init_transactions"]


def test_commit_cadence_by_message_count():
    broker = FakeBroker()
    feed(broker, 10)
    app = make_app(broker, commit_max_messages=4, commit_interval_ms=60_000)
    run_until_idle(app, broker)
    assert committed_offsets(broker) == [4, 8, 10]           # the last one on the idle poll
    assert [n for n, _ in main_consumer(broker).consume_calls[:3]] == [4, 4, 4]   # never more than the room left in the txn
    assert [v["n"] for v in broker.values(QUEUE)] == list(range(1, 11))


def test_commit_cadence_by_time():
    broker = FakeBroker(max_batch=1, per_consume_s=1 / 32)     # one message per poll, 31.25 ms apart
    feed(broker, 10)
    run_until_idle(make_app(broker, commit_interval_ms=100, commit_max_messages=500), broker)
    # txn starts at the first message; after 4 more polls 125 ms >= 100 ms have passed -> commit with 5 msgs
    assert committed_offsets(broker) == [5, 10]
    assert all(t <= 0.1 for _, t in main_consumer(broker).consume_calls)


def test_decision_headers_and_keys():
    broker = FakeBroker()
    broker.append(TEL, tel("T-04", 200, decide=True), key="T-04")
    run_until_idle(make_app(broker), broker)
    (dec,) = broker.log(DEC)
    assert dec.key == b"T-04" and dec.headers == [("decision-id", b"T-04:200")]
    assert dec.value == b'{"unit_uuid":"T-04","at":200,"result":"rejected","reason":"no_gain"}'
    (q,) = broker.log(QUEUE)
    assert q.key == b"S1" and not q.headers
    assert txn_calls(broker)[2:4] == [DEC, QUEUE]                # engine output order kept


def test_malformed_input_skipped_and_counted():
    broker = FakeBroker()
    for raw in (b"not json", b"[1, 2]", None, b"\xff\xfe{", b'"str"'):
        broker.append(TEL, raw, key="T-01")
    broker.append(TEL, tel("T-01", 100), key="T-01")
    factory = StubFactory()
    stats = run_until_idle(make_app(broker, factory), broker)
    assert stats["consumed"] == 6 and stats["malformed"] == 5
    assert factory.engines[0].seen == [tel("T-01", 100)]
    assert committed_offsets(broker) == [6]                     # malformed messages are committed past


def test_decode_telemetry():
    assert decode_telemetry(b'{"ts": 1}') == {"ts": 1}
    assert decode_telemetry(b'\xff{"ts": 1}') is None            # not UTF-8
    assert decode_telemetry(b"") is None and decode_telemetry(None) is None


# ------------------------------------------------------------------------------------------------- restore

def test_restore_on_assign_from_snapshot():
    broker = FakeBroker()
    feed(broker, 3)
    run_until_idle(make_app(broker), broker)
    feed(broker, 2, start_ts=103)
    broker.group_offsets[("vq-service", TEL, 0)] = 0            # the snapshot's offsets win
    factory = StubFactory()
    logs: list[str] = []
    stats = run_until_idle(make_app(broker, factory, logs=logs), broker)
    assert factory.calls[0] == {"n": 3, "last": {"T-01": 102}, "now": 102, "ignored": 0}
    assert main_consumer(broker).assigned_with[0] == [(TEL, 0, 3)]
    assert stats["consumed"] == 2 and stats["engine"]["accepted"] == 5
    assert [v["n"] for v in broker.values(QUEUE)] == [1, 2, 3, 4, 5]     # nothing lost, nothing twice
    assert broker.values(STATE)[-1]["offsets"] == {f"{TEL}:0": 5}
    assert any("restored from snapshot" in m and "1970-01-01T00:01:42Z" in m for m in logs)


def test_no_snapshot_fresh_engine_from_group_offset_or_earliest():
    broker = FakeBroker()
    feed(broker, 3)
    factory = StubFactory()
    run_until_idle(make_app(broker, factory), broker)
    assert factory.calls == [None]
    assert main_consumer(broker).assigned_with[0] == [(TEL, 0, OFFSET_INVALID)]
    assert committed_offsets(broker) == [3]


def test_restore_takes_last_snapshot_of_own_key():
    broker = FakeBroker()
    feed(broker, 6)

    def snap(n: int, off: int) -> dict:
        return {"version": 1, "engine": {"n": n, "last": {"T-01": 99 + n}, "now": 99 + n, "ignored": 0},
                "offsets": {f"{TEL}:0": off}}
    broker.append(STATE, snap(1, 1), key="vq-service")
    broker.append(STATE, snap(5, 5), key="other-instance")
    broker.append(STATE, snap(2, 2), key="vq-service")
    broker.append(STATE, snap(6, 6), key="other-instance")
    factory = StubFactory()
    run_until_idle(make_app(broker, factory), broker)
    assert factory.calls[0]["n"] == 2
    assert main_consumer(broker).assigned_with[0] == [(TEL, 0, 2)]
    assert [v["n"] for v in broker.values(QUEUE)] == [3, 4, 5, 6]


def test_restore_falls_back_to_a_full_scan_beyond_the_tail():
    broker = FakeBroker()
    broker.append(STATE, {"version": 1, "engine": {"n": 7, "last": {}, "now": 1, "ignored": 0},
                          "offsets": {f"{TEL}:0": 0}}, key="vq-service")
    for i in range(STATE_TAIL + 5):
        broker.append(STATE, {"version": 1, "engine": {}, "offsets": {}}, key="someone-else")
    factory = StubFactory()
    run_until_idle(make_app(broker, factory), broker)
    assert factory.calls[0]["n"] == 7


def test_tombstone_means_fresh_state():
    broker = FakeBroker()
    broker.append(STATE, {"version": 1, "engine": {"n": 7, "last": {}, "now": 1, "ignored": 0},
                          "offsets": {f"{TEL}:0": 0}}, key="vq-service")
    broker.append(STATE, None, key="vq-service")
    factory = StubFactory()
    run_until_idle(make_app(broker, factory), broker)
    assert factory.calls == [None]


def _snap(n: int, off: int) -> dict:
    return {"version": 1, "engine": {"n": n, "last": {"T-01": 99 + n}, "now": 99 + n, "ignored": 0},
            "offsets": {f"{TEL}:0": off}}


def test_restore_waits_until_the_snapshot_matches_the_committed_offsets():
    """Right after a commit the newest snapshot may not be readable yet: re-read until it catches up."""
    broker = FakeBroker()
    feed(broker, 4)
    broker.append(STATE, _snap(1, 1), key="vq-service")
    broker.group_offsets[("vq-service", TEL, 0)] = 2                # the commit with snapshot n=2 happened
    reads = [0]

    def becomes_visible(reader):
        reads[0] += 1
        if reads[0] == 3:
            broker.append(STATE, _snap(2, 2), key="vq-service")
    broker.on_state_reader = becomes_visible
    factory = StubFactory()
    run_until_idle(make_app(broker, factory), broker)
    assert broker.sleeps == [0.1, 0.1]
    assert factory.calls[0]["n"] == 2 and main_consumer(broker).assigned_with[0] == [(TEL, 0, 2)]
    assert [v["n"] for v in broker.values(QUEUE)] == [3, 4]


def test_restore_gives_up_waiting_and_uses_the_latest_snapshot():
    broker = FakeBroker()
    feed(broker, 3)
    broker.append(STATE, _snap(1, 1), key="vq-service")
    broker.group_offsets[("vq-service", TEL, 0)] = 2
    factory = StubFactory()
    logs: list[str] = []
    run_until_idle(make_app(broker, factory, logs=logs), broker)
    assert len(broker.sleeps) == 50 and any(m.startswith("warning: no snapshot") for m in logs)
    assert factory.calls[0]["n"] == 1 and main_consumer(broker).assigned_with[0] == [(TEL, 0, 1)]


def test_unsupported_snapshot_version_fails():
    broker = FakeBroker()
    broker.append(STATE, {"version": 99, "engine": {}, "offsets": {}}, key="vq-service")
    with pytest.raises(ValueError, match="version 1"):
        run_until_idle(make_app(broker), broker)


# --------------------------------------------------------------------------------------------- abort path

@pytest.mark.parametrize("method,skip", [
    ("commit_transaction", 1),            # the 2nd commit fails
    ("send_offsets_to_transaction", 1),
    ("produce", 4),                       # txn 1: 3 queue + state = 4 produce calls; the 5th fails
])
def test_abortable_error_aborts_restores_and_rewinds(method, skip):
    broker = FakeBroker()
    feed(broker, 6)
    broker.fail(method, kerr(KafkaError.INVALID_PRODUCER_EPOCH, abortable=True), skip=skip)
    factory = StubFactory()
    logs: list[str] = []
    stats = run_until_idle(make_app(broker, factory, logs=logs, commit_max_messages=3), broker)
    assert "abort_transaction" in txn_calls(broker)
    assert factory.calls[1] == {"n": 3, "last": {"T-01": 102}, "now": 102, "ignored": 0}   # last committed state
    assert broker.seeks == [(TEL, 0, 3)]
    assert [v["n"] for v in broker.values(QUEUE)] == [1, 2, 3, 4, 5, 6]
    assert committed_offsets(broker)[-1] == 6
    assert stats["aborts"] == 1 and stats["engine"]["accepted"] == 6
    assert any("transaction aborted" in m for m in logs)


def test_abort_before_the_first_commit_rewinds_to_the_start():
    broker = FakeBroker()
    broker.append(TEL, b"junk")
    feed(broker, 3)
    broker.fail("commit_transaction", kerr(KafkaError.INVALID_PRODUCER_EPOCH, abortable=True))
    factory = StubFactory()
    stats = run_until_idle(make_app(broker, factory), broker)
    assert factory.calls == [None, None]
    assert broker.seeks == [(TEL, 0, 0)]
    assert [v["n"] for v in broker.values(QUEUE)] == [1, 2, 3]
    assert committed_offsets(broker)[-1] == 4 and stats["aborts"] == 1


def test_retriable_commit_error_is_retried():
    broker = FakeBroker()
    feed(broker, 2)
    broker.fail("commit_transaction", kerr(KafkaError._TIMED_OUT, retriable=True))
    stats = run_until_idle(make_app(broker), broker)
    assert stats["aborts"] == 0 and stats["commits"] == 1
    assert [v["n"] for v in broker.values(QUEUE)] == [1, 2]


def test_fatal_error_raises_and_closes_without_committing():
    broker = FakeBroker()
    feed(broker, 2)
    broker.fail("commit_transaction", kerr(KafkaError._FENCED, fatal=True))
    with pytest.raises(KafkaException) as ei:
        run_until_idle(make_app(broker), broker)
    assert ei.value.args[0].fatal()
    assert main_consumer(broker).closed
    assert broker.log(QUEUE) == [] and "abort_transaction" not in txn_calls(broker)


# ------------------------------------------------------------------------------------------ stop, rebalance

def test_stop_commits_the_open_transaction():
    broker = FakeBroker()
    feed(broker, 3)
    stop = threading.Event()
    broker.after_consume = lambda c, msgs: msgs and stop.set()      # stop right after the first batch
    stats = make_app(broker, commit_interval_ms=60_000).run(stop)
    assert ("idle",) not in broker.calls                         # committed by the shutdown, not an idle poll
    assert committed_offsets(broker) == [3] and stats["commits"] == 1
    assert main_consumer(broker).closed


def test_revoke_commits_pending_and_reassign_restores():
    broker = FakeBroker(max_batch=2)
    feed(broker, 4)
    polls = [0]

    def hook(consumer):
        polls[0] += 1
        if polls[0] == 2:
            consumer.rebalance()                                # revoke while 2 messages are pending
    broker.before_consume = hook
    factory = StubFactory()
    run_until_idle(make_app(broker, factory, commit_interval_ms=60_000), broker)
    assert committed_offsets(broker) == [2, 4]
    assert factory.calls[0] is None and factory.calls[1]["n"] == 2
    assert main_consumer(broker).assigned_with[1] == [(TEL, 0, 2)]
    assert [v["n"] for v in broker.values(QUEUE)] == [1, 2, 3, 4]


# --------------------------------------------------------------------------------------------------- stats

def test_periodic_stats_line():
    broker = FakeBroker(max_batch=1, per_consume_s=4.0)
    feed(broker, 4)
    logs: list[str] = []
    run_until_idle(make_app(broker, logs=logs, stats_interval_s=10.0), broker)
    lines = [m for m in logs if m.startswith("stats: ")]
    assert len(lines) == 1
    assert "consumed 3 (0.2 msg/s)" in lines[0] and "lag 1" in lines[0] and "accepted=3" in lines[0]
    assert logs[-1].startswith("stopped: consumed 4") and "lag 0" in logs[-1]


def test_quiet_has_no_stats_line():
    broker = FakeBroker(max_batch=1, per_consume_s=4.0)
    feed(broker, 4)
    logs: list[str] = []
    run_until_idle(make_app(broker, logs=logs, stats_interval_s=None), broker)
    assert not any(m.startswith("stats: ") for m in logs)


def test_state_value_is_compact_json():
    broker = FakeBroker()
    feed(broker, 1)
    run_until_idle(make_app(broker), broker)
    raw = broker.log(STATE)[0].value
    assert b" " not in raw and json.loads(raw)["version"] == 1


# ------------------------------------------------------------------------------------- with the real engine

def test_restart_and_abort_match_offline_output(tmp_path):
    """Real engine, generator record: stop mid-stream, restart, an aborted transaction -> the committed output
    topics equal an uninterrupted offline run (no loss, no duplicates)."""
    gen_cli = pytest.importorskip("generator.cli")
    from vqservice.kafka_app import make_engine_factory
    from vqservice.offline import process_record, read_values
    from vqservice.site import load_site

    site_path = Path(__file__).resolve().parents[2] / "site.yaml"
    rec = tmp_path / "rec.jsonl"
    assert gen_cli.main(["run", "--site", str(site_path), "--no-kafka", "--speed", "0", "--quiet",
                         "--warmup-min", "60", "--duration-s", "600", "--record", str(rec)]) == 0
    cfg = make_cfg(commit_max_messages=500)
    site = load_site(site_path)

    class Lines:
        def __init__(self): self.items = []
        def write(self, obj): self.items.append(obj)
    expected = Lines()
    process_record(rec, make_engine_factory(site, cfg.engine)(None), expected)

    broker = FakeBroker(max_batch=200)
    for v in read_values(rec):
        broker.append(TEL, v, key=v["unit_uuid"])

    def app() -> ServiceApp:
        return ServiceApp(cfg, engine_factory=make_engine_factory(site, cfg.engine), log=lambda m: None,
                          stats_interval_s=None, **broker.factories())

    stop = threading.Event()
    polls = [0]

    def stop_after(n):
        def hook(consumer, msgs):
            polls[0] += 1
            if polls[0] >= n:
                stop.set()
        return hook
    broker.after_consume = stop_after(7)              # 1st run: stops after 7 polls (1400 msgs, mid-txn)
    app().run(stop)
    stop.clear()
    polls[0] = 0
    broker.after_consume = None
    broker.fail("commit_transaction", kerr(KafkaError.INVALID_PRODUCER_EPOCH, abortable=True), skip=2)
    broker.on_idle = stop_after_idle(stop, 2)
    stats = app().run(stop)                           # 2nd run: restore, one aborted txn, to the end
    assert stats["aborts"] == 1
    for topic in (QUEUE, DEC):
        want = [(x["key"].encode(), x["value"]) for x in expected.items if x["topic"] == topic]
        assert [(r.key, r.json()) for r in broker.log(topic)] == want
    assert len(broker.values(DEC)) > 0
