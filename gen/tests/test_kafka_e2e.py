"""End-to-end tests against a real broker. Skipped unless KAFKA_BOOTSTRAP is set (see scripts/kafka_local.sh)."""

import json
import os
import threading
import time
import uuid
from pathlib import Path

import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from generator.cli import main
from generator.constants import DEFAULT_START_TS
from generator.decisions import DecisionFollower
from generator.kafka_producer import ensure_topic
from generator.model import Redirect
from generator.recording import dumps_compact, read_record

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP")
pytestmark = [pytest.mark.kafka, pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_BOOTSTRAP not set")]

ROOT = Path(__file__).resolve().parent.parent
SITE = str(ROOT.parent / "site.yaml")         # repo-level site, shared with the service
SCENARIO = str(ROOT / "scenarios" / "task_example.yaml")
T_1200 = DEFAULT_START_TS


def unique(name: str) -> str:
    return f"e2e-{name}-{uuid.uuid4().hex[:8]}"


def consume(topic: str, n: int, timeout_s: float = 30.0, extra_wait_s: float = 1.0) -> list:
    """All messages of partition 0 from the beginning: waits for n, then `extra_wait_s` more for surplus ones."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": unique("reader"), "enable.auto.commit": False})
    c.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
    out = []
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline and len(out) < n:
            out += [m for m in c.consume(500, timeout=0.5) if m.error() is None]
        end = time.monotonic() + extra_wait_s
        while time.monotonic() < end:
            out += [m for m in c.consume(500, timeout=0.2) if m.error() is None]
    finally:
        c.close()
    return out


def assert_topic_equals_record(topic: str, record: Path, shift: int = 0) -> None:
    ems = list(read_record(record))
    msgs = consume(topic, len(ems))
    assert len(msgs) == len(ems)
    for m, em in zip(msgs, ems):
        assert m.partition() == 0
        assert m.key() == em.key.encode()
        value = dict(em.value, ts=em.value["ts"] + shift)
        assert m.value() == dumps_compact(value).encode()


@pytest.fixture(scope="module")
def faulty_record(tmp_path_factory) -> Path:
    rec = tmp_path_factory.mktemp("rec") / "rec.jsonl"
    assert main(["run", "--site", SITE, "--no-kafka", "--speed", "0", "--duration-s", "180", "--warmup-min", "5",
                 "--dup-prob", "0.05", "--late-prob", "0.05", "--lag-rate", "20", "--record", str(rec),
                 "--quiet"]) == 0
    return rec


def test_run_publishes_exactly_the_record(tmp_path):
    topic, rec = unique("telemetry"), tmp_path / "rec.jsonl"
    t0 = time.time()
    assert main(["run", "--site", SITE, "--bootstrap", BOOTSTRAP, "--topic", topic, "--speed", "0",
                 "--duration-s", "300", "--warmup-min", "5", "--dup-prob", "0.05", "--record", str(rec),
                 "--quiet"]) == 0
    meta = AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(topic, timeout=10)
    assert len(meta.topics[topic].partitions) == 1                       # created by the generator
    assert_topic_equals_record(topic, rec)
    # message timestamps are the broker/producer wall clock, not the simulated ts
    m = consume(topic, 1, extra_wait_s=0)[0]
    assert t0 - 5 <= m.timestamp()[1] / 1000 <= time.time() + 5


def test_decision_follower_filters(tmp_path):
    topic = unique("decision")
    ensure_topic(BOOTSTRAP, topic)
    p = Producer({"bootstrap.servers": BOOTSTRAP})

    def send(doc) -> None:
        p.produce(topic, key=b"k", value=doc if isinstance(doc, bytes) else json.dumps(doc).encode())

    rec = {"result": "recommended", "from_station": "S1", "gain_seconds": 80}
    send({**rec, "unit_uuid": "T-PRE", "to_station": "S2", "at": "2026-09-15T12:00:00Z"})   # before the follower
    p.flush(10)
    time.sleep(0.2)
    follower = DecisionFollower(BOOTSTRAP, topic=topic, run_id=unique("run"), min_at=T_1200)
    try:
        send({**rec, "unit_uuid": "T4", "to_station": "S2", "at": "2026-09-15T12:00:00Z"})
        send({"unit_uuid": "T4", "at": "2026-09-15T12:00:00Z", "result": "rejected", "reason": "no_gain"})
        send(b"not json")
        send({**rec, "unit_uuid": "T4", "to_station": "S3", "at": "2026-09-15T11:00:00Z"})  # older than min_at
        send({**rec, "unit_uuid": "T-07", "to_station": "S3", "at": "2026-09-15T12:00:05Z"})
        p.flush(10)
        got: list[Redirect] = []
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and len(got) < 2:
            t = time.monotonic()
            got += follower.poll()
            assert time.monotonic() - t < 1.0                              # non-blocking
            time.sleep(0.05)
        time.sleep(0.5)
        got += follower.poll()
        assert got == [Redirect("T4", "S2", T_1200), Redirect("T-07", "S3", T_1200 + 5)]
        assert (follower.ignored, follower.malformed, follower.stale, follower.errors) == (1, 1, 1, 0)
    finally:
        follower.close()


def test_closed_loop_redirect(tmp_path):
    tele, dec = unique("telemetry"), unique("decision")
    truth, rec = tmp_path / "truth.jsonl", tmp_path / "rec.jsonl"
    ensure_topic(BOOTSTRAP, tele)
    result: list[int] = []
    gen = threading.Thread(target=lambda: result.append(main([
        "run", "--scenario", SCENARIO, "--bootstrap", BOOTSTRAP, "--topic", tele, "--follow-decisions",
        "--decision-topic", dec, "--speed", "20", "--duration-s", "420", "--no-faults", "--truth", str(truth),
        "--record", str(rec), "--quiet"])))
    gen.start()
    # the "service": watch telemetry, publish the recommendation once T4 reports its 12:00:00 position
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": unique("service"), "enable.auto.commit": False})
    c.assign([TopicPartition(tele, 0, OFFSET_BEGINNING)])
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    published_at = None
    deadline = time.monotonic() + 60
    try:
        while published_at is None and time.monotonic() < deadline:
            for m in c.consume(100, timeout=0.2):
                if m.error() is None and m.key() == b"T4" and json.loads(m.value())["ts"] == T_1200:
                    p.produce(dec, key=b"T4", value=json.dumps({
                        "unit_uuid": "T4", "at": "2026-09-15T12:00:00Z", "result": "recommended",
                        "from_station": "S1", "to_station": "S2", "gain_seconds": 80}).encode())
                    p.flush(10)
                    published_at = time.monotonic()
                    break
    finally:
        c.close()
    gen.join(timeout=120)
    assert not gen.is_alive() and result == [0]
    assert published_at is not None
    events = [json.loads(x) for x in truth.read_text().splitlines()]
    applied = [e for e in events if e["event"] == "redirect_applied"]
    assert len(applied) == 1 and applied[0]["unit_uuid"] == "T4"
    assert applied[0]["detail"] == {"from_station": "S1", "to_station": "S2"}
    lag = applied[0]["ts"] - T_1200
    print(f"redirect applied {lag} sim s after 12:00:00 (speed 20)")
    assert 1 <= lag <= 60
    t4 = [(e["event"], e.get("station_uuid")) for e in events if e["unit_uuid"] == "T4"]
    assert ("service_start", "S2") in t4 and ("service_start", "S1") not in t4
    assert_topic_equals_record(tele, rec)


@pytest.mark.parametrize("shift", [0, 1000])
def test_replay_into_fresh_topic(faulty_record, shift):
    topic = unique("replay")
    assert main(["replay", "--record", str(faulty_record), "--bootstrap", BOOTSTRAP, "--topic", topic,
                 "--shift-ts", str(shift), "--quiet"]) == 0
    assert_topic_equals_record(topic, faulty_record, shift=shift)
