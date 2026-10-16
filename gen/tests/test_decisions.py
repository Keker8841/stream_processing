import json
import time

import pytest
from confluent_kafka import OFFSET_END, KafkaError, KafkaException, TopicPartition

import generator.decisions as decisions
from generator.constants import DECISION_TOPIC
from generator.decisions import DecisionFollower, parse_decision, parse_iso_utc
from generator.model import Redirect

T_1200 = 1789473600   # 2026-09-15T12:00:00Z
RECOMMENDED = {"unit_uuid": "T4", "at": "2026-09-15T12:00:00Z", "result": "recommended",
               "from_station": "S1", "to_station": "S2", "gain_seconds": 80}
REJECTED = {"unit_uuid": "T4", "at": "2026-09-15T12:00:00Z", "result": "rejected", "reason": "no_gain"}


def _j(d) -> str:
    return json.dumps(d)


def test_parse_recommended_bytes_and_str():
    expected = Redirect(unit_uuid="T4", to_station="S2", at=T_1200)
    assert parse_decision(_j(RECOMMENDED)) == expected
    assert parse_decision(_j(RECOMMENDED).encode()) == expected
    assert parse_decision(bytearray(_j(RECOMMENDED).encode())) == expected
    assert parse_decision(memoryview(_j(RECOMMENDED).encode())) == expected


@pytest.mark.parametrize("at, expected", [
    ("2026-09-15T12:00:00Z", T_1200),
    ("2026-09-15T12:00:00+00:00", T_1200),
    ("2026-09-15T15:00:00+03:00", T_1200),
    ("2026-09-15T12:00:00.999Z", T_1200),        # fractions floored
    ("2026-09-15T12:00:01Z", T_1200 + 1),
])
def test_parse_at_formats(at, expected):
    assert parse_decision(_j({**RECOMMENDED, "at": at})).at == expected


def test_parse_minimal_and_extra_fields():
    minimal = {"result": "recommended", "unit_uuid": "T-07", "to_station": "S3", "at": "2026-09-15T12:00:05Z"}
    assert parse_decision(_j(minimal)) == Redirect("T-07", "S3", T_1200 + 5)
    assert parse_decision(_j({**minimal, "extra": [1, 2], "gain_seconds": None})) == Redirect("T-07", "S3", T_1200 + 5)


@pytest.mark.parametrize("doc", [
    REJECTED,
    {"result": "rejected"},                       # no other fields needed for non-recommended results
    {**RECOMMENDED, "result": "deferred"},
    {**RECOMMENDED, "result": "RECOMMENDED"},
    {"result": "", "at": "garbage"},
])
def test_non_recommended_is_none(doc):
    assert parse_decision(_j(doc)) is None


@pytest.mark.parametrize("raw", [
    "", "not json", "{", '{"result": "recommended",}', b"\xff\xfe{}",
    "[]", '"recommended"', "null", "42",
    None, 42, {"result": "recommended"},
])
def test_malformed_payload_raises(raw):
    with pytest.raises(ValueError):
        parse_decision(raw)


@pytest.mark.parametrize("change", [
    {"unit_uuid": None}, {"to_station": None}, {"at": None},
    {"unit_uuid": ""}, {"to_station": ""}, {"unit_uuid": 4}, {"to_station": ["S2"]},
    {"at": 1789473600}, {"at": "2026-09-15T12:00:00"}, {"at": "yesterday"}, {"at": ""},
])
def test_recommended_missing_or_bad_fields_raise(change):
    doc = {**RECOMMENDED, **change}
    doc = {k: v for k, v in doc.items() if v is not None}
    with pytest.raises(ValueError):
        parse_decision(_j(doc))


@pytest.mark.parametrize("doc", [{k: v for k, v in RECOMMENDED.items() if k != "result"},
                                 {**RECOMMENDED, "result": None}, {**RECOMMENDED, "result": 1}])
def test_missing_or_non_string_result_raises(doc):
    with pytest.raises(ValueError, match="result"):
        parse_decision(_j(doc))


def test_parse_iso_utc():
    assert parse_iso_utc("2026-09-15T12:00:00Z") == T_1200
    assert parse_iso_utc(" 2026-09-15T12:00:00+00:00 ") == T_1200
    assert parse_iso_utc("1970-01-01T00:00:00Z") == 0
    for bad in ("2026-09-15T12:00:00", "2026-09-15", "x", 1789473600, None):
        with pytest.raises(ValueError):
            parse_iso_utc(bad)


# --- DecisionFollower with a fake consumer (no broker) ----------------------------------------------------

class FakeMessage:
    def __init__(self, value=None, error=None):
        self._value, self._error = value, error

    def value(self):
        return self._value

    def error(self):
        return self._error


class FakeConsumer:
    def __init__(self, config):
        self.config = config
        self.queue: list[FakeMessage] = []
        self.consume_calls: list[tuple[int, float]] = []
        self.offsets_queries: list[list[tuple[str, int, int]]] = []
        self.offsets_result = None            # callable(query) -> list[TopicPartition] or exception
        self.assigned = None
        self.incremental_assigned = None
        self.closed = 0
        self.subscribed = None
        self.on_assign = None

    def subscribe(self, topics, on_assign=None):
        self.subscribed, self.on_assign = topics, on_assign

    def consume(self, num_messages=1, timeout=-1):
        self.consume_calls.append((num_messages, timeout))
        batch, self.queue = self.queue[:num_messages], self.queue[num_messages:]
        return batch

    def offsets_for_times(self, partitions, timeout=None):
        self.offsets_queries.append([(p.topic, p.partition, p.offset) for p in partitions])
        if isinstance(self.offsets_result, Exception):
            raise self.offsets_result
        return self.offsets_result(partitions)

    def assign(self, partitions):
        self.assigned = [(p.topic, p.partition, p.offset) for p in partitions]

    def incremental_assign(self, partitions):
        self.incremental_assigned = [(p.topic, p.partition, p.offset) for p in partitions]

    def close(self):
        self.closed += 1


def _follower(**kw) -> tuple[DecisionFollower, FakeConsumer]:
    made = []

    def factory(config):
        made.append(FakeConsumer(config))
        return made[-1]

    kw.setdefault("run_id", "r1")
    kw.setdefault("min_at", T_1200)
    f = DecisionFollower("localhost:9092", consumer_factory=factory, **kw)
    return f, made[0]


def _msg(doc) -> FakeMessage:
    return FakeMessage(value=_j(doc).encode())


def test_follower_config_and_subscription():
    before = int(time.time() * 1000)
    f, c = _follower(run_id="abc", extra_config={"client.id": "gen", "auto.offset.reset": "earliest"})
    after = int(time.time() * 1000)
    assert before <= f.start_ms <= after
    assert c.config == {
        "bootstrap.servers": "localhost:9092",
        "group.id": "gen-follow-abc",
        "isolation.level": "read_committed",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",           # extra_config wins
        "client.id": "gen",
    }
    assert c.subscribed == [DECISION_TOPIC]
    assert c.on_assign is not None
    assert DecisionFollower.__init__.__kwdefaults__["topic"] == DECISION_TOPIC
    f2, c2 = _follower(topic="other")
    assert c2.subscribed == ["other"] and c2.config["auto.offset.reset"] == "latest"


def test_on_assign_seeks_to_start_time():
    f, c = _follower()

    def lookup(parts):
        out = []
        for p in parts:
            tp = TopicPartition(p.topic, p.partition, {0: 42, 1: -1, 2: 7}[p.partition])
            out.append(tp)
        return out

    c.offsets_result = lookup
    parts = [TopicPartition(DECISION_TOPIC, i) for i in range(3)]
    c.on_assign(c, parts)
    assert c.offsets_queries == [[(DECISION_TOPIC, i, f.start_ms) for i in range(3)]]
    assert c.assigned == [(DECISION_TOPIC, 0, 42), (DECISION_TOPIC, 1, OFFSET_END), (DECISION_TOPIC, 2, 7)]
    assert [(p.partition, p.offset) for p in f.assigned] == [(0, 42), (1, OFFSET_END), (2, 7)]


def test_on_assign_falls_back_to_end_on_lookup_failure():
    f, c = _follower()
    c.offsets_result = KafkaException(KafkaError(KafkaError._TIMED_OUT))
    c.on_assign(c, [TopicPartition(DECISION_TOPIC, 0)])
    assert c.assigned == [(DECISION_TOPIC, 0, OFFSET_END)]


def test_on_assign_incremental_for_cooperative_strategy():
    f, c = _follower(extra_config={"partition.assignment.strategy": "cooperative-sticky"})
    c.offsets_result = lambda parts: [TopicPartition(p.topic, p.partition, 5) for p in parts]
    c.on_assign(c, [TopicPartition(DECISION_TOPIC, 0)])
    assert c.incremental_assigned == [(DECISION_TOPIC, 0, 5)] and c.assigned is None


def test_on_assign_empty_does_nothing():
    f, c = _follower()
    c.on_assign(c, [])
    assert c.offsets_queries == [] and c.assigned is None


def test_poll_filters_and_counts():
    f, c = _follower(min_at=T_1200)
    c.queue = [
        _msg(RECOMMENDED),                                                  # kept
        _msg({**RECOMMENDED, "unit_uuid": "T5", "at": "2026-09-15T11:59:59Z"}),   # stale
        _msg({**RECOMMENDED, "unit_uuid": "T6", "at": "2026-09-15T12:00:30Z"}),   # kept
        _msg(REJECTED),                                                     # ignored
        FakeMessage(value=b"{not json"),                                    # malformed
        FakeMessage(value=None),                                            # tombstone -> malformed
        _msg({"result": "recommended", "unit_uuid": "T7"}),                 # malformed (missing fields)
        FakeMessage(error=KafkaError(KafkaError._PARTITION_EOF)),           # skipped silently
        FakeMessage(error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)),    # skipped, counted
    ]
    assert f.poll() == [Redirect("T4", "S2", T_1200), Redirect("T6", "S2", T_1200 + 30)]
    assert (f.malformed, f.stale, f.ignored, f.errors) == (3, 1, 1, 1)
    assert all(timeout == 0 for _, timeout in c.consume_calls)
    assert f.poll() == []


def test_poll_drains_in_batches(monkeypatch):
    monkeypatch.setattr(decisions, "DRAIN_BATCH", 2)
    f, c = _follower()
    c.queue = [_msg({**RECOMMENDED, "unit_uuid": f"T{i}"}) for i in range(5)]
    assert [r.unit_uuid for r in f.poll()] == [f"T{i}" for i in range(5)]
    assert c.consume_calls == [(2, 0), (2, 0), (2, 0)]
    c.consume_calls.clear()
    c.queue = [_msg(RECOMMENDED), _msg(RECOMMENDED)]   # exactly one full batch -> one extra empty call
    assert len(f.poll()) == 2
    assert c.consume_calls == [(2, 0), (2, 0)]


def test_close_is_idempotent_and_context_manager():
    f, c = _follower()
    f.close()
    f.close()
    assert c.closed == 1
    with _follower()[0] as g:
        consumer = g._consumer
    assert consumer.closed == 1
