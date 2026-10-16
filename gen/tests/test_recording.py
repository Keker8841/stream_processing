import json
from concurrent.futures import Future

import pytest
from confluent_kafka import KafkaError, KafkaException

from generator.kafka_producer import (TelemetryProducer, encode_key, encode_value, ensure_topic,
                                      producer_config)
from generator.model import Emission, Telemetry, TruthEvent
from generator.recording import JsonlWriter, emission_to_line, read_record

EMS = [
    Emission(100, "T-01", Telemetry("T-01", 100, 49.1288, 142.6601, 36.0).to_dict()),
    Emission(100, "T-02", Telemetry("T-02", 100, 49.13, 142.66, 0.0).to_dict()),
    Emission(103, "T-01", Telemetry("T-01", 97, 49.1, 142.7, 36.0).to_dict()),
    Emission(103, "Т-кир", {"unit_uuid": "Т-кир", "ts": 103, "lat": 1.5, "lon": -2.25, "speed_kmh": 0.0}),
]


def write_record(path, ems=EMS):
    with JsonlWriter(path) as w:
        for em in ems:
            w.write(emission_to_line(em))


# --- recording ------------------------------------------------------------------------------------------------

def test_emission_line_key_order():
    assert list(emission_to_line(EMS[0])) == ["emit_at", "key", "value"]
    assert list(emission_to_line(EMS[0])["value"]) == ["unit_uuid", "ts", "lat", "lon", "speed_kmh"]


def test_round_trip_creates_dirs_and_writes_compact_lines(tmp_path):
    path = tmp_path / "a" / "b" / "rec.jsonl"
    write_record(path)
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == ('{"emit_at":100,"key":"T-01","value":{"unit_uuid":"T-01","ts":100,"lat":49.1288,'
                        '"lon":142.6601,"speed_kmh":36.0}}')
    assert "Т-кир" in lines[3]                                 # ensure_ascii=False
    assert all(" " not in line for line in lines)
    assert list(read_record(path)) == EMS
    assert list(read_record(str(path))) == EMS


def test_writer_context_manager_flushes_and_closes(tmp_path):
    path = tmp_path / "truth.jsonl"
    ev = TruthEvent(5, "T-03", "service_start", "S2")
    w = JsonlWriter(path)
    with w:
        w.write(ev.to_dict())
        w.write(TruthEvent(6, "T-03", "redirect_ignored", detail={"to_station": "S9", "reason": "x"}).to_dict())
        w.flush()
        assert path.read_text().count("\n") == 2
    assert w.closed and w.lines == 2
    w.close()                                                  # idempotent
    with pytest.raises(ValueError):
        w.write({"x": 1})
    first = json.loads(path.read_text().splitlines()[0])
    assert TruthEvent.from_dict(first) == ev


def test_writer_to_stdout(capsys):
    with JsonlWriter("-", line_buffered=True) as w:
        w.write(emission_to_line(EMS[1]))
    assert capsys.readouterr().out == json.dumps(emission_to_line(EMS[1]), separators=(",", ":")) + "\n"


def test_blank_lines_skipped(tmp_path):
    path = tmp_path / "rec.jsonl"
    write_record(path, EMS[:2])
    path.write_text(path.read_text().replace("\n", "\n\n", 1) + "  \n")
    assert list(read_record(path)) == EMS[:2]


@pytest.mark.parametrize("bad", [
    '{"emit_at":1,"key":"T-01","value":{"ts":1}',              # truncated
    '[1,2,3]',
    '{"emit_at":1,"key":"T-01"}',
    '{"emit_at":"1","key":"T-01","value":{"ts":1}}',
    '{"emit_at":true,"key":"T-01","value":{"ts":1}}',
    '{"emit_at":1,"key":7,"value":{"ts":1}}',
    '{"emit_at":1,"key":"T-01","value":{"lat":1}}',
    '{"emit_at":1,"key":"T-01","value":{"ts":1.5}}',
])
def test_malformed_line_reports_line_number(tmp_path, bad):
    path = tmp_path / "rec.jsonl"
    write_record(path, EMS[:2])
    with open(path, "a", encoding="utf-8") as f:
        f.write(bad + "\n")
    it = read_record(path)
    assert next(it) == EMS[0] and next(it) == EMS[1]
    with pytest.raises(ValueError, match=r"rec\.jsonl:3: malformed"):
        next(it)


# --- Kafka wire format (no broker) ----------------------------------------------------------------------------

def test_kafka_key_and_value_encoding_matches_record():
    em = EMS[0]
    assert encode_key(em.key) == b"T-01"
    assert encode_value(em.value) == (b'{"unit_uuid":"T-01","ts":100,"lat":49.1288,"lon":142.6601,'
                                      b'"speed_kmh":36.0}')
    assert json.loads(encode_value(EMS[3].value)) == EMS[3].value
    assert "Т-кир".encode() in encode_value(EMS[3].value)


def test_producer_config_defaults_and_override():
    cfg = producer_config("localhost:9092")
    assert cfg["bootstrap.servers"] == "localhost:9092"
    assert cfg["enable.idempotence"] is True and cfg["acks"] == "all"
    assert cfg["partitioner"] == "murmur2_random"
    cfg = producer_config("b:1", {"linger.ms": 50, "security.protocol": "SSL"})
    assert cfg["linger.ms"] == 50 and cfg["security.protocol"] == "SSL" and cfg["acks"] == "all"


class FakeProducer:
    def __init__(self, conf, buffer_errors=0):
        self.conf = conf
        self.buffer_errors = buffer_errors
        self.queue = []
        self.polls = []
        self.produced = []

    def produce(self, topic, value=None, key=None, on_delivery=None):
        if self.buffer_errors:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        self.produced.append((topic, key, value))
        self.queue.append(on_delivery)

    def poll(self, timeout):
        self.polls.append(timeout)

    def flush(self, timeout):
        for i, cb in enumerate(self.queue):
            cb(KafkaError(KafkaError._MSG_TIMED_OUT) if i == 1 else None, None)
        self.queue.clear()
        return 0


def test_producer_send_retries_buffer_errors_and_counts_failures():
    fake = None

    def factory(conf):
        nonlocal fake
        fake = FakeProducer(conf, buffer_errors=2)
        return fake

    p = TelemetryProducer("localhost:9092", "topic.x", producer_factory=factory)
    assert fake.conf["enable.idempotence"] is True
    for em in EMS[:3]:
        p.send(em)
    assert fake.polls.count(0.1) == 2                          # one poll per BufferError before retrying
    assert [(t, k) for t, k, _ in fake.produced] == [("topic.x", b"T-01"), ("topic.x", b"T-02"), ("topic.x", b"T-01")]
    assert fake.produced[0][2] == encode_value(EMS[0].value)
    assert p.sent == 3 and p.errors == 0
    assert p.flush(5) == 0
    assert p.errors == 1 and p.delivered == 2
    assert "MSG_TIMED_OUT" in p.last_error


class FakeAdmin:
    def __init__(self, outcome):
        self.outcome = outcome
        self.calls = []

    def create_topics(self, topics, **kw):
        self.calls.append((topics, kw))
        f: Future = Future()
        if self.outcome is None:
            f.set_result(None)
        else:
            f.set_exception(KafkaException(KafkaError(self.outcome)))
        return {topics[0].topic: f}


@pytest.mark.parametrize("outcome, created", [(None, True), (KafkaError.TOPIC_ALREADY_EXISTS, False)])
def test_ensure_topic(outcome, created):
    admin = FakeAdmin(outcome)
    assert ensure_topic("b:1", "telemetry.v1", 1, 1, admin_factory=lambda conf: admin) is created
    (topics, kw), = admin.calls
    assert topics[0].topic == "telemetry.v1" and topics[0].num_partitions == 1
    assert kw["operation_timeout"] == 10


def test_ensure_topic_other_errors_raise():
    admin = FakeAdmin(KafkaError.TOPIC_AUTHORIZATION_FAILED)
    with pytest.raises(KafkaException):
        ensure_topic("b:1", "telemetry.v1", admin_factory=lambda conf: admin)
