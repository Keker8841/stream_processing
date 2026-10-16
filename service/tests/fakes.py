"""Test doubles: an in-memory single-broker Kafka (Consumer / Producer / AdminClient) and a stub engine.

FakeBroker keeps one log per (topic, partition). Transactional records are buffered by the producer and appended
only on commit_transaction (so every reader effectively reads committed data); aborted ones are dropped.
`broker.calls` logs the producer's transactional API calls and idle polls in order.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, KafkaError, KafkaException, TopicPartition


class FakeClock:
    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def kerr(code: int, *, fatal: bool = False, retriable: bool = False, abortable: bool = False) -> KafkaException:
    return KafkaException(KafkaError(code, "injected", fatal=fatal, retriable=retriable, txn_requires_abort=abortable))


@dataclass
class Record:
    key: bytes | None
    value: bytes | None
    headers: list | None = None

    def json(self) -> Any:
        return json.loads(self.value)


class FakeMessage:
    def __init__(self, topic: str, partition: int, offset: int, rec: Record | None = None,
                 error: KafkaError | None = None) -> None:
        self._t, self._p, self._o, self._rec, self._err = topic, partition, offset, rec, error

    def topic(self) -> str: return self._t
    def partition(self) -> int: return self._p
    def offset(self) -> int: return self._o
    def key(self) -> bytes | None: return self._rec.key if self._rec else None
    def value(self) -> bytes | None: return self._rec.value if self._rec else None
    def headers(self) -> list | None: return self._rec.headers if self._rec else None
    def error(self) -> KafkaError | None: return self._err


class _Future:
    def __init__(self, fn: Callable[[], None]) -> None:
        self._fn = fn

    def result(self, timeout: float | None = None) -> None:
        self._fn()


@dataclass
class FakeBroker:
    clock: FakeClock = field(default_factory=FakeClock)
    down: bool = False                       # admin calls time out
    per_consume_s: float = 0.0               # clock advance per consume() returning data
    max_batch: int | None = None             # cap on messages returned by one consume()
    on_idle: Callable[[Any], None] | None = None          # main consumer got no data
    before_consume: Callable[[Any], None] | None = None   # e.g. trigger a rebalance inside consume()
    after_consume: Callable[[Any, list], None] | None = None
    on_state_reader: Callable[[Any], None] | None = None  # a consumer with enable.partition.eof was created
    logs: dict[tuple[str, int], list[Record]] = field(default_factory=dict)
    topics: dict[str, dict] = field(default_factory=dict)              # created via admin: partitions, config
    group_offsets: dict[tuple[str, str, int], int] = field(default_factory=dict)
    calls: list[tuple] = field(default_factory=list)
    seeks: list[tuple[str, int, int]] = field(default_factory=list)
    sleeps: list[float] = field(default_factory=list)
    consumers: list[FakeConsumer] = field(default_factory=list)
    producers: list[FakeProducer] = field(default_factory=list)
    _faults: dict[str, list[list]] = field(default_factory=dict)       # method -> [[skip, exc], ...]

    # --- data
    def log(self, topic: str, partition: int = 0) -> list[Record]:
        return self.logs.setdefault((topic, partition), [])

    def append(self, topic: str, value: Any, key: str | bytes | None = None, partition: int = 0) -> int:
        """Non-transactional produce (e.g. telemetry). dict values are JSON-encoded."""
        if isinstance(value, dict):
            value = json.dumps(value).encode()
        if isinstance(key, str):
            key = key.encode()
        log = self.log(topic, partition)
        log.append(Record(key, value))
        return len(log) - 1

    def values(self, topic: str, partition: int = 0) -> list[Any]:
        return [r.json() for r in self.log(topic, partition)]

    # --- fault injection: the call after `skip` successful ones raises `exc` (once per fail())
    def fail(self, method: str, exc: Exception, skip: int = 0) -> None:
        self._faults.setdefault(method, []).append([skip, exc])

    def _check(self, method: str) -> None:
        pending = self._faults.get(method)
        if not pending:
            return
        if pending[0][0] > 0:
            pending[0][0] -= 1
            return
        _, exc = pending.pop(0)
        raise exc

    # --- factories
    def consumer(self, conf: dict) -> FakeConsumer:
        c = FakeConsumer(self, conf)
        self.consumers.append(c)
        if c.eof and self.on_state_reader:
            self.on_state_reader(c)
        return c

    def producer(self, conf: dict) -> FakeProducer:
        p = FakeProducer(self, conf)
        self.producers.append(p)
        return p

    def admin(self, conf: dict) -> FakeAdmin:
        return FakeAdmin(self, conf)

    def factories(self) -> dict[str, Any]:
        return {"consumer_factory": self.consumer, "producer_factory": self.producer,
                "admin_factory": self.admin, "clock": self.clock, "sleep": self.sleeps.append}


class FakeAdmin:
    def __init__(self, broker: FakeBroker, conf: dict) -> None:
        self.broker, self.conf = broker, conf

    def create_topics(self, new_topics: list, operation_timeout: float = 0, request_timeout: float = 0) -> dict:
        def create(nt: Any) -> Callable[[], None]:
            def run() -> None:
                if self.broker.down:
                    raise kerr(KafkaError._TIMED_OUT)
                if nt.topic in self.broker.topics:
                    raise KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS, "exists"))
                self.broker.topics[nt.topic] = {"partitions": nt.num_partitions, "config": dict(nt.config)}
                self.broker.log(nt.topic, 0)
            return run
        return {nt.topic: _Future(create(nt)) for nt in new_topics}


class FakeProducer:
    def __init__(self, broker: FakeBroker, conf: dict) -> None:
        self.broker, self.conf = broker, conf
        self.in_txn = False
        self.initialized = False
        self.buffer: list[tuple[str, int, Record]] = []
        self.offsets: list[tuple[str, dict]] = []
        self.flushed = False

    def _call(self, name: str, *detail: Any) -> None:
        self.broker._check(name)
        self.broker.calls.append((name, *detail))

    def init_transactions(self, timeout: float | None = None) -> None:
        assert "transactional.id" in self.conf
        self._call("init_transactions")
        self.initialized = True

    def begin_transaction(self) -> None:
        if not self.initialized or self.in_txn:
            raise kerr(KafkaError._STATE)
        self._call("begin_transaction")
        self.in_txn = True

    def produce(self, topic: str, value: bytes | None = None, key: bytes | None = None, headers: list | None = None,
                partition: int | None = None, on_delivery: Any = None) -> None:
        if not self.in_txn:
            raise kerr(KafkaError._STATE)
        self._call("produce", topic)
        self.buffer.append((topic, 0 if partition is None else partition, Record(key, value, headers)))

    def send_offsets_to_transaction(self, positions: list, group_metadata: Any, timeout: float | None = None) -> None:
        if not self.in_txn:
            raise kerr(KafkaError._STATE)
        offs = {(tp.topic, tp.partition): tp.offset for tp in positions}
        self._call("send_offsets_to_transaction", offs)
        self.offsets.append((group_metadata[1], offs))

    def commit_transaction(self, timeout: float | None = None) -> None:
        if not self.in_txn:
            raise kerr(KafkaError._STATE)
        self._call("commit_transaction")
        for topic, partition, rec in self.buffer:
            self.broker.log(topic, partition).append(rec)
        for group, offs in self.offsets:
            for (t, p), o in offs.items():
                self.broker.group_offsets[(group, t, p)] = o
        self.buffer, self.offsets, self.in_txn = [], [], False

    def abort_transaction(self, timeout: float | None = None) -> None:
        self._call("abort_transaction")
        self.buffer, self.offsets, self.in_txn = [], [], False

    def poll(self, timeout: float = 0) -> int:
        return 0

    def flush(self, timeout: float = 0) -> int:
        self.flushed = True
        return 0


class FakeConsumer:
    def __init__(self, broker: FakeBroker, conf: dict) -> None:
        assert "group.id" in conf
        self.broker, self.conf = broker, conf
        self.group = conf["group.id"]
        self.eof = bool(conf.get("enable.partition.eof"))
        self.topics: list[str] = []
        self.callbacks: dict[str, Any] = {}
        self.positions: dict[tuple[str, int], int] = {}
        self.assigned_with: list[list[tuple[str, int, int]]] = []    # every assign() call: (topic, part, offset)
        self._eof_sent: set[tuple[str, int]] = set()
        self._assign_called = False
        self._needs_assign = False
        self.consume_calls: list[tuple[int, float]] = []
        self.closed = False

    # --- group membership
    def subscribe(self, topics: list[str], on_assign: Any = None, on_revoke: Any = None, on_lost: Any = None) -> None:
        self.topics = list(topics)
        self.callbacks = {"assign": on_assign, "revoke": on_revoke, "lost": on_lost}
        self._needs_assign = True

    def _partitions(self) -> list[TopicPartition]:
        return [TopicPartition(t, p) for t in self.topics
                for p in sorted(p for (tt, p) in self.broker.logs if tt == t) or [0]]

    def _rebalance_assign(self) -> None:
        self._needs_assign = False
        parts = self._partitions()
        self._assign_called = False
        if self.callbacks.get("assign"):
            self.callbacks["assign"](self, parts)
        if not self._assign_called:
            self.assign(parts)

    def rebalance(self) -> None:
        """Revoke everything, re-assign on the next consume() (eager protocol)."""
        if self.callbacks.get("revoke"):
            self.callbacks["revoke"](self, self.assignment())
        self.unassign()
        self._needs_assign = True

    def assign(self, partitions: list[TopicPartition]) -> None:
        self._assign_called = True
        self.positions = {}
        self._eof_sent = set()
        rec = []
        for tp in partitions:
            key = (tp.topic, tp.partition)
            end = len(self.broker.log(*key))
            if tp.offset >= 0:
                pos = tp.offset
            elif tp.offset == OFFSET_BEGINNING:
                pos = 0
            elif tp.offset == OFFSET_END:
                pos = end
            else:                                               # OFFSET_INVALID / STORED: group offset, earliest
                pos = self.broker.group_offsets.get((self.group, *key), 0)
            self.positions[key] = pos
            rec.append((tp.topic, tp.partition, tp.offset))
        self.assigned_with.append(rec)

    def unassign(self) -> None:
        self.positions = {}

    def assignment(self) -> list[TopicPartition]:
        return [TopicPartition(t, p) for (t, p) in self.positions]

    # --- consumption
    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[FakeMessage]:
        assert not self.closed
        self.consume_calls.append((num_messages, timeout))
        if self.broker.before_consume and not self.eof:
            self.broker.before_consume(self)
        if self._needs_assign:
            self._rebalance_assign()
        limit = num_messages if self.broker.max_batch is None else min(num_messages, self.broker.max_batch)
        out: list[FakeMessage] = []
        for key in list(self.positions):
            log = self.broker.log(*key)
            while len(out) < limit and self.positions[key] < len(log):
                off = self.positions[key]
                out.append(FakeMessage(key[0], key[1], off, log[off]))
                self.positions[key] = off + 1
            if self.eof and self.positions[key] >= len(log) and key not in self._eof_sent and len(out) < limit:
                self._eof_sent.add(key)
                out.append(FakeMessage(key[0], key[1], self.positions[key],
                                       error=KafkaError(KafkaError._PARTITION_EOF, "eof")))
        data = [m for m in out if m.error() is None]
        if data:
            self.broker.clock.advance(self.broker.per_consume_s)
        elif not self.eof:
            self.broker.clock.advance(max(timeout, 0))
            self.broker.calls.append(("idle",))
            if self.broker.on_idle:
                self.broker.on_idle(self)
        if self.broker.after_consume and not self.eof:
            self.broker.after_consume(self, out)
        return out

    def seek(self, tp: TopicPartition) -> None:
        key = (tp.topic, tp.partition)
        if key not in self.positions:
            raise kerr(KafkaError._STATE)
        self.positions[key] = tp.offset
        self.broker.seeks.append((tp.topic, tp.partition, tp.offset))

    def committed(self, partitions: list[TopicPartition], timeout: float | None = None) -> list[TopicPartition]:
        return [TopicPartition(tp.topic, tp.partition,
                               self.broker.group_offsets.get((self.group, tp.topic, tp.partition), OFFSET_INVALID))
                for tp in partitions]

    def get_watermark_offsets(self, tp: TopicPartition, timeout: float | None = None,
                              cached: bool = False) -> tuple[int, int]:
        if self.closed:
            raise RuntimeError("Consumer closed")
        return 0, len(self.broker.log(tp.topic, tp.partition))

    def consumer_group_metadata(self) -> tuple[str, str]:
        return ("group-metadata", self.group)

    def close(self) -> None:
        if self.positions and self.callbacks.get("revoke"):
            self.callbacks["revoke"](self, self.assignment())
        self.positions = {}
        self.closed = True


def stop_after_idle(stop: Any, n: int = 1) -> Callable[[Any], None]:
    """on_idle hook: set `stop` on the n-th idle poll."""
    count = [0]

    def hook(consumer: Any) -> None:
        count[0] += 1
        if count[0] >= n:
            stop.set()
    return hook


# ------------------------------------------------------------------------------------------------ stub engine

@dataclass(frozen=True)
class StubOutput:
    kind: str
    key: str
    value: dict
    headers: tuple[tuple[str, str], ...] = ()


class StubEngine:
    """Engine stand-in with the contract's interface. Per-unit dedup by ts; every accepted message -> one queue
    output for S1 with the running count; `"decide": true` additionally -> a decision (emitted first) with a
    decision-id header. Messages without unit_uuid / ts are ignored."""

    def __init__(self, snap: dict | None = None) -> None:
        snap = snap or {"n": 0, "last": {}, "now": None, "ignored": 0}
        self.n, self.last, self._now, self.ignored = snap["n"], dict(snap["last"]), snap["now"], snap["ignored"]
        self.restored_from = snap if snap.get("n") or snap.get("last") else None
        self.seen: list[dict] = []

    def process(self, raw: dict) -> list[StubOutput]:
        self.seen.append(raw)
        unit, ts = raw.get("unit_uuid"), raw.get("ts")
        if not isinstance(unit, str) or not isinstance(ts, int) or ts <= self.last.get(unit, -1):
            self.ignored += 1
            return []
        self.last[unit] = ts
        self.n += 1
        self._now = ts if self._now is None else max(self._now, ts)
        outs = []
        if raw.get("decide"):
            outs.append(StubOutput("decision", unit, {"unit_uuid": unit, "at": ts, "result": "rejected",
                                                      "reason": "no_gain"}, (("decision-id", f"{unit}:{ts}"),)))
        outs.append(StubOutput("queue", "S1", {"station_uuid": "S1", "n": self.n}))
        return outs

    def snapshot(self) -> dict:
        return {"n": self.n, "last": dict(self.last), "now": self._now, "ignored": self.ignored}

    @property
    def now(self) -> int | None:
        return self._now

    @property
    def stats(self) -> dict[str, int]:
        return {"accepted": self.n, "ignored_old": self.ignored}


class StubFactory:
    """engine_factory recording every call (None = fresh, else the restored snapshot)."""

    def __init__(self) -> None:
        self.calls: list[dict | None] = []
        self.engines: list[StubEngine] = []

    def __call__(self, snap: dict | None) -> StubEngine:
        self.calls.append(None if snap is None else json.loads(json.dumps(snap)))
        eng = StubEngine(snap)
        self.engines.append(eng)
        return eng
