"""Kafka application: telemetry.v1 -> Engine -> queue.v1 / decision.v1, exactly-once via Kafka transactions.

One transaction = the outputs of a batch of input messages + a full engine snapshot (compacted state topic,
key = state_key, partition 0) + the input offsets (send_offsets_to_transaction). On partition assignment the
engine is restored from the last committed snapshot and the input is sought to the offsets stored in it, so a
restart or a fenced zombie never loses or duplicates outputs for read_committed readers.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from .offline import dumps_compact

if TYPE_CHECKING:
    from .config import EngineConfig, ServiceConfig
    from .site import Site

STATE_VERSION = 1
STATE_PARTITION = 0
STATE_TAIL = 64                 # restore scans the last N offsets of the state topic first, then all of it
COMPACT = {"cleanup.policy": "compact"}
# a snapshot every commit (100 ms): small segments so compaction (never applied to the active one) keeps up
STATE_TOPIC_CONFIG = {**COMPACT, "segment.bytes": str(16 * 1024 * 1024)}
POLL_TIMEOUT_S = 0.1
TOPIC_TIMEOUT_S = 10.0
TXN_TIMEOUT_S = 30.0
TXN_RETRIES = 10
STATE_READ_TIMEOUT_S = 30.0
# consume() waits for a full batch or its timeout, and the last state offset is always a transaction marker that is
# never delivered: the scan ends on partition EOF, so keep each wait short (restore time)
STATE_SCAN_POLL_S = 0.05
STATE_CATCHUP_TRIES = 50        # x 0.1 s: wait for the last commit's markers to make its snapshot visible
FLUSH_TIMEOUT_S = 10.0
STATS_INTERVAL_S = 10.0

TP = tuple[str, int]
EngineFactory = Callable[[Mapping[str, Any] | None], Any]     # engine snapshot (None = fresh) -> engine


class StartupError(Exception):
    """Kafka unreachable or topics/transactions cannot be set up; nothing was processed."""


def make_engine_factory(site: Site, cfg: EngineConfig) -> EngineFactory:
    from .engine import Engine

    def make(snap: Mapping[str, Any] | None) -> Engine:
        return Engine(site, cfg) if snap is None else Engine.restore(site, cfg, snap)
    return make


def decode_telemetry(value: bytes | None) -> dict | None:
    """Telemetry JSON object, or None if the value is not decodable as one."""
    if value is None:
        return None
    try:
        obj = json.loads(value)
    except ValueError:              # JSONDecodeError and UnicodeDecodeError
        return None
    return obj if isinstance(obj, dict) else None


def parse_state(blob: bytes) -> dict:
    state = json.loads(blob)
    if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
        raise ValueError(f"unsupported state snapshot, expected version {STATE_VERSION}")
    return state


def _kafka_error(e: KafkaException) -> KafkaError | None:
    err = e.args[0] if e.args else None
    return err if isinstance(err, KafkaError) else None


def _iso(ts: int | None) -> str:
    return "-" if ts is None else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


_MISSING = object()


class ServiceApp:
    def __init__(self, cfg: ServiceConfig, *, engine_factory: EngineFactory,
                 consumer_factory: Callable[[dict], Any] = Consumer,
                 producer_factory: Callable[[dict], Any] = Producer,
                 admin_factory: Callable[[dict], Any] = AdminClient,
                 clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep,
                 log: Callable[[str], None] = print,
                 stats_interval_s: float | None = STATS_INTERVAL_S) -> None:
        self.cfg = cfg
        self.k = cfg.kafka
        self._engine_factory = engine_factory
        self._consumer_factory = consumer_factory
        self._producer_factory = producer_factory
        self._admin_factory = admin_factory
        self._clock = clock
        self._sleep = sleep
        self._log = log
        self._stats_interval_s = stats_interval_s
        self._topics = {"queue": self.k.queue_topic, "decision": self.k.decision_topic}
        self.consumer: Any = None
        self.producer: Any = None
        self.engine: Any = None
        self.counters = {"consumed": 0, "malformed": 0, "commits": 0, "aborts": 0, "consumer_errors": 0}
        self._in_txn = False
        self._txn_started = 0.0
        self._txn_msgs = 0
        self._offsets: dict[TP, int] = {}           # next offset to process
        self._committed: dict[TP, int] = {}         # next offset as of the last commit (or restore)
        self._committed_state: bytes | None = None  # state value of the last commit (None = fresh engine)
        self._failed = False
        self._logged_codes: set[int] = set()
        self._last_report = 0.0
        self._last_consumed = 0

    # ---------------------------------------------------------------------------------------------- configs

    def producer_config(self) -> dict[str, Any]:
        return {"bootstrap.servers": self.k.bootstrap, "client.id": "vq-service",
                "transactional.id": self.k.transactional_id, "enable.idempotence": True, "acks": "all",
                "linger.ms": 5, "partitioner": "murmur2_random"}

    def consumer_config(self) -> dict[str, Any]:
        # static membership: a restarted instance (same transactional.id) takes its partition back at once instead
        # of waiting session.timeout.ms (45 s) for the coordinator to drop the dead member
        return {"bootstrap.servers": self.k.bootstrap, "client.id": "vq-service", "group.id": self.k.group_id,
                "group.instance.id": self.k.transactional_id,
                "isolation.level": "read_committed", "enable.auto.commit": False,
                "enable.auto.offset.store": False, "auto.offset.reset": "earliest"}

    def state_reader_config(self) -> dict[str, Any]:
        # assign()-only consumer: never joins or commits to this group. It reads up to partition EOF, which is
        # only reported by a fetch that comes back empty: a short broker wait keeps restore fast (default 500 ms)
        return {"bootstrap.servers": self.k.bootstrap, "client.id": "vq-service-state",
                "group.id": f"{self.k.group_id}.state-reader", "isolation.level": "read_committed",
                "enable.auto.commit": False, "enable.partition.eof": True, "auto.offset.reset": "earliest",
                "fetch.wait.max.ms": 10}

    # ---------------------------------------------------------------------------------------------- startup

    def ensure_topics(self) -> list[str]:
        """Create missing topics (1 partition each, queue/state compacted). Returns the created ones."""
        specs = {self.k.telemetry_topic: {}, self.k.queue_topic: COMPACT, self.k.decision_topic: {},
                 self.k.state_topic: STATE_TOPIC_CONFIG}
        admin = self._admin_factory({"bootstrap.servers": self.k.bootstrap, "log_level": 0})
        futures = admin.create_topics(
            [NewTopic(t, num_partitions=1, replication_factor=-1, config=dict(c)) for t, c in specs.items()],
            operation_timeout=TOPIC_TIMEOUT_S, request_timeout=TOPIC_TIMEOUT_S)
        created = []
        for topic, fut in futures.items():
            try:
                fut.result(timeout=TOPIC_TIMEOUT_S + 5)
                created.append(topic)
            except KafkaException as e:
                err = _kafka_error(e)
                if err is not None and err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    continue
                raise StartupError(f"cannot create/check topic {topic!r} on {self.k.bootstrap}: {e}") from None
            except TimeoutError:
                raise StartupError(f"creating topic {topic!r} on {self.k.bootstrap} timed out") from None
        return created

    def start(self) -> None:
        created = self.ensure_topics()
        if created:
            self._log(f"created topics: {', '.join(created)}")
        self.producer = self._producer_factory(self.producer_config())
        try:
            self.producer.init_transactions(TXN_TIMEOUT_S)
        except KafkaException as e:
            raise StartupError(f"init_transactions ({self.k.transactional_id!r}) failed: {e}") from None
        self.consumer = self._consumer_factory(self.consumer_config())
        self.consumer.subscribe([self.k.telemetry_topic], on_assign=self._on_assign, on_revoke=self._on_revoke,
                                on_lost=self._on_lost)
        self._last_report = self._clock()
        self._log(f"vq-service: {self.k.telemetry_topic} -> {self.k.queue_topic}, {self.k.decision_topic} "
                  f"on {self.k.bootstrap} (group {self.k.group_id}, transactional.id {self.k.transactional_id}, "
                  f"state {self.k.state_topic}/{self.k.state_key}, commit every {self.k.commit_interval_ms} ms "
                  f"or {self.k.commit_max_messages} messages)")

    # ------------------------------------------------------------------------------------------------- loop

    def run(self, stop: threading.Event) -> dict[str, Any]:
        """Process until `stop` is set, then commit the open transaction and close. Fatal errors raise."""
        try:
            self.start()
            while not stop.is_set():
                self.step()
                self._maybe_report()
            self._finish()
            stats, summary = self.stats, self.format_stats()    # before close() drops the assignment
        except BaseException:
            self._failed = True
            raise
        finally:
            self._close()
        self._log("stopped: " + summary)
        return stats

    def step(self) -> None:
        """One consume() call: process the batch, commit when due."""
        room = max(1, self.k.commit_max_messages - self._txn_msgs)
        msgs = self.consumer.consume(num_messages=room, timeout=self._poll_timeout())
        self.producer.poll(0)
        for msg in msgs:                    # first sight of a partition: its rewind point
            if msg.error() is None:
                self._committed.setdefault((msg.topic(), msg.partition()), msg.offset())
        try:
            got = 0
            for msg in msgs:
                err = msg.error()
                if err is not None:
                    self._consumer_error(err)
                    continue
                self._handle(msg)
                got += 1
            if self._in_txn and (got == 0 or self._txn_msgs >= self.k.commit_max_messages
                                 or self._clock() - self._txn_started >= self.k.commit_interval_ms / 1000):
                self._commit()
        except KafkaException as e:
            if not self._abortable(e):
                raise
            self._abort(str(_kafka_error(e)))

    def _poll_timeout(self) -> float:
        if not self._in_txn:
            return POLL_TIMEOUT_S
        left = self.k.commit_interval_ms / 1000 - (self._clock() - self._txn_started)
        return min(POLL_TIMEOUT_S, max(0.0, left))

    def _handle(self, msg: Any) -> None:
        tp = (msg.topic(), msg.partition())
        if not self._in_txn:
            self.producer.begin_transaction()
            self._in_txn = True
            self._txn_started = self._clock()
            self._txn_msgs = 0
        self._offsets[tp] = msg.offset() + 1
        self._txn_msgs += 1
        self.counters["consumed"] += 1
        raw = decode_telemetry(msg.value())
        if raw is None:
            self.counters["malformed"] += 1
            return
        for out in self.engine.process(raw):
            headers = [(k, v.encode()) for k, v in out.headers]
            self._produce(self._topics[out.kind], out.key, dumps_compact(out.value).encode(), headers=headers)

    def _produce(self, topic: str, key: str, value: bytes, *, headers: list | None = None,
                 partition: int | None = None) -> None:
        kw: dict[str, Any] = {}
        if headers:
            kw["headers"] = headers
        if partition is not None:
            kw["partition"] = partition
        while True:
            try:
                self.producer.produce(topic, value=value, key=key.encode(), **kw)
                return
            except BufferError:         # local queue full: serve delivery reports, retry
                self.producer.poll(0.1)

    def _consumer_error(self, err: KafkaError) -> None:
        if err.fatal():
            raise KafkaException(err)
        if err.code() == KafkaError._PARTITION_EOF:
            return
        self.counters["consumer_errors"] += 1
        if err.code() not in self._logged_codes:
            self._logged_codes.add(err.code())
            self._log(f"consumer error: {err.name()}: {err.str()}")

    # ------------------------------------------------------------------------------------------ transactions

    def state_value(self) -> bytes:
        offsets = {f"{t}:{p}": o for (t, p), o in sorted(self._offsets.items())}
        return dumps_compact({"version": STATE_VERSION, "engine": self.engine.snapshot(),
                              "offsets": offsets}).encode()

    def _commit(self) -> None:
        blob = self.state_value()
        self._produce(self.k.state_topic, self.k.state_key, blob, partition=STATE_PARTITION)
        positions = [TopicPartition(t, p, o) for (t, p), o in sorted(self._offsets.items())]
        self._txn_call(self.producer.send_offsets_to_transaction, positions,
                       self.consumer.consumer_group_metadata(), TXN_TIMEOUT_S)
        self._txn_call(self.producer.commit_transaction, TXN_TIMEOUT_S)
        self._in_txn = False
        self._txn_msgs = 0
        self._committed = dict(self._offsets)
        self._committed_state = blob
        self.counters["commits"] += 1

    def _txn_call(self, fn: Callable[..., Any], *args: Any) -> Any:
        """Call a transactional API, retrying retriable errors; abortable / fatal ones raise."""
        for attempt in range(1, TXN_RETRIES + 1):
            try:
                return fn(*args)
            except KafkaException as e:
                err = _kafka_error(e)
                if (err is None or not err.retriable() or err.txn_requires_abort() or err.fatal()
                        or attempt == TXN_RETRIES):
                    raise
                self._log(f"{getattr(fn, '__name__', 'transaction call')} failed ({err.name()}), retrying")

    def _abortable(self, e: KafkaException) -> bool:
        err = _kafka_error(e)
        ok = self._in_txn and err is not None and err.txn_requires_abort() and not err.fatal()
        if not ok:
            self._failed = True
        return ok

    def _abort(self, reason: str, *, rewind: bool = True) -> None:
        """Abort the open transaction; with `rewind` restore the engine and input positions of the last commit."""
        self._log(f"transaction aborted ({reason})" + (", rewinding to the last commit" if rewind else ""))
        self.producer.abort_transaction(TXN_TIMEOUT_S)
        self._in_txn = False
        self._txn_msgs = 0
        self.counters["aborts"] += 1
        if rewind:
            self.engine = self._make_engine(self._committed_state)
            for (t, p), off in sorted(self._committed.items()):
                self.consumer.seek(TopicPartition(t, p, off))
            self._offsets = dict(self._committed)

    def _make_engine(self, blob: bytes | None) -> Any:
        return self._engine_factory(None if blob is None else parse_state(blob)["engine"])

    # ------------------------------------------------------------------------------------------- rebalancing

    def _on_assign(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        self._reset_assignment()
        if not partitions:
            return
        blob, state = self._restore_point(consumer, partitions)
        self.engine = self._engine_factory(state["engine"] if state else None)
        self._committed_state = blob
        stored = state["offsets"] if state else {}
        for tp in partitions:
            off = stored.get(f"{tp.topic}:{tp.partition}")
            if off is not None:                 # else: committed group offset, or earliest
                tp.offset = int(off)
                self._offsets[(tp.topic, tp.partition)] = int(off)
        self._committed = dict(self._offsets)
        consumer.assign(partitions)
        where = ", ".join(f"{tp.topic}:{tp.partition}@{tp.offset if tp.offset >= 0 else 'group/earliest'}"
                          for tp in partitions)
        if state:
            self._log(f"assigned {where}: engine restored from snapshot (now {_iso(self.engine.now)})")
        else:
            self._log(f"assigned {where}: no snapshot, fresh engine")

    def _on_revoke(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        if self._in_txn and not self._failed:
            try:
                self._commit()
            except KafkaException as e:
                if not self._abortable(e):
                    raise
                self._abort(str(_kafka_error(e)), rewind=False)
        self._reset_assignment()

    def _on_lost(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        if self._in_txn and not self._failed:
            self._abort("partitions lost", rewind=False)     # offsets can no longer be committed
        self._reset_assignment()

    def _reset_assignment(self) -> None:
        self.engine = None
        self._offsets, self._committed = {}, {}
        self._committed_state = None

    def _restore_point(self, consumer: Any, partitions: list[TopicPartition]) -> tuple[bytes | None, dict | None]:
        """Last snapshot, re-read until it is not behind the group's committed offsets.

        Snapshot and offsets commit in one transaction, but right after a commit (e.g. revoke -> assign in this
        process) the state partition's commit marker may not be visible yet. committed() returns stable offsets
        under read_committed (KIP-447), so they tell which snapshot must be there.
        """
        group = {f"{tp.topic}:{tp.partition}": tp.offset
                 for tp in consumer.committed(partitions, timeout=STATE_READ_TIMEOUT_S) if tp.offset >= 0}
        for attempt in range(STATE_CATCHUP_TRIES):
            blob = self.read_state()
            state = parse_state(blob) if blob is not None else None
            stored = state["offsets"] if state else {}
            if all(stored.get(k, -1) >= off for k, off in group.items()):
                return blob, state
            self._sleep(0.1)
        self._log(f"warning: no snapshot in {self.k.state_topic} matches the committed offsets {group} of group "
                  f"{self.k.group_id}; using the latest one")
        return blob, state

    def read_state(self) -> bytes | None:
        """Value of the last committed snapshot for state_key (None if there is none)."""
        reader = self._consumer_factory(self.state_reader_config())
        try:
            tp = TopicPartition(self.k.state_topic, STATE_PARTITION)
            wm = reader.get_watermark_offsets(tp, timeout=STATE_READ_TIMEOUT_S)
            if wm is None:
                raise TimeoutError(f"watermark query for {self.k.state_topic} timed out")
            lo, hi = wm
            if hi <= lo:
                return None
            tail = max(lo, hi - STATE_TAIL)
            found = self._scan_state(reader, tail, hi)
            if found is _MISSING and tail > lo:
                found = self._scan_state(reader, lo, hi)
            return None if found is _MISSING else found     # a tombstone (None) also means "fresh"
        finally:
            reader.close()

    def _scan_state(self, reader: Any, start: int, hi: int) -> Any:
        reader.assign([TopicPartition(self.k.state_topic, STATE_PARTITION, start)])
        key = self.k.state_key.encode()
        found: Any = _MISSING
        deadline = self._clock() + STATE_READ_TIMEOUT_S
        while True:
            for m in reader.consume(num_messages=500, timeout=STATE_SCAN_POLL_S):
                err = m.error()
                if err is not None:
                    if err.code() == KafkaError._PARTITION_EOF:
                        return found
                    if err.fatal():
                        raise KafkaException(err)
                    continue
                if m.key() == key:
                    found = m.value()
                if m.offset() >= hi - 1:
                    return found
            if self._clock() > deadline:
                raise TimeoutError(f"reading {self.k.state_topic} did not reach its end "
                                   f"within {STATE_READ_TIMEOUT_S:.0f} s")

    # -------------------------------------------------------------------------------------------- shutdown

    def _finish(self) -> None:
        if not self._in_txn:
            return
        try:
            self._commit()
        except KafkaException as e:
            if not self._abortable(e):
                raise
            self._abort(str(_kafka_error(e)), rewind=False)    # reprocessed after the restart

    def _close(self) -> None:
        try:
            if self.consumer is not None:
                self.consumer.close()       # on_revoke runs here; nothing is pending after _finish()
        except Exception:
            if not self._failed:
                raise
        finally:
            if self.producer is not None:
                self.producer.flush(FLUSH_TIMEOUT_S)

    # ---------------------------------------------------------------------------------------------- stats

    @property
    def stats(self) -> dict[str, Any]:
        return {**self.counters, "lag": self.lag(), "engine": dict(self.engine.stats) if self.engine else {}}

    def lag(self) -> int | None:
        """Input messages behind the high watermark (cached by the consumer), None if unknown."""
        total = None
        for (t, p), nxt in self._offsets.items():
            try:
                wm = self.consumer.get_watermark_offsets(TopicPartition(t, p), cached=True)
            except (KafkaException, RuntimeError):     # RuntimeError: consumer closed
                continue
            if wm and wm[1] >= 0:
                total = (total or 0) + max(0, wm[1] - nxt)
        return total

    def format_stats(self, rate: float | None = None) -> str:
        c = self.counters
        eng = self.engine
        head = (f"consumed {c['consumed']}" + (f" ({rate:.1f} msg/s)" if rate is not None else "")
                + f", lag {self.lag()}, malformed {c['malformed']}, "
                f"commits {c['commits']}, aborts {c['aborts']}, now {_iso(eng.now if eng else None)}")
        return head + (" | " + " ".join(f"{k}={v}" for k, v in eng.stats.items()) if eng else "")

    def _maybe_report(self) -> None:
        if self._stats_interval_s is None:
            return
        now = self._clock()
        dt = now - self._last_report
        if dt < self._stats_interval_s:
            return
        rate = (self.counters["consumed"] - self._last_consumed) / dt
        self._last_report, self._last_consumed = now, self.counters["consumed"]
        self._log("stats: " + self.format_stats(rate))
