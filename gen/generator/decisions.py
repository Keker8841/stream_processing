"""Closed loop: parse decision.v1 messages and follow them from Kafka."""

from __future__ import annotations

import json
import logging
import math
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

from confluent_kafka import OFFSET_END, Consumer, KafkaError, KafkaException, TopicPartition

from .constants import DECISION_TOPIC
from .model import Redirect

log = logging.getLogger(__name__)

RECOMMENDED = "recommended"
DRAIN_BATCH = 500             # messages per consume() call while draining
OFFSETS_TIMEOUT_S = 10.0      # offsets_for_times lookup on partition assignment


def parse_iso_utc(text: str) -> int:
    """ISO-8601 timestamp with an explicit offset ("...Z", "+00:00") -> epoch seconds (fractions floored)."""
    if not isinstance(text, str):
        raise ValueError(f"timestamp must be a string, got {text!r}")
    try:
        dt = datetime.fromisoformat(text.strip())
    except ValueError:
        raise ValueError(f"bad ISO-8601 timestamp {text!r}") from None
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"timestamp {text!r} has no timezone")
    return math.floor(dt.timestamp())


def parse_decision(raw: bytes | str) -> Redirect | None:
    """Redirect for a "recommended" decision, None for any other result; ValueError if malformed."""
    if isinstance(raw, (bytes, bytearray, memoryview)):
        try:
            raw = bytes(raw).decode("utf-8")
        except UnicodeDecodeError as e:
            raise ValueError(f"decision is not UTF-8: {e}") from None
    if not isinstance(raw, str):
        raise ValueError(f"decision must be bytes or str, got {type(raw).__name__}")
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"decision is not valid JSON: {e}") from None
    if not isinstance(doc, dict):
        raise ValueError(f"decision must be a JSON object, got {type(doc).__name__}")
    result = doc.get("result")
    if not isinstance(result, str):
        raise ValueError(f"decision has no string 'result': {result!r}")
    if result != RECOMMENDED:
        return None
    unit = _required_str(doc, "unit_uuid")
    to_station = _required_str(doc, "to_station")
    at = parse_iso_utc(_required_str(doc, "at"))
    return Redirect(unit_uuid=unit, to_station=to_station, at=at)


def _required_str(doc: dict, key: str) -> str:
    v = doc.get(key)
    if not isinstance(v, str) or not v:
        raise ValueError(f"recommended decision needs a non-empty string {key!r}, got {v!r}")
    return v


class DecisionFollower:
    """Non-blocking consumer of decision.v1 that yields recommended redirects with at >= min_at.

    A fresh group per run never commits offsets. On partition assignment it seeks to the first offset whose
    timestamp is >= the wall-clock time of construction, so decisions produced while the group was still
    joining are not lost (plain auto.offset.reset=latest would skip them) and older runs are ignored.
    """

    def __init__(self, bootstrap: str, *, topic: str = DECISION_TOPIC, run_id: str, min_at: int,
                 extra_config: dict | None = None,
                 consumer_factory: Callable[[dict], Any] = Consumer) -> None:
        self.start_ms = int(time.time() * 1000)
        self.topic = topic
        self.min_at = int(min_at)
        self.group_id = f"gen-follow-{run_id}"
        self.malformed = 0        # undecodable / invalid messages (skipped)
        self.ignored = 0          # well-formed, result != recommended
        self.stale = 0            # recommended, but at < min_at
        self.errors = 0           # consumer errors other than partition EOF
        self.assigned: list[TopicPartition] = []
        self._logged_codes: set[int] = set()
        self._closed = False
        self.config: dict[str, Any] = {
            "bootstrap.servers": bootstrap,
            "group.id": self.group_id,
            "isolation.level": "read_committed",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "latest",
        }
        self.config.update(extra_config or {})
        self._incremental = (self.config.get("group.protocol") == "consumer"
                             or "cooperative" in str(self.config.get("partition.assignment.strategy", "")))
        self._consumer = consumer_factory(dict(self.config))
        self._consumer.subscribe([topic], on_assign=self._on_assign)

    def _on_assign(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        if not partitions:
            return
        query = [TopicPartition(p.topic, p.partition, self.start_ms) for p in partitions]
        try:
            found = consumer.offsets_for_times(query, timeout=OFFSETS_TIMEOUT_S)
        except KafkaException as e:
            log.warning("offsets_for_times failed (%s); starting %s at the end", e, self.topic)
            found = [TopicPartition(p.topic, p.partition, OFFSET_END) for p in partitions]
        start = [TopicPartition(tp.topic, tp.partition,
                                tp.offset if tp.error is None and tp.offset >= 0 else OFFSET_END)
                 for tp in found]
        if self._incremental:
            consumer.incremental_assign(start)
        else:
            consumer.assign(start)
        self.assigned = start

    def poll(self) -> list[Redirect]:
        """Everything available right now, never blocks."""
        out: list[Redirect] = []
        while True:
            msgs = self._consumer.consume(num_messages=DRAIN_BATCH, timeout=0)
            for msg in msgs:
                redirect = self._handle(msg)
                if redirect is not None:
                    out.append(redirect)
            if len(msgs) < DRAIN_BATCH:
                return out

    def _handle(self, msg: Any) -> Redirect | None:
        err = msg.error()
        if err is not None:
            if err.code() != KafkaError._PARTITION_EOF:
                self.errors += 1
                if err.code() not in self._logged_codes:
                    self._logged_codes.add(err.code())
                    log.warning("decision consumer error on %s: %s", self.topic, err)
            return None
        try:
            redirect = parse_decision(msg.value())
        except ValueError as e:
            self.malformed += 1
            log.debug("skipping malformed decision: %s", e)
            return None
        if redirect is None:
            self.ignored += 1
            return None
        if redirect.at < self.min_at:
            self.stale += 1
            return None
        return redirect

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._consumer.close()

    def __enter__(self) -> DecisionFollower:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
