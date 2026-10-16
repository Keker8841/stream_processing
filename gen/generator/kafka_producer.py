"""Kafka sink for telemetry emissions: idempotent producer, key = unit_uuid, value = compact JSON."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .constants import TELEMETRY_TOPIC
from .model import Emission
from .recording import dumps_compact


def encode_key(key: str) -> bytes:
    return key.encode("utf-8")


def encode_value(value: Mapping[str, Any]) -> bytes:
    return dumps_compact(value).encode("utf-8")


def producer_config(bootstrap: str, extra_config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Producer settings; `extra_config` overrides the defaults."""
    cfg: dict[str, Any] = {
        "bootstrap.servers": bootstrap,
        "client.id": "vq-generator",
        "enable.idempotence": True,
        "acks": "all",
        "partitioner": "murmur2_random",
        "linger.ms": 5,
    }
    if extra_config:
        cfg.update(extra_config)
    return cfg


def _describe(err: Any) -> str:
    if isinstance(err, KafkaError):
        return f"{err.name()}: {err.str()}"
    return str(err)


def ensure_topic(bootstrap: str, topic: str, partitions: int = 1, replication: int = 1, timeout_s: float = 10, *,
                 extra_config: Mapping[str, Any] | None = None,
                 admin_factory: Callable[[dict], Any] = AdminClient) -> bool:
    """Create `topic` if missing. True if created, False if it already existed; any other failure raises."""
    conf: dict[str, Any] = {"bootstrap.servers": bootstrap}
    if extra_config:
        conf.update(extra_config)
    admin = admin_factory(conf)
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=replication)],
        operation_timeout=timeout_s, request_timeout=timeout_s)
    try:
        futures[topic].result(timeout=timeout_s + 5)
    except KafkaException as e:
        err = e.args[0] if e.args else None
        if isinstance(err, KafkaError) and err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
            return False
        raise
    except TimeoutError as e:
        raise TimeoutError(f"creating topic {topic!r} on {bootstrap} did not finish within {timeout_s + 5} s") from e
    return True


class TelemetryProducer:
    """Asynchronous producer. Delivery failures are counted, not raised; call flush() before exiting."""

    def __init__(self, bootstrap: str, topic: str = TELEMETRY_TOPIC, *, extra_config: dict | None = None,
                 producer_factory: Callable[[dict], Any] = Producer) -> None:
        self.topic = topic
        self.config = producer_config(bootstrap, extra_config)
        self._producer = producer_factory(self.config)
        self._sent = 0
        self._delivered = 0
        self._errors = 0
        self._last_error: str | None = None

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self._errors += 1
            self._last_error = _describe(err)
        else:
            self._delivered += 1

    def send(self, em: Emission) -> None:
        key = encode_key(em.key)
        value = encode_value(em.value)
        while True:
            try:
                self._producer.produce(self.topic, value=value, key=key, on_delivery=self._on_delivery)
                break
            except BufferError:
                # Local queue full: serve delivery reports to make room, then retry.
                self._producer.poll(0.1)
        self._sent += 1
        self._producer.poll(0)

    def poll(self, timeout: float = 0) -> None:
        self._producer.poll(timeout)

    def flush(self, timeout: float = 30) -> int:
        """Wait for outstanding deliveries; returns the number of messages still queued."""
        return int(self._producer.flush(timeout))

    @property
    def errors(self) -> int:
        return self._errors

    @property
    def last_error(self) -> str | None:
        return self._last_error

    @property
    def sent(self) -> int:
        return self._sent

    @property
    def delivered(self) -> int:
        return self._delivered
