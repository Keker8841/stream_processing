"""Re-publish a recorded emission stream in its original order, optionally paced and time-shifted."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from .model import Emission
from .pacing import Pacer
from .recording import read_record


def shift_emission(em: Emission, shift_ts: int) -> Emission:
    """Copy of `em` with shift_ts added to emit_at and value["ts"]; `em` itself is not modified."""
    if not shift_ts:
        return em
    value = dict(em.value)
    value["ts"] = value["ts"] + shift_ts
    return Emission(emit_at=em.emit_at + shift_ts, key=em.key, value=value)


def first_emit_at(record_path: str | Path) -> int | None:
    """emit_at of the first emission (None for an empty record); used to compute an automatic shift."""
    for em in read_record(record_path):
        return em.emit_at
    return None


def replay(record_path: str | Path, sink: Callable[[Emission], None], *, speed: float = 0,
           shift_ts: int = 0, pacer_factory: Callable[[float], Pacer] = Pacer) -> int:
    """Send every emission of the record to `sink` in file order; returns the number sent.

    Emission i is due at (emit_at_i - emit_at_0) / speed wall seconds after the first one; speed <= 0 never sleeps.
    """
    pacer: Pacer | None = None
    first = 0
    n = 0
    for em in read_record(record_path):
        if pacer is None:
            first = em.emit_at
            pacer = pacer_factory(speed)
        pacer.wait_until(em.emit_at - first)
        sink(shift_emission(em, shift_ts))
        n += 1
    return n
