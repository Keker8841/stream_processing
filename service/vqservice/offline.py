"""Offline mode: feed a generator record (JSONL {"emit_at", "key", "value"}) into the engine, write outputs as JSONL.

Output line: {"topic": "queue.v1" | "decision.v1", "key": ..., "value": {...}, "headers": {...}} (compact JSON).
No Kafka involved; the record is processed in file (= emission) order, exactly as the Kafka app would see it.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, Protocol, TextIO

STDOUT = "-"
DEFAULT_TOPICS: Mapping[str, str] = {"queue": "queue.v1", "decision": "decision.v1"}


def dumps_compact(obj: Any) -> str:
    """Compact JSON used for output lines and Kafka values."""
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


class EngineLike(Protocol):
    def process(self, raw: Mapping[str, Any]) -> list[Any]: ...
    @property
    def stats(self) -> dict[str, int]: ...


class Sink(Protocol):
    def write(self, obj: dict) -> None: ...


def output_line(out: Any, topics: Mapping[str, str] = DEFAULT_TOPICS) -> dict:
    """engine.Output -> output line dict."""
    return {"topic": topics[out.kind], "key": out.key, "value": out.value, "headers": dict(out.headers)}


def read_values(path: str | Path) -> Iterator[dict]:
    """Telemetry values of a record file in file order; blank lines skipped, malformed lines -> ValueError."""
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except ValueError as e:
                raise ValueError(f"{path}:{lineno}: malformed record line: {e}") from None
            if not isinstance(obj, dict) or not isinstance(obj.get("value"), dict):
                raise ValueError(f"{path}:{lineno}: malformed record line: expected {{\"emit_at\", \"key\", \"value\"}}")
            yield obj["value"]


def process_record(record_path: str | Path, engine: EngineLike, sink: Sink, *,
                   topics: Mapping[str, str] = DEFAULT_TOPICS) -> dict[str, Any]:
    """Run every record line through `engine.process` and write the outputs to `sink`.

    Stops early when the sink reports `gone` (its reader went away). Returns counters (+ "engine": engine.stats).
    """
    stats: dict[str, Any] = {"lines": 0, "outputs": 0, "queue": 0, "decision": 0, "stopped": False}
    for value in read_values(record_path):
        if getattr(sink, "gone", False):
            stats["stopped"] = True
            break
        stats["lines"] += 1
        for out in engine.process(value):
            sink.write(output_line(out, topics))
            stats["outputs"] += 1
            stats[out.kind] += 1
    stats["stopped"] = stats["stopped"] or bool(getattr(sink, "gone", False))
    stats["engine"] = dict(engine.stats)
    return stats


def silence_stdout() -> None:
    """Point fd 1 at /dev/null so later stdout writes (incl. the flush at exit) no longer fail."""
    try:
        fd = sys.stdout.fileno()
    except (AttributeError, ValueError, OSError):    # stdout replaced (e.g. captured in tests)
        return
    devnull = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(devnull, fd)
    finally:
        os.close(devnull)


class JsonlSink:
    """Compact JSON lines to a file or stdout ("-"). Pipe-safe: when the reader goes away (BrokenPipeError,
    e.g. `| head`) it becomes `gone`, drops later writes and points stdout at /dev/null."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self.lines = 0
        self.gone = False
        self._own = self.path != STDOUT
        if self._own:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._f: TextIO = open(path, "w", encoding="utf-8", newline="\n")
        else:
            self._f = sys.stdout

    def _broken(self) -> None:
        self.gone = True
        if not self._own:
            silence_stdout()

    def write(self, obj: dict) -> None:
        if self.gone:
            return
        try:
            self._f.write(dumps_compact(obj) + "\n")
            self.lines += 1
        except BrokenPipeError:
            self._broken()

    def close(self) -> None:
        try:
            self._f.flush()
        except BrokenPipeError:
            self._broken()
        if self._own:
            self._f.close()
