"""JSON-lines files: the emission record (replayable) and generic writers (truth events, stdout)."""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, TextIO

from .model import Emission

STDOUT = "-"


def dumps_compact(obj: Any) -> str:
    """Compact JSON used for record lines and Kafka values."""
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


class JsonlWriter:
    """One compact JSON object per line. Creates parent directories; path "-" writes to stdout (never closed).

    Writes are block-buffered unless `line_buffered` is set; `flush()` and `close()` push the buffer out.
    """

    def __init__(self, path: str | Path, *, line_buffered: bool = False) -> None:
        self.path = path
        self.lines = 0
        self._own = str(path) != STDOUT
        if self._own:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            self._f: TextIO = open(p, "w", encoding="utf-8", newline="\n")
        else:
            self._f = sys.stdout
        self._line_buffered = line_buffered
        self._closed = False

    def write(self, obj: dict) -> None:
        if self._closed:
            raise ValueError(f"write to closed JsonlWriter({self.path!r})")
        self._f.write(dumps_compact(obj) + "\n")
        self.lines += 1
        if self._line_buffered:
            self._f.flush()

    def flush(self) -> None:
        if not self._closed:
            self._f.flush()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._f.flush()
        if self._own:
            self._f.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> JsonlWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def emission_to_line(em: Emission) -> dict:
    return {"emit_at": em.emit_at, "key": em.key, "value": em.value}


def line_to_emission(obj: Any) -> Emission:
    """Inverse of emission_to_line; raises ValueError on a malformed object."""
    if not isinstance(obj, dict):
        raise ValueError(f"expected a JSON object, got {type(obj).__name__}")
    missing = [k for k in ("emit_at", "key", "value") if k not in obj]
    if missing:
        raise ValueError(f"missing field(s) {', '.join(missing)}")
    emit_at, key, value = obj["emit_at"], obj["key"], obj["value"]
    if not isinstance(emit_at, int) or isinstance(emit_at, bool):
        raise ValueError(f"emit_at must be an integer, got {emit_at!r}")
    if not isinstance(key, str):
        raise ValueError(f"key must be a string, got {key!r}")
    if not isinstance(value, dict):
        raise ValueError(f"value must be an object, got {value!r}")
    ts = value.get("ts")
    if not isinstance(ts, int) or isinstance(ts, bool):
        raise ValueError(f"value.ts must be an integer, got {ts!r}")
    return Emission(emit_at=emit_at, key=key, value=value)


def read_record(path: str | Path) -> Iterator[Emission]:
    """Yield the emissions of a record file in file order. Blank lines are skipped."""
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                em = line_to_emission(json.loads(line))
            except ValueError as e:     # json.JSONDecodeError is a ValueError
                raise ValueError(f"{path}:{lineno}: malformed record line: {e}") from e
            yield em
