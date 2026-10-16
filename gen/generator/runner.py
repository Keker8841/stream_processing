"""Run loop: simulator -> fault injector -> sinks (Kafka, record file, stdout), plus ground truth and pacing.

Per tick: poll decisions -> request_redirect -> sim.step() -> faults.push -> emit faults.pop_due to every sink ->
write truth -> pacer.wait_until(tick + 1). Warmup ticks are simulated silently before the first emitted tick and
never pass through the fault injector. On exit (duration reached, stop event, Ctrl+C) the fault buffer is drained
into the sinks and every sink, the truth writer and the follower are closed.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from .constants import DEFAULT_COORD_PRECISION, MANEUVER_S, SERVICE_S
from .faults import FaultConfig, FaultInjector
from .model import Emission, Redirect, ScriptedDelay, Site, UnitInit
from .pacing import Pacer
from .recording import STDOUT, JsonlWriter, emission_to_line
from .sim import MAX_COORD_PRECISION, Simulator


def iso_utc(ts: int | None) -> str:
    return "-" if ts is None else datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Sink(Protocol):
    name: str

    def send(self, em: Emission) -> None: ...
    def tick(self) -> None: ...                  # end of a paced tick: make output visible
    def close(self) -> dict[str, Any]: ...       # flush, release, return final stats


class Follower(Protocol):
    def poll(self) -> list[Redirect]: ...
    def close(self) -> None: ...


def silence_stdout() -> None:
    """Point fd 1 at /dev/null so buffered and later stdout writes (incl. the flush at exit) no longer fail."""
    try:
        fd = sys.stdout.fileno()
    except (AttributeError, ValueError, OSError):    # stdout replaced (e.g. captured in tests)
        return
    devnull = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(devnull, fd)
    finally:
        os.close(devnull)


class PipeSafeWriter:
    """JsonlWriter wrapper: when the reader goes away (BrokenPipeError, e.g. `| head`) it becomes `gone`,
    drops later writes and, for stdout, redirects fd 1 to /dev/null. The runner stops after that tick."""

    def __init__(self, writer: JsonlWriter) -> None:
        self.writer = writer
        self.path = writer.path
        self.gone = False

    def _guard(self, fn: Callable[..., None], *args: Any) -> None:
        if self.gone:
            return
        try:
            fn(*args)
        except BrokenPipeError:
            self.gone = True
            if str(self.path) == STDOUT:
                silence_stdout()

    def write(self, obj: dict) -> None:
        self._guard(self.writer.write, obj)

    def flush(self) -> None:
        self._guard(self.writer.flush)

    def close(self) -> None:
        try:
            self.writer.close()
        except BrokenPipeError:
            self.gone = True
            if str(self.path) == STDOUT:
                silence_stdout()

    @property
    def lines(self) -> int:
        return self.writer.lines


class RecordSink:
    """Emissions as record lines ({"emit_at", "key", "value"}) to a file or stdout ("-"); pipe-safe."""

    def __init__(self, path: str, *, name: str | None = None) -> None:
        self.name = name or ("stdout" if path == STDOUT else f"record:{path}")
        self.writer = PipeSafeWriter(JsonlWriter(path))

    @property
    def gone(self) -> bool:
        return self.writer.gone

    def send(self, em: Emission) -> None:
        self.writer.write(emission_to_line(em))

    def tick(self) -> None:
        self.writer.flush()

    def close(self) -> dict[str, Any]:
        self.writer.close()
        stats: dict[str, Any] = {"lines": self.writer.lines}
        if self.gone:
            stats["reader_closed"] = True
        return stats


class KafkaSink:
    """Adapter for TelemetryProducer: flushes on close and reports delivery stats."""

    def __init__(self, producer: Any, *, flush_timeout_s: float = 30.0, name: str | None = None) -> None:
        self.producer = producer
        self.flush_timeout_s = flush_timeout_s
        self.name = name or f"kafka:{producer.topic}"

    def send(self, em: Emission) -> None:
        self.producer.send(em)

    def tick(self) -> None:
        self.producer.poll(0)

    def close(self) -> dict[str, Any]:
        unflushed = self.producer.flush(self.flush_timeout_s)
        return {"sent": self.producer.sent, "delivered": self.producer.delivered, "errors": self.producer.errors,
                "unflushed": unflushed, "last_error": self.producer.last_error}


@dataclass(frozen=True)
class RunConfig:
    site: Site
    start_ts: int                                  # ts of the first emitted tick
    duration_s: int | None = None                  # emitted ticks; None = until stopped
    warmup_s: int = 0                              # silent ticks simulated before start_ts
    seed: int = 1                                  # fault injector seed
    speed: float = 1.0                             # 1 = real time, N = N x, <= 0 = as fast as possible
    service_s: int = SERVICE_S
    maneuver_s: int = MANEUVER_S
    coord_precision: int = DEFAULT_COORD_PRECISION
    initial: tuple[UnitInit, ...] = ()
    faults: FaultConfig = field(default_factory=FaultConfig)
    delays: tuple[ScriptedDelay, ...] = ()         # scenario out-of-order deliveries, on top of `faults`
    run_id: str = "run"

    def __post_init__(self) -> None:
        for name, lo, hi in (("service_s", 1, None), ("maneuver_s", 0, None),
                             ("coord_precision", 0, MAX_COORD_PRECISION)):
            v = getattr(self, name)
            if not isinstance(v, int) or isinstance(v, bool) or v < lo or (hi is not None and v > hi):
                raise ValueError(f"{name} must be an int in [{lo}, {hi if hi is not None else 'inf'}], got {v!r}")
        if self.duration_s is not None and self.duration_s < 0:
            raise ValueError(f"duration_s must be >= 0, got {self.duration_s}")
        if self.warmup_s < 0:
            raise ValueError(f"warmup_s must be >= 0, got {self.warmup_s}")
        if self.warmup_s and self.initial:
            raise ValueError("warmup must be 0 when initial unit states are given (scenario runs)")


@dataclass
class RunSummary:
    run_id: str
    start_ts: int
    end_ts: int | None = None                      # ts of the last emitted tick
    ticks: int = 0                                 # emitted ticks (warmup excluded)
    warmup_ticks: int = 0
    records: int = 0                               # telemetry records produced by the simulator
    emissions: int = 0                             # emissions handed to the sinks (incl. duplicates and drain)
    drained: int = 0                               # emissions released at shutdown
    truth_events: int = 0
    redirects_received: int = 0
    redirects_applied: int = 0
    redirects_ignored: int = 0
    redirects_pending: int = 0                     # received with `at` after the last tick, never requested
    faults: dict[str, int] = field(default_factory=dict)
    sinks: dict[str, dict[str, Any]] = field(default_factory=dict)
    follower: dict[str, Any] | None = None
    interrupted: bool = False                      # stopped by a signal / stop event / Ctrl+C
    output_closed: list[str] = field(default_factory=list)   # outputs whose reader went away (run stopped)
    wall_s: float = 0.0
    max_behind_s: float = 0.0                      # worst lag behind the pacing deadline
    close_errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """No delivery errors, nothing left unflushed, every sink closed cleanly."""
        bad = any(s.get("errors") or s.get("unflushed") for s in self.sinks.values())
        return not bad and not self.close_errors

    def format(self) -> str:
        iso = iso_utc
        lines = [
            f"run {self.run_id}: {self.ticks} ticks {iso(self.start_ts)} .. {iso(self.end_ts)}"
            f" (warmup {self.warmup_ticks} s){' INTERRUPTED' if self.interrupted else ''}"
            f"{' STOPPED: reader closed ' + ', '.join(self.output_closed) if self.output_closed else ''},"
            f" wall {self.wall_s:.1f} s,"
            f" max behind {self.max_behind_s:.2f} s",
            f"  records {self.records}, emissions {self.emissions} (drained at exit {self.drained}),"
            f" truth events {self.truth_events}",
            "  faults " + " ".join(f"{k}={v}" for k, v in self.faults.items()),
        ]
        for name, stats in self.sinks.items():
            lines.append(f"  {name}: " + " ".join(f"{k}={v}" for k, v in stats.items()))
        if self.follower is not None:
            lines.append(f"  decisions: received {self.redirects_received}, applied {self.redirects_applied},"
                         f" ignored {self.redirects_ignored}, pending {self.redirects_pending}; "
                         + " ".join(f"{k}={v}" for k, v in self.follower.items()))
        for err in self.close_errors:
            lines.append(f"  close error: {err}")
        return "\n".join(lines)


FOLLOWER_COUNTERS = ("malformed", "ignored", "stale", "errors")


def run(cfg: RunConfig, *, sinks: Sequence[Sink] = (), truth: JsonlWriter | PipeSafeWriter | None = None,
        follower: Follower | None = None, pacer: Pacer | None = None, stop: threading.Event | None = None,
        progress: Callable[[RunSummary, int], None] | None = None, progress_every: int = 0) -> RunSummary:
    """Run the simulation, emit to `sinks`, write truth events. Owns and closes sinks, truth and follower.

    `pacer` defaults to Pacer(cfg.speed) created right before the first emitted tick, sleeping on `stop` so a
    stop request interrupts the wait. `progress(summary, sim_now)` is called every `progress_every` ticks.
    """
    stop = stop or threading.Event()
    summary = RunSummary(run_id=cfg.run_id, start_ts=cfg.start_ts)
    t_wall = time.monotonic()
    sim = Simulator(cfg.site, start_ts=cfg.start_ts - cfg.warmup_s, service_s=cfg.service_s,
                    maneuver_s=cfg.maneuver_s, coord_precision=cfg.coord_precision, initial=cfg.initial)
    faults = FaultInjector(cfg.faults, cfg.seed, cfg.delays)
    pending: list[Redirect] = []      # received redirects whose `at` is not reached yet
    if isinstance(truth, JsonlWriter):
        truth = PipeSafeWriter(truth)

    def gone_outputs() -> list[str]:
        out = [s.name for s in sinks if getattr(s, "gone", False)]
        if getattr(truth, "gone", False):
            out.append("truth" if str(truth.path) != STDOUT else "stdout (truth)")
        return out

    def emit(ems: list[Emission]) -> None:
        for em in ems:
            for s in sinks:
                s.send(em)
        summary.emissions += len(ems)

    try:
        for _ in range(cfg.warmup_s):
            if stop.is_set():
                break
            sim.step()
            summary.warmup_ticks += 1
        if pacer is None:
            pacer = Pacer(cfg.speed, sleep=stop.wait)
        while not stop.is_set() and (cfg.duration_s is None or summary.ticks < cfg.duration_s):
            if follower is not None:
                got = follower.poll()
                summary.redirects_received += len(got)
                pending.extend(got)
            if pending:
                # A decision made at event time `at` is applied on the next tick after `at`.
                due = [r for r in pending if r.at <= sim.now]
                pending = [r for r in pending if r.at > sim.now]
                for r in due:
                    sim.request_redirect(r.unit_uuid, r.to_station)
            records, events = sim.step()
            now = sim.now
            faults.push(now, records)
            emit(faults.pop_due(now))
            for ev in events:
                if truth is not None:
                    truth.write(ev.to_dict())
                if ev.event == "redirect_applied":
                    summary.redirects_applied += 1
                elif ev.event == "redirect_ignored":
                    summary.redirects_ignored += 1
            summary.truth_events += len(events)
            summary.records += len(records)
            summary.ticks += 1
            summary.end_ts = now
            if pacer.speed > 0:
                for s in sinks:
                    s.tick()
                if truth is not None:
                    truth.flush()
                summary.max_behind_s = max(summary.max_behind_s, pacer.behind_s(summary.ticks))
            if progress is not None and progress_every and summary.ticks % progress_every == 0:
                progress(summary, now)
            closed = gone_outputs()
            if closed:
                summary.output_closed = closed
                break
            pacer.wait_until(summary.ticks)
    except KeyboardInterrupt:
        summary.interrupted = True
    finally:
        summary.interrupted = summary.interrupted or stop.is_set()
        try:
            drained = faults.drain()
            emit(drained)
            summary.drained = len(drained)
        finally:
            summary.redirects_pending = len(pending)
            summary.output_closed = summary.output_closed or gone_outputs()
            summary.faults = faults.stats
            for s in sinks:
                try:
                    summary.sinks[s.name] = s.close()
                except Exception as e:  # noqa: BLE001 - report and keep closing the others
                    summary.close_errors.append(f"{s.name}: {e!r}")
            for name, closer in (("truth", truth), ("follower", follower)):
                if closer is not None:
                    try:
                        closer.close()
                    except Exception as e:  # noqa: BLE001
                        summary.close_errors.append(f"{name}: {e!r}")
            if follower is not None:
                summary.follower = {k: getattr(follower, k) for k in FOLLOWER_COUNTERS if hasattr(follower, k)}
            summary.wall_s = time.monotonic() - t_wall
    return summary
