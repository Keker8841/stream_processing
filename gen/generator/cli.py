"""Command line: `site` (generate a layout), `run` (simulate and publish), `replay` (re-publish a record).

Exit codes: 0 ok, 1 delivery errors / unflushed messages, 2 bad arguments, bad input files or Kafka unreachable,
130 / 143 stopped by SIGINT / SIGTERM (after a clean shutdown).

Pipes: stdout output is pipe-safe. When the reader closes early (e.g. `| head -3`) the run / replay stops cleanly
after the current tick (fault buffer drained, Kafka flushed, summary on stderr), stdout is pointed at /dev/null and
the exit code is 0: a reader that has seen enough is not an error.
"""

from __future__ import annotations

import argparse
import dataclasses
import secrets
import signal
import sys
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaException

from .constants import (DECISION_TOPIC, DEFAULT_COORD_PRECISION, DEFAULT_START_TS, MANEUVER_S, SERVICE_S,
                        TELEMETRY_TOPIC)
from .decisions import DecisionFollower
from .faults import FaultConfig
from .kafka_producer import TelemetryProducer, ensure_topic
from .model import Site
from .pacing import Pacer
from .recording import JsonlWriter, emission_to_line
from .replay import first_emit_at, replay
from .runner import KafkaSink, PipeSafeWriter, RecordSink, RunConfig, iso_utc, run, silence_stdout
from .scenario import load_scenario, parse_ts
from .site import generate_site, load_site, pairwise_distances, save_site, unload_distances

DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_WARMUP_MIN = 60.0
TOPIC_TIMEOUT_S = 10.0
PROGRESS_EVERY_S = 300            # sim seconds between progress lines of paced runs

EXIT_OK, EXIT_DELIVERY, EXIT_USAGE = 0, 1, 2
EXIT_SIGNAL = {signal.SIGINT: 130, signal.SIGTERM: 143}


class CliError(Exception):
    """Clean user-facing error: printed as `error: ...`, exit code 2."""


def _err(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)


def _info(args: argparse.Namespace, msg: str) -> None:
    if not getattr(args, "quiet", False):
        print(msg, file=sys.stderr, flush=True)


def parse_start_ts(text: str) -> int:
    """ISO-8601 with an offset, epoch seconds, or `now` (current wall-clock second)."""
    if text.strip().lower() == "now":
        return int(time.time())
    try:
        return parse_ts(text)
    except ValueError as e:
        raise CliError(f"--start-ts: {e}") from None


def _split(text: str) -> tuple[int, ...]:
    try:
        return tuple(int(x) for x in text.split(","))
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected comma-separated integers, got {text!r}") from None


# ------------------------------------------------------------------------------------------------------- parser

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m generator", description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="command", required=True, metavar="{site,run,replay}")

    s = sub.add_parser("site", help="generate a seeded site layout (YAML)")
    s.add_argument("--seed", type=int, default=42)
    s.add_argument("--out", required=True, help="output YAML path")
    s.add_argument("--force", action="store_true", help="overwrite an existing file")
    s.add_argument("--split", type=_split, default=(10, 10, 10, 10), help="units per station, e.g. 10,10,10,10")
    s.add_argument("--unload-min", type=float, default=3000.0, help="min station distance to unload, m")
    s.add_argument("--unload-max", type=float, default=10000.0, help="max station distance to unload, m")
    s.add_argument("--pit-radius", type=float, default=1500.0, help="stations lie within this radius of the pit center, m")
    s.add_argument("--min-gap", type=float, default=800.0, help="min distance between stations, m")
    s.add_argument("--max-gap", type=float, default=3500.0, help="max distance between stations, m")
    s.add_argument("--quiet", action="store_true")

    r = sub.add_parser("run", help="simulate the site and publish telemetry")
    src = r.add_mutually_exclusive_group(required=True)
    src.add_argument("--site", help="site YAML (steady-state run with warmup)")
    src.add_argument("--scenario", help="scenario YAML (explicit initial states, no warmup)")
    _kafka_args(r)
    r.add_argument("--speed", type=float, default=1.0, help="1 = real time, N = N x faster, 0 = as fast as possible")
    r.add_argument("--start-ts", help="first emitted ts: ISO-8601, epoch seconds or 'now' "
                                      f"(default {iso_utc(DEFAULT_START_TS)} / scenario start)")
    r.add_argument("--warmup-min", type=float, help=f"silent warmup in minutes (site runs, default "
                                                    f"{DEFAULT_WARMUP_MIN:g}; must be 0 with --scenario)")
    r.add_argument("--duration-s", type=int, help="emitted seconds (default: scenario duration / until Ctrl+C)")
    r.add_argument("--seed", type=int, default=1, help="fault injector seed")
    r.add_argument("--coord-precision", type=int, help=f"lat/lon decimals (default {DEFAULT_COORD_PRECISION})")
    r.add_argument("--maneuver-seconds", type=int, help=f"gap between departure and next service (default {MANEUVER_S})")
    r.add_argument("--service-seconds", type=int, help=f"service duration (default {SERVICE_S})")
    r.add_argument("--dup-prob", type=float, help=f"duplicate probability (default {FaultConfig.dup_prob})")
    r.add_argument("--late-prob", type=float, help=f"per-record delay probability (default {FaultConfig.late_prob})")
    r.add_argument("--lag-rate", type=float, help=f"lag episodes per unit-hour (default {FaultConfig.lag_rate_per_hour})")
    r.add_argument("--no-faults", action="store_true", help="no random duplicates, delays or lags (scenario delays still apply)")
    r.add_argument("--shuffle-within-tick", action="store_true", help="shuffle on-time records within a tick")
    r.add_argument("--follow-decisions", action="store_true", help="consume decisions and redirect units")
    r.add_argument("--decision-topic", default=DECISION_TOPIC)
    r.add_argument("--record", help="write every emission to this JSONL file (replayable)")
    r.add_argument("--truth", help="write ground-truth events to this JSONL file ('-' = stdout)")
    r.add_argument("--run-id", help="run id (decision consumer group gen-follow-<run id>); default: random")
    r.add_argument("--quiet", action="store_true", help="no start line, progress or summary on stderr")

    p_rep = sub.add_parser("replay", help="re-publish a recorded emission stream")
    p_rep.add_argument("--record", required=True)
    _kafka_args(p_rep)
    p_rep.add_argument("--speed", type=float, default=0.0, help="0 = as fast as possible (default), 1 = real time")
    p_rep.add_argument("--shift-ts", default="0", help="seconds added to ts/emit_at, or 'auto' (first emission = now)")
    p_rep.add_argument("--quiet", action="store_true")
    return p


def _kafka_args(p: argparse.ArgumentParser) -> None:
    g = p.add_mutually_exclusive_group()
    g.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help=f"Kafka bootstrap servers (default {DEFAULT_BOOTSTRAP})")
    g.add_argument("--no-kafka", action="store_true", help="no Kafka; without --record emissions go to stdout")
    p.add_argument("--topic", default=TELEMETRY_TOPIC)


# --------------------------------------------------------------------------------------------------------- site

def cmd_site(args: argparse.Namespace) -> int:
    out = Path(args.out)
    if out.exists() and not args.force:
        raise CliError(f"{out} exists, use --force to overwrite")
    try:
        site = generate_site(args.seed, split=args.split, unload_dist_m=(args.unload_min, args.unload_max),
                             min_station_gap_m=args.min_gap, max_station_gap_m=args.max_gap,
                             pit_radius_m=args.pit_radius)
    except ValueError as e:
        raise CliError(str(e)) from None
    save_site(site, out)
    if not args.quiet:
        print(site_summary(site, f"wrote {out} (seed {args.seed})"))
    return EXIT_OK


def site_summary(site: Site, title: str) -> str:
    to_unload = unload_distances(site)
    pairs = pairwise_distances(site)
    counts = {sid: sum(1 for v in site.assignment.values() if v == sid) for sid in site.station_ids}
    lines = [f"{title}: unload {site.unload.uuid} at {site.unload.lat}, {site.unload.lon}; "
             f"{len(site.stations)} stations, {len(site.assignment)} units"]
    for s in site.stations:
        lines.append(f"  {s.uuid}  {s.lat:.6f}, {s.lon:.6f}  to unload {to_unload[s.uuid]:7.0f} m  "
                     f"units {counts[s.uuid]}")
    (a1, b1), dmin = min(pairs.items(), key=lambda kv: kv[1])
    (a2, b2), dmax = max(pairs.items(), key=lambda kv: kv[1])
    lines.append(f"  to unload: min {min(to_unload.values()):.0f} m, max {max(to_unload.values()):.0f} m; "
                 f"between stations: min {dmin:.0f} m ({a1}-{b1}), max {dmax:.0f} m ({a2}-{b2})")
    return "\n".join(lines)


# ---------------------------------------------------------------------------------------------------------- run

def build_run_config(args: argparse.Namespace) -> RunConfig:
    """Resolve settings: explicit CLI flag > scenario value > default."""
    scenario = None
    try:
        if args.scenario:
            scenario = load_scenario(args.scenario)
            site = scenario.site
        else:
            site = load_site(args.site)
    except FileNotFoundError as e:
        raise CliError(f"file not found: {e.filename}") from None
    except ValueError as e:
        raise CliError(str(e)) from None

    def pick(flag: Any, scen_attr: str, default: Any) -> Any:
        if flag is not None:
            return flag
        if scenario is not None and getattr(scenario, scen_attr) is not None:
            return getattr(scenario, scen_attr)
        return default

    initial: tuple = ()
    delays: tuple = ()
    if scenario is not None:
        if args.warmup_min:
            raise CliError("--warmup-min must be 0 with --scenario (the scenario defines the initial state)")
        warmup_s = 0
        start_ts = parse_start_ts(args.start_ts) if args.start_ts is not None else scenario.start_ts
        shift = start_ts - scenario.start_ts       # keep FIFO keys of waiting units relative to the new start
        initial = tuple(u if u.radius_enter_ts is None else dataclasses.replace(u, radius_enter_ts=u.radius_enter_ts + shift)
                        for u in scenario.initial)
        delays = tuple(dataclasses.replace(d, from_ts=d.from_ts + shift, to_ts=d.to_ts + shift,
                                           emit_at=d.emit_at + shift) for d in scenario.delays)
    else:
        minutes = DEFAULT_WARMUP_MIN if args.warmup_min is None else args.warmup_min
        if minutes < 0:
            raise CliError(f"--warmup-min must be >= 0, got {minutes:g}")
        warmup_s = round(minutes * 60)
        start_ts = parse_start_ts(args.start_ts) if args.start_ts is not None else DEFAULT_START_TS

    duration_s = pick(args.duration_s, "duration_s", None)
    if duration_s is not None and duration_s < 0:
        raise CliError(f"--duration-s must be >= 0, got {duration_s}")
    if args.no_faults:
        given = [f for f, v in (("--dup-prob", args.dup_prob), ("--late-prob", args.late_prob),
                                ("--lag-rate", args.lag_rate)) if v is not None]
        if given:
            raise CliError(f"--no-faults conflicts with {', '.join(given)}")
        faults = dataclasses.replace(FaultConfig.none(), shuffle_within_tick=args.shuffle_within_tick)
    else:
        kw = {k: v for k, v in (("dup_prob", args.dup_prob), ("late_prob", args.late_prob),
                                ("lag_rate_per_hour", args.lag_rate)) if v is not None}
        try:
            faults = FaultConfig(**kw, shuffle_within_tick=args.shuffle_within_tick)
        except ValueError as e:
            raise CliError(str(e)) from None
    try:
        return RunConfig(
            site=site, start_ts=start_ts, duration_s=duration_s, warmup_s=warmup_s, seed=args.seed,
            speed=args.speed, service_s=pick(args.service_seconds, "service_s", SERVICE_S),
            maneuver_s=pick(args.maneuver_seconds, "maneuver_s", MANEUVER_S),
            coord_precision=pick(args.coord_precision, "coord_precision", DEFAULT_COORD_PRECISION),
            initial=initial, faults=faults, delays=delays,
            run_id=args.run_id or f"{int(time.time())}-{secrets.token_hex(3)}")
    except ValueError as e:
        raise CliError(str(e)) from None


def _ensure_topics(bootstrap: str, topics: Sequence[str]) -> None:
    for topic in topics:
        try:
            # librdkafka would log every failed connect attempt; the exception below says it once
            ensure_topic(bootstrap, topic, timeout_s=TOPIC_TIMEOUT_S, extra_config={"log_level": 0})
        except (KafkaException, TimeoutError) as e:
            raise CliError(f"cannot create/check topic {topic!r} on Kafka {bootstrap}: {e}. "
                           f"Is the broker running? (or use --no-kafka)") from None


class _Signals:
    """SIGINT/SIGTERM set a stop event (main thread only); the run loop then shuts down cleanly."""

    def __init__(self) -> None:
        self.stop = threading.Event()
        self.received: int | None = None
        self._old: dict[int, Any] = {}

    def __enter__(self) -> _Signals:
        if threading.current_thread() is threading.main_thread():
            for sig in EXIT_SIGNAL:
                self._old[sig] = signal.signal(sig, self._handle)
        return self

    def _handle(self, signum: int, frame: Any) -> None:
        if self.received is not None:          # second signal: give up on the clean shutdown
            raise KeyboardInterrupt
        self.received = signum
        self.stop.set()

    def __exit__(self, *exc: object) -> None:
        for sig, old in self._old.items():
            signal.signal(sig, old)


def cmd_run(args: argparse.Namespace) -> int:
    cfg = build_run_config(args)
    if args.follow_decisions and args.no_kafka:
        raise CliError("--follow-decisions needs Kafka, drop --no-kafka")
    if args.record and args.truth and args.record == args.truth:
        raise CliError("--record and --truth must be different files")
    to_stdout = args.no_kafka and not args.record
    if args.truth == "-" and (to_stdout or args.record == "-"):
        raise CliError("emissions already go to stdout, write --truth to a file")

    if not args.no_kafka:
        _ensure_topics(args.bootstrap, [args.topic] + ([args.decision_topic] if args.follow_decisions else []))

    sinks: list = []
    truth = follower = None
    try:
        if not args.no_kafka:
            sinks.append(KafkaSink(TelemetryProducer(args.bootstrap, args.topic), name=f"kafka:{args.topic}"))
        if args.record or to_stdout:
            sinks.append(RecordSink(args.record or "-"))
        if args.truth:
            truth = PipeSafeWriter(JsonlWriter(args.truth))
        if args.follow_decisions:
            follower = DecisionFollower(args.bootstrap, topic=args.decision_topic, run_id=cfg.run_id,
                                        min_at=cfg.start_ts)
    except (OSError, KafkaException) as e:
        for s in sinks:
            s.close()
        if truth is not None:
            truth.close()
        raise CliError(f"cannot open outputs: {e}") from None

    mode = "as fast as possible" if cfg.speed <= 0 else f"speed {cfg.speed:g}x"
    _info(args, f"run {cfg.run_id}: {len(cfg.site.assignment)} units, {len(cfg.site.stations)} stations, "
                f"start {iso_utc(cfg.start_ts)}, warmup {cfg.warmup_s} s, "
                f"duration {'until stopped' if cfg.duration_s is None else f'{cfg.duration_s} s'}, {mode}, "
                f"faults {'on' if cfg.faults.active else 'off'} (seed {cfg.seed}), "
                f"outputs: {', '.join(s.name for s in sinks) or 'none'}"
                f"{', truth:' + args.truth if args.truth else ''}"
                f"{', following ' + args.decision_topic if follower else ''}")

    def progress(summary: Any, now: int) -> None:
        _info(args, f"  {iso_utc(now)}: {summary.ticks} ticks, {summary.emissions} emissions, "
                    f"redirects applied {summary.redirects_applied}")

    with _Signals() as sig:
        summary = run(cfg, sinks=sinks, truth=truth, follower=follower, stop=sig.stop,
                      progress=progress if cfg.speed > 0 else None, progress_every=PROGRESS_EVERY_S)
    _info(args, summary.format())
    if sig.received is not None:
        return EXIT_SIGNAL[sig.received]
    return EXIT_OK if summary.ok else EXIT_DELIVERY     # a closed reader (summary.output_closed) is not an error


# ------------------------------------------------------------------------------------------------------- replay

class _StopReplay(Exception):
    pass


def cmd_replay(args: argparse.Namespace) -> int:
    path = Path(args.record)
    if not path.is_file():
        raise CliError(f"record not found: {path}")
    shift_text = args.shift_ts.strip().lower()
    try:
        if shift_text == "auto":
            first = first_emit_at(path)
            shift = 0 if first is None else int(time.time()) - first
        else:
            shift = int(shift_text)
    except ValueError as e:
        raise CliError(f"--shift-ts: expected an integer or 'auto' ({e})") from None

    producer = writer = None
    if args.no_kafka:
        writer = PipeSafeWriter(JsonlWriter("-", line_buffered=args.speed > 0))
        send = lambda em: writer.write(emission_to_line(em))  # noqa: E731
    else:
        _ensure_topics(args.bootstrap, [args.topic])
        producer = TelemetryProducer(args.bootstrap, args.topic)
        send = producer.send
    target = "stdout" if args.no_kafka else f"kafka:{args.topic}@{args.bootstrap}"
    pace = "as fast as possible" if args.speed <= 0 else f"speed {args.speed:g}x"
    _info(args, f"replay {path} -> {target}, shift {shift:+d} s, {pace}")

    sent = 0
    interrupted = False
    failure: str | None = None
    t0 = time.monotonic()
    with _Signals() as sig:
        def guarded(em: Any) -> None:
            nonlocal sent
            if sig.stop.is_set() or (writer is not None and writer.gone):
                raise _StopReplay
            send(em)
            if writer is not None and writer.gone:    # this line did not reach the reader
                raise _StopReplay
            sent += 1

        try:
            replay(path, guarded, speed=args.speed, shift_ts=shift,
                   pacer_factory=lambda speed: Pacer(speed, sleep=sig.stop.wait))
        except (_StopReplay, KeyboardInterrupt):
            interrupted = not (writer is not None and writer.gone)
        except ValueError as e:              # malformed record line: report after flushing what was sent
            failure = str(e)
        finally:
            unflushed = 0
            stats = ""
            if writer is not None:
                writer.close()
            if producer is not None:
                unflushed = producer.flush(30)
                stats = (f", delivered {producer.delivered}, errors {producer.errors}, unflushed {unflushed}"
                         + (f", last error: {producer.last_error}" if producer.last_error else ""))
    reader_closed = writer is not None and writer.gone
    _info(args, f"replayed {sent} emissions in {time.monotonic() - t0:.1f} s"
                f"{' INTERRUPTED' if interrupted else ''}{' STOPPED: reader closed stdout' if reader_closed else ''}"
                f"{stats}")
    if failure is not None:
        raise CliError(failure)
    if sig.received is not None:
        return EXIT_SIGNAL[sig.received]
    if producer is not None and (producer.errors or unflushed):
        return EXIT_DELIVERY
    return EXIT_OK


# --------------------------------------------------------------------------------------------------------- main

COMMANDS = {"site": cmd_site, "run": cmd_run, "replay": cmd_replay}


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        code = COMMANDS[args.command](args)
        sys.stdout.flush()                     # surface a broken pipe here, not at interpreter exit
        return code
    except CliError as e:
        _err(str(e))
        return EXIT_USAGE
    except KeyboardInterrupt:                  # repeated Ctrl+C during shutdown
        return EXIT_SIGNAL[signal.SIGINT]
    except BrokenPipeError:
        # stdout reader went away (e.g. `| head`) outside the guarded writers: silence the flush at exit
        silence_stdout()
        return EXIT_OK
