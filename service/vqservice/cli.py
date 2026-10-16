"""Command line: `run` (Kafka service, exactly-once) and `process` (offline: generator record -> outputs JSONL).

Exit codes: 0 ok (also `run` stopped cleanly by SIGINT/SIGTERM, and `process --out -` whose reader went away),
1 fatal Kafka error while running, 2 bad arguments / config / site / record or Kafka unreachable at start,
130 interrupted by a repeated Ctrl+C (no clean shutdown).
"""

from __future__ import annotations

import argparse
import dataclasses
import signal
import sys
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import yaml
from confluent_kafka import KafkaException

from .config import ServiceConfig, load_config
from .kafka_app import STATS_INTERVAL_S, ServiceApp, StartupError, make_engine_factory
from .offline import JsonlSink, process_record, silence_stdout
from .site import Site, load_site

EXIT_OK, EXIT_FATAL, EXIT_USAGE, EXIT_INTERRUPTED = 0, 1, 2, 130
METRICS = ("queue_wait", "service_end")


class CliError(Exception):
    """Clean user-facing error: printed as `error: ...`, exit code 2."""


def _err(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr, flush=True)


def _logger(quiet: bool):
    if quiet:
        return lambda msg: None
    return lambda msg: print(f"{time.strftime('%H:%M:%S')} {msg}", file=sys.stderr, flush=True)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m vqservice", description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="command", required=True, metavar="{run,process}")
    r = sub.add_parser("run", help="Kafka service: telemetry -> queue.v1 / decision.v1")
    _common(r)
    r.add_argument("--bootstrap", help="Kafka bootstrap servers (overrides kafka.bootstrap)")
    pr = sub.add_parser("process", help="offline: feed a generator record, write outputs as JSONL")
    _common(pr)
    pr.add_argument("--record", required=True, help="generator record (JSONL {emit_at, key, value})")
    pr.add_argument("--out", default="-", help="output JSONL file, '-' = stdout (default)")
    return p


def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--config", default="service.yaml", help="service config YAML (default service.yaml)")
    p.add_argument("--shadow", action="store_true",
                   help="shadow mode: decisions are published but not applied to the queues")
    p.add_argument("--metric", choices=METRICS, help="gain metric (overrides engine.metric)")
    p.add_argument("--quiet", action="store_true", help="no info / stats lines on stderr")


def apply_overrides(cfg: ServiceConfig, args: argparse.Namespace) -> ServiceConfig:
    eng: dict[str, Any] = {}
    if args.shadow:
        eng["apply_decisions"] = False
    if args.metric:
        eng["metric"] = args.metric
    kafka: dict[str, Any] = {}
    if getattr(args, "bootstrap", None):
        kafka["bootstrap"] = args.bootstrap
    try:
        return dataclasses.replace(cfg, engine=dataclasses.replace(cfg.engine, **eng),
                                   kafka=dataclasses.replace(cfg.kafka, **kafka))
    except ValueError as e:
        raise CliError(str(e)) from None


def _with_path(path: Path, e: Exception) -> str:
    msg = str(e)
    return msg if str(path) in msg else f"{path}: {msg}"


def load(args: argparse.Namespace) -> tuple[ServiceConfig, Site]:
    path = Path(args.config)
    if not path.is_file():
        raise CliError(f"config not found: {path}")
    try:
        cfg = load_config(path)
    except (ValueError, TypeError, KeyError, yaml.YAMLError) as e:
        raise CliError(f"bad config {_with_path(path, e)}") from None
    cfg = apply_overrides(cfg, args)
    try:
        site = load_site(cfg.site_file)
    except FileNotFoundError:
        raise CliError(f"site file not found: {cfg.site_file} (site_file in {path})") from None
    except (ValueError, TypeError, KeyError, yaml.YAMLError) as e:
        raise CliError(f"bad site file {_with_path(cfg.site_file, e)}") from None
    return cfg, site


def _describe(cfg: ServiceConfig) -> str:
    e = cfg.engine
    return f"metric {e.metric}, {'decisions applied' if e.apply_decisions else 'shadow mode (decisions not applied)'}"


# -------------------------------------------------------------------------------------------------------- run

class _Signals:
    """SIGINT/SIGTERM set a stop event (main thread only); a second signal aborts the clean shutdown."""

    def __init__(self) -> None:
        self.stop = threading.Event()
        self.received: int | None = None
        self._old: dict[int, Any] = {}

    def __enter__(self) -> _Signals:
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGINT, signal.SIGTERM):
                self._old[sig] = signal.signal(sig, self._handle)
        return self

    def _handle(self, signum: int, frame: Any) -> None:
        if self.received is not None:
            raise KeyboardInterrupt
        self.received = signum
        self.stop.set()

    def __exit__(self, *exc: object) -> None:
        for sig, old in self._old.items():
            signal.signal(sig, old)


def cmd_run(args: argparse.Namespace, **app_kw: Any) -> int:
    """`app_kw` (factories, clock) is for tests."""
    cfg, site = load(args)
    log = _logger(args.quiet)
    log(f"site {cfg.site_file}: {len(site.stations)} stations, {len(site.assignment)} units; {_describe(cfg)}")
    app = ServiceApp(cfg, engine_factory=make_engine_factory(site, cfg.engine), log=log,
                     stats_interval_s=None if args.quiet else STATS_INTERVAL_S, **app_kw)
    with _Signals() as sig:
        try:
            app.run(sig.stop)
        except StartupError as e:
            raise CliError(f"{e}. Is the broker running?") from None
        except (KafkaException, TimeoutError) as e:
            _err(f"fatal: {e}")
            return EXIT_FATAL
    return EXIT_OK


# ---------------------------------------------------------------------------------------------------- process

def cmd_process(args: argparse.Namespace) -> int:
    cfg, site = load(args)
    record = Path(args.record)
    if not record.is_file():
        raise CliError(f"record not found: {record}")
    log = _logger(args.quiet)
    engine = make_engine_factory(site, cfg.engine)(None)
    try:
        sink = JsonlSink(args.out)
    except OSError as e:
        raise CliError(f"cannot open --out: {e}") from None
    t0 = time.monotonic()
    try:
        stats = process_record(record, engine, sink,
                               topics={"queue": cfg.kafka.queue_topic, "decision": cfg.kafka.decision_topic})
    except ValueError as e:                     # malformed record line: outputs so far are kept
        raise CliError(str(e)) from None
    finally:
        sink.close()
    log(f"processed {stats['lines']} record lines in {time.monotonic() - t0:.1f} s ({_describe(cfg)}) -> "
        f"{stats['outputs']} outputs (queue {stats['queue']}, decision {stats['decision']})"
        f"{' STOPPED: reader closed stdout' if stats['stopped'] else ''}")
    log("engine: " + " ".join(f"{k}={v}" for k, v in stats["engine"].items()))
    return EXIT_OK


# ------------------------------------------------------------------------------------------------------- main

COMMANDS = {"run": cmd_run, "process": cmd_process}


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        code = COMMANDS[args.command](args)
        sys.stdout.flush()                     # surface a broken pipe here, not at interpreter exit
        return code
    except CliError as e:
        _err(str(e))
        return EXIT_USAGE
    except KeyboardInterrupt:
        return EXIT_INTERRUPTED
    except BrokenPipeError:                    # stdout reader went away outside the guarded sink
        silence_stdout()
        return EXIT_OK
