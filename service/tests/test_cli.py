"""CLI: parsing, config overrides, error exits, `process` and `run` (fake broker, stub engine)."""

import json
import os
import signal
import subprocess
import sys
from functools import partial
from pathlib import Path

import pytest

import vqservice.cli as cli
from fakes import FakeBroker, StubFactory, kerr
from vqservice.config import load_config
from vqservice.kafka_app import ServiceApp

SERVICE = Path(__file__).resolve().parents[1]
REPO_SITE = SERVICE.parent / "site.yaml"
TESTS = Path(__file__).resolve().parent


def write_cfg(tmp_path: Path, extra: str = "", site: str | None = None) -> Path:
    path = tmp_path / "service.yaml"
    path.write_text(f"site_file: {site or REPO_SITE}\n{extra}")
    return path


def write_record(path: Path, n: int) -> Path:
    path.write_text("".join(json.dumps({"emit_at": ts, "key": "T-01", "value": {"unit_uuid": "T-01", "ts": ts}})
                            + "\n" for ts in range(1, n + 1)))
    return path


@pytest.fixture
def stub_engine(monkeypatch):
    factory = StubFactory()
    monkeypatch.setattr(cli, "make_engine_factory", lambda site, cfg: factory)
    return factory


@pytest.fixture
def broker(monkeypatch):
    b = FakeBroker()
    monkeypatch.setattr(cli, "ServiceApp", partial(ServiceApp, **b.factories()))
    return b


# ------------------------------------------------------------------------------------------------- parsing

def test_parser_defaults():
    a = cli.build_parser().parse_args(["run"])
    assert (a.config, a.shadow, a.metric, a.bootstrap, a.quiet) == ("service.yaml", False, None, None, False)
    a = cli.build_parser().parse_args(["process", "--record", "r.jsonl"])
    assert (a.record, a.out) == ("r.jsonl", "-")


@pytest.mark.parametrize("argv", [[], ["process"], ["run", "--metric", "fastest"], ["process", "--bootstrap", "x",
                                                                                     "--record", "r"]])
def test_bad_arguments_exit_2(argv, capsys):
    with pytest.raises(SystemExit) as ei:
        cli.main(argv)
    assert ei.value.code == 2


def test_overrides_via_dataclasses_replace(tmp_path):
    cfg = load_config(write_cfg(tmp_path))
    args = cli.build_parser().parse_args(["run", "--shadow", "--metric", "service_end", "--bootstrap", "h:1"])
    new = cli.apply_overrides(cfg, args)
    assert new.engine.apply_decisions is False and new.engine.metric == "service_end"
    assert new.kafka.bootstrap == "h:1"
    assert new.engine.horizon_s == cfg.engine.horizon_s and new.kafka.group_id == cfg.kafka.group_id
    assert cli.apply_overrides(cfg, cli.build_parser().parse_args(["run"])) == cfg


def test_repo_service_yaml():
    cfg = load_config(SERVICE / "service.yaml")
    assert cfg.site_file == REPO_SITE.resolve() and cfg.site_file.is_file()
    args = cli.build_parser().parse_args(["run", "--config", str(SERVICE / "service.yaml")])
    cfg, site = cli.load(args)
    assert len(site.stations) == 4 and len(site.assignment) == 40
    assert (cfg.kafka.commit_interval_ms, cfg.kafka.commit_max_messages) == (100, 500)


# ------------------------------------------------------------------------------------------- config errors

@pytest.mark.parametrize("cmd", ["run", "process"])
def test_missing_config(tmp_path, capsys, cmd):
    argv = [cmd, "--config", str(tmp_path / "nope.yaml")] + (["--record", "r"] if cmd == "process" else [])
    assert cli.main(argv) == 2
    assert capsys.readouterr().err == f"error: config not found: {tmp_path / 'nope.yaml'}\n"


@pytest.mark.parametrize("cmd", ["run", "process"])
def test_missing_site(tmp_path, capsys, cmd):
    cfg = write_cfg(tmp_path, site="missing-site.yaml")
    argv = [cmd, "--config", str(cfg)] + (["--record", "r"] if cmd == "process" else [])
    assert cli.main(argv) == 2
    err = capsys.readouterr().err
    assert err.startswith("error: site file not found:") and "missing-site.yaml" in err


@pytest.mark.parametrize("text,match", [
    ("engine: {speed: 3}\n", "engine: unknown key(s) speed"),
    ("kafka: [1, 2]\n", "kafka must be a mapping"),
    ("engine: {metric: fastest}\n", "metric must be one of"),
    ("engine: {horizon_s: [unclosed\n", "service.yaml"),
])
def test_bad_config(tmp_path, capsys, text, match):
    cfg = write_cfg(tmp_path, extra=text)
    assert cli.main(["run", "--config", str(cfg)]) == 2
    err = capsys.readouterr().err
    assert err.startswith("error: bad config") and match in err


def test_bad_site(tmp_path, capsys):
    site = tmp_path / "site.yaml"
    site.write_text("stations: []\n")
    assert cli.main(["run", "--config", str(write_cfg(tmp_path, site=str(site)))]) == 2
    assert capsys.readouterr().err.startswith(f"error: bad site file {site}")


# ------------------------------------------------------------------------------------------------- process

def test_process_to_file(tmp_path, capsys, stub_engine):
    rec = write_record(tmp_path / "rec.jsonl", 3)
    out = tmp_path / "out.jsonl"
    argv = ["process", "--config", str(write_cfg(tmp_path)), "--record", str(rec), "--out", str(out), "--shadow"]
    assert cli.main(argv) == 0
    lines = [json.loads(x) for x in out.read_text().splitlines()]
    assert [x["value"]["n"] for x in lines] == [1, 2, 3] and lines[0]["topic"] == "queue.v1"
    err = capsys.readouterr().err
    assert "processed 3 record lines" in err and "3 outputs (queue 3, decision 0)" in err
    assert "shadow mode" in err and "accepted=3" in err


def test_process_to_stdout_quiet(tmp_path, capsys, stub_engine):
    rec = write_record(tmp_path / "rec.jsonl", 2)
    assert cli.main(["process", "--config", str(write_cfg(tmp_path)), "--record", str(rec), "--quiet"]) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert [json.loads(x)["value"]["n"] for x in captured.out.splitlines()] == [1, 2]


def test_process_missing_record(tmp_path, capsys):
    assert cli.main(["process", "--config", str(write_cfg(tmp_path)), "--record", str(tmp_path / "x.jsonl")]) == 2
    assert capsys.readouterr().err.startswith("error: record not found:")


def test_process_malformed_record_keeps_earlier_outputs(tmp_path, capsys, stub_engine):
    rec = write_record(tmp_path / "rec.jsonl", 2)
    rec.write_text(rec.read_text() + "garbage\n")
    out = tmp_path / "out.jsonl"
    argv = ["process", "--config", str(write_cfg(tmp_path)), "--record", str(rec), "--out", str(out)]
    assert cli.main(argv) == 2
    assert "rec.jsonl:3: malformed record line" in capsys.readouterr().err
    assert len(out.read_text().splitlines()) == 2


# ------------------------------------------------------------------------------------------------------ run

def _sigint_on_idle(broker: FakeBroker) -> None:
    broker.on_idle = lambda consumer: os.kill(os.getpid(), signal.SIGINT)


def test_run_until_sigint(tmp_path, capsys, broker, stub_engine):
    for ts in (1, 2, 3):
        broker.append("telemetry.v1", {"unit_uuid": "T-01", "ts": ts}, key="T-01")
    _sigint_on_idle(broker)
    assert cli.main(["run", "--config", str(write_cfg(tmp_path)), "--bootstrap", "fake:1"]) == 0
    assert [v["n"] for v in broker.values("queue.v1")] == [1, 2, 3]
    assert broker.producers[0].conf["bootstrap.servers"] == "fake:1"
    err = capsys.readouterr().err
    assert "vq-service: telemetry.v1 -> queue.v1, decision.v1 on fake:1" in err
    assert "stopped: consumed 3" in err and "commits 1" in err


def test_run_quiet(tmp_path, capsys, broker, stub_engine):
    _sigint_on_idle(broker)
    assert cli.main(["run", "--config", str(write_cfg(tmp_path)), "--quiet"]) == 0
    assert capsys.readouterr().err == ""


def test_run_broker_unreachable(tmp_path, capsys, broker, stub_engine):
    broker.down = True
    assert cli.main(["run", "--config", str(write_cfg(tmp_path))]) == 2
    err = capsys.readouterr().err
    assert "error: cannot create/check topic" in err and "Is the broker running?" in err


def test_run_fatal_kafka_error(tmp_path, capsys, broker, stub_engine):
    from confluent_kafka import KafkaError
    broker.append("telemetry.v1", {"unit_uuid": "T-01", "ts": 1}, key="T-01")
    broker.fail("commit_transaction", kerr(KafkaError._FENCED, fatal=True))
    assert cli.main(["run", "--config", str(write_cfg(tmp_path))]) == 1
    assert "error: fatal:" in capsys.readouterr().err


# -------------------------------------------------------------------------------------------- subprocesses

# Runs the CLI in a child process with the stub engine (and, for `run`, a fake broker that idles forever).
CHILD = """
import sys, time
sys.path[:0] = [{service!r}, {tests!r}]
from functools import partial
import fakes, vqservice.cli as cli
from vqservice.kafka_app import ServiceApp
cli.make_engine_factory = lambda site, cfg: fakes.StubFactory()
broker = fakes.FakeBroker()
broker.on_idle = lambda c: (print("idle", file=sys.stderr, flush=True), time.sleep(0.01))
cli.ServiceApp = partial(ServiceApp, **broker.factories())
raise SystemExit(cli.main(sys.argv[1:]))
"""


@pytest.fixture
def child(tmp_path):
    script = tmp_path / "child.py"
    script.write_text(CHILD.format(service=str(SERVICE), tests=str(TESTS)))
    return lambda *argv: [sys.executable, str(script), *argv]


def test_process_stdout_into_head_is_silent(tmp_path, child):
    rec = write_record(tmp_path / "rec.jsonl", 20_000)        # far more output than a pipe buffer holds
    cmd = " ".join(child("process", "--config", str(write_cfg(tmp_path)), "--record", str(rec), "--quiet"))
    proc = subprocess.run(["bash", "-c", f"set -o pipefail; {cmd} | head -2"], cwd=SERVICE,
                          capture_output=True, text=True, timeout=60)
    assert (proc.returncode, proc.stderr) == (0, ""), proc.stderr
    assert len(proc.stdout.splitlines()) == 2


def test_process_reader_closes_early_reports_stop(tmp_path, child):
    rec = write_record(tmp_path / "rec.jsonl", 20_000)
    proc = subprocess.Popen(child("process", "--config", str(write_cfg(tmp_path)), "--record", str(rec)),
                            cwd=SERVICE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    proc.stdout.readline()
    proc.stdout.close()
    _, err = proc.communicate(timeout=60)
    assert proc.returncode == 0, err
    assert "STOPPED: reader closed stdout" in err and "Traceback" not in err and "Exception ignored" not in err


def test_run_sigterm_stops_cleanly(tmp_path, child):
    proc = subprocess.Popen(child("run", "--config", str(write_cfg(tmp_path))), cwd=SERVICE,
                            stderr=subprocess.PIPE, text=True)
    for line in proc.stderr:                  # wait until the service loop is polling
        if line.strip() == "idle":
            break
    proc.terminate()
    _, err = proc.communicate(timeout=30)
    assert proc.returncode == 0, err
    assert "stopped: consumed 0" in err and "Traceback" not in err
