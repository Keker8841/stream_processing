import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from confluent_kafka import KafkaError, KafkaException

import generator.cli as cli
from generator.cli import build_parser, build_run_config, main
from generator.constants import DEFAULT_START_TS
from generator.faults import FaultConfig
from generator.recording import read_record
from generator.site import generate_site, load_site

ROOT = Path(__file__).resolve().parent.parent
SITE = str(ROOT.parent / "site.yaml")         # repo-level site, shared with the service
SCENARIO = str(ROOT / "scenarios" / "task_example.yaml")
T_1159 = DEFAULT_START_TS - 60


def cfg(*argv):
    return build_run_config(build_parser().parse_args(["run", *argv]))


def lines(text: str) -> list[dict]:
    return [json.loads(x) for x in text.splitlines() if x.strip()]


# ------------------------------------------------------------------------------------------------- parsing

def test_run_requires_exactly_one_source(capsys):
    with pytest.raises(SystemExit) as e:
        build_parser().parse_args(["run"])
    assert e.value.code == 2
    with pytest.raises(SystemExit):
        build_parser().parse_args(["run", "--site", SITE, "--scenario", SCENARIO])
    with pytest.raises(SystemExit):
        build_parser().parse_args(["run", "--site", SITE, "--no-kafka", "--bootstrap", "x:1"])


def test_site_run_defaults():
    c = cfg("--site", SITE)
    assert (c.start_ts, c.warmup_s, c.duration_s, c.seed, c.speed) == (DEFAULT_START_TS, 3600, None, 1, 1.0)
    assert (c.service_s, c.maneuver_s, c.coord_precision, c.initial) == (200, 0, 4, ())
    assert c.faults == FaultConfig()
    assert c.site == load_site(SITE)
    assert c.run_id and c.run_id != cfg("--site", SITE).run_id     # fresh run id per run


def test_site_run_flags():
    c = cfg("--site", SITE, "--start-ts", "2026-09-15T15:00:00+03:00", "--warmup-min", "1.5", "--duration-s", "60",
            "--seed", "9", "--speed", "0", "--service-seconds", "180", "--maneuver-seconds", "30",
            "--coord-precision", "6", "--dup-prob", "0.1", "--late-prob", "0", "--lag-rate", "0.5",
            "--shuffle-within-tick", "--run-id", "abc")
    assert (c.start_ts, c.warmup_s, c.duration_s, c.seed, c.speed) == (DEFAULT_START_TS, 90, 60, 9, 0.0)
    assert (c.service_s, c.maneuver_s, c.coord_precision, c.run_id) == (180, 30, 6, "abc")
    assert c.faults == FaultConfig(dup_prob=0.1, late_prob=0.0, lag_rate_per_hour=0.5, shuffle_within_tick=True)


@pytest.mark.parametrize("text, expected", [
    ("2026-09-15T12:00:00Z", DEFAULT_START_TS), ("1789473600", DEFAULT_START_TS), ("now", 1_800_000_000),
    ("NOW", 1_800_000_000)])
def test_start_ts_forms(monkeypatch, text, expected):
    monkeypatch.setattr(cli.time, "time", lambda: 1_800_000_000.7)
    assert cfg("--site", SITE, "--start-ts", text).start_ts == expected


def test_scenario_values_and_precedence():
    c = cfg("--scenario", SCENARIO)
    assert (c.start_ts, c.warmup_s, c.duration_s) == (T_1159, 0, 960)
    assert (c.service_s, c.maneuver_s, c.coord_precision) == (200, 30, 6)
    assert len(c.initial) == 4
    c = cfg("--scenario", SCENARIO, "--duration-s", "100", "--maneuver-seconds", "0", "--service-seconds", "150",
            "--coord-precision", "5", "--start-ts", "1789000000", "--warmup-min", "0")
    assert (c.start_ts, c.duration_s, c.service_s, c.maneuver_s, c.coord_precision) == (1789000000, 100, 150, 0, 5)


def test_scenario_start_override_shifts_fifo_keys(tmp_path):
    doc = yaml.safe_load(Path(SCENARIO).read_text())
    doc["units"].append({"unit_uuid": "T5", "phase": "waiting", "radius_enter_ts": "2026-09-15T11:58:30Z"})
    doc["site"]["assignment"]["T5"] = "S2"
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(doc))
    (t5,) = [u for u in cfg("--scenario", str(path), "--start-ts", str(T_1159 + 1000)).initial if u.unit_uuid == "T5"]
    assert t5.radius_enter_ts == T_1159 - 30 + 1000


@pytest.mark.parametrize("argv, message", [
    (["--scenario", SCENARIO, "--warmup-min", "5"], "--warmup-min must be 0"),
    (["--site", SITE, "--warmup-min", "-1"], "--warmup-min must be >= 0"),
    (["--site", SITE, "--start-ts", "yesterday"], "--start-ts"),
    (["--site", SITE, "--start-ts", "2026-09-15T12:00:00"], "no timezone"),
    (["--site", SITE, "--no-faults", "--dup-prob", "0.1"], "--no-faults conflicts with --dup-prob"),
    (["--site", SITE, "--dup-prob", "2"], "dup_prob"),
    (["--site", SITE, "--duration-s", "-5"], "--duration-s"),
    (["--site", SITE, "--coord-precision", "20"], "coord_precision"),
    (["--site", "/nonexistent/site.yaml"], "file not found"),
    (["--scenario", "/nonexistent/s.yaml"], "file not found"),
])
def test_run_config_errors(argv, message):
    with pytest.raises(cli.CliError, match=message.replace("-", r"\-")):
        cfg(*argv)


def test_no_faults_with_shuffle():
    c = cfg("--site", SITE, "--no-faults", "--shuffle-within-tick")
    assert c.faults == FaultConfig(dup_prob=0, late_prob=0, lag_rate_per_hour=0, shuffle_within_tick=True)
    assert cfg("--site", SITE, "--no-faults").faults == FaultConfig.none()


# --------------------------------------------------------------------------------------------------------- site

def test_site_command(tmp_path, capsys):
    out = tmp_path / "site.yaml"
    assert main(["site", "--seed", "42", "--out", str(out)]) == 0
    assert load_site(out) == generate_site(42)
    text = capsys.readouterr().out
    assert "S1" in text and "to unload" in text and "between stations: min" in text
    before = out.read_text()
    assert main(["site", "--seed", "43", "--out", str(out)]) == 2
    assert "exists, use --force" in capsys.readouterr().err and out.read_text() == before
    assert main(["site", "--seed", "43", "--out", str(out), "--force", "--quiet"]) == 0
    assert capsys.readouterr().out == "" and load_site(out) == generate_site(43)


def test_site_command_params_and_errors(tmp_path, capsys):
    out = tmp_path / "s.yaml"
    assert main(["site", "--out", str(out), "--split", "5,5,5,5,5", "--unload-min", "4000", "--unload-max", "6000",
                 "--quiet"]) == 0
    assert load_site(out).station_ids == ["S1", "S2", "S3", "S4", "S5"]
    assert main(["site", "--out", str(tmp_path / "x.yaml"), "--unload-max", "1000"]) == 2
    assert "error:" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        main(["site", "--out", str(tmp_path / "y.yaml"), "--split", "a,b"])


# ---------------------------------------------------------------------------------------------------------- run

def test_run_no_kafka_to_record(tmp_path, capsys):
    rec, truth = tmp_path / "rec.jsonl", tmp_path / "truth.jsonl"
    code = main(["run", "--site", SITE, "--no-kafka", "--speed", "0", "--duration-s", "120", "--warmup-min", "10",
                 "--record", str(rec), "--truth", str(truth)])
    assert code == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "120 ticks" in captured.err and "record:" in captured.err
    ems = list(read_record(rec))
    assert len(ems) >= 120 * 40
    assert {em.value["ts"] for em in ems} == set(range(DEFAULT_START_TS, DEFAULT_START_TS + 120))
    assert all(json.loads(x)["ts"] >= DEFAULT_START_TS for x in truth.read_text().splitlines())


def test_run_no_kafka_stdout(capsys):
    assert main(["run", "--scenario", SCENARIO, "--no-kafka", "--speed", "0", "--duration-s", "3", "--no-faults",
                 "--quiet"]) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    out = lines(captured.out)
    assert len(out) == 12 and out[0] == {"emit_at": T_1159, "key": "T1", "value": out[0]["value"]}
    assert out[0]["value"]["speed_kmh"] == 0.0 and out[1]["value"]["speed_kmh"] == 36.0


def test_run_truth_to_stdout(tmp_path, capsys):
    rec = tmp_path / "r.jsonl"
    assert main(["run", "--scenario", SCENARIO, "--no-kafka", "--speed", "0", "--no-faults", "--quiet",
                 "--record", str(rec), "--truth", "-"]) == 0
    events = lines(capsys.readouterr().out)
    assert {"ts": T_1159 + 200, "unit_uuid": "T1", "event": "depart", "station_uuid": "S1"} in events
    assert {"ts": DEFAULT_START_TS + 170, "unit_uuid": "T4", "event": "service_start", "station_uuid": "S1"} in events
    assert {"ts": DEFAULT_START_TS + 400, "unit_uuid": "T2", "event": "service_start", "station_uuid": "S1"} in events


@pytest.mark.parametrize("argv, message", [
    (["--follow-decisions", "--no-kafka"], "--follow-decisions needs Kafka"),
    (["--no-kafka", "--truth", "-"], "emissions already go to stdout"),
    (["--no-kafka", "--record", "x.jsonl", "--truth", "x.jsonl"], "different files"),
])
def test_run_output_errors(capsys, argv, message):
    assert main(["run", "--site", SITE, "--duration-s", "1", *argv]) == 2
    assert message in capsys.readouterr().err


def test_run_kafka_unreachable(monkeypatch, capsys):
    def fail(*a, **kw):
        raise KafkaException(KafkaError(KafkaError._TIMED_OUT, "Failed while waiting for controller"))
    monkeypatch.setattr(cli, "ensure_topic", fail)
    assert main(["run", "--site", SITE, "--bootstrap", "localhost:1", "--duration-s", "1"]) == 2
    err = capsys.readouterr().err
    assert "error: cannot create/check topic 'telemetry.v1' on Kafka localhost:1" in err
    assert "Is the broker running?" in err


def test_run_with_fake_kafka(monkeypatch, capsys):
    sent = []

    class FakeProducer:
        def __init__(self, bootstrap, topic):
            self.topic, self.sent, self.delivered, self.errors, self.last_error = topic, 0, 0, 0, None

        def send(self, em):
            sent.append(em)
            self.sent += 1
            self.delivered += 1

        def poll(self, timeout=0):
            pass

        def flush(self, timeout=30):
            return 0
    topics = []
    monkeypatch.setattr(cli, "ensure_topic", lambda bs, topic, **kw: topics.append((bs, topic)) or True)
    monkeypatch.setattr(cli, "TelemetryProducer", FakeProducer)
    assert main(["run", "--scenario", SCENARIO, "--bootstrap", "b:9", "--topic", "t.x", "--speed", "0",
                 "--duration-s", "5"]) == 0
    assert topics == [("b:9", "t.x")] and len(sent) >= 20
    assert "kafka:t.x: sent=" in capsys.readouterr().err


# ------------------------------------------------------------------------------------------------------- replay

@pytest.fixture
def record(tmp_path):
    rec = tmp_path / "rec.jsonl"
    assert main(["run", "--site", SITE, "--no-kafka", "--speed", "0", "--duration-s", "30", "--warmup-min", "0",
                 "--record", str(rec), "--quiet", "--dup-prob", "0.2"]) == 0
    return rec


def test_replay_no_kafka(record, capsys):
    assert main(["replay", "--record", str(record), "--no-kafka", "--quiet"]) == 0
    assert capsys.readouterr().out == record.read_text()
    assert main(["replay", "--record", str(record), "--no-kafka", "--shift-ts", "100"]) == 0
    captured = capsys.readouterr()
    shifted, orig = lines(captured.out), lines(record.read_text())
    assert [(x["emit_at"] - 100, x["key"], x["value"]["ts"] - 100) for x in shifted] == \
        [(x["emit_at"], x["key"], x["value"]["ts"]) for x in orig]
    assert f"replayed {len(orig)} emissions" in captured.err


def test_replay_shift_auto(record, monkeypatch, capsys):
    monkeypatch.setattr(cli.time, "time", lambda: DEFAULT_START_TS + 5000.5)
    assert main(["replay", "--record", str(record), "--no-kafka", "--shift-ts", "auto", "--quiet"]) == 0
    first = lines(capsys.readouterr().out)[0]
    assert first["emit_at"] == DEFAULT_START_TS + 5000


def test_replay_errors(record, capsys):
    assert main(["replay", "--record", str(record), "--no-kafka", "--shift-ts", "soon"]) == 2
    assert "--shift-ts" in capsys.readouterr().err
    assert main(["replay", "--record", str(record) + ".nope", "--no-kafka"]) == 2
    assert "record not found" in capsys.readouterr().err


def test_replay_malformed_record(tmp_path, capsys):
    rec = tmp_path / "bad.jsonl"
    rec.write_text('{"emit_at": 1, "key": "T-01", "value": {"ts": 1}}\nnot json\n')
    assert main(["replay", "--record", str(rec), "--no-kafka"]) == 2
    captured = capsys.readouterr()
    assert len(lines(captured.out)) == 1 and "malformed record line" in captured.err


def test_module_entry_point():
    proc = subprocess.run([sys.executable, "-m", "generator", "run", "--scenario", SCENARIO, "--no-kafka",
                           "--speed", "0", "--duration-s", "2", "--no-faults", "--quiet"],
                          cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert len(proc.stdout.splitlines()) == 8
    proc = subprocess.run([sys.executable, "-m", "generator", "run", "--site", "nope.yaml"],
                          cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert proc.returncode == 2 and proc.stderr.startswith("error: file not found")


def test_sigterm_stops_cleanly(tmp_path):
    rec = tmp_path / "r.jsonl"
    proc = subprocess.Popen([sys.executable, "-m", "generator", "run", "--site", SITE, "--no-kafka", "--speed", "50",
                             "--warmup-min", "0", "--record", str(rec)], cwd=ROOT, stderr=subprocess.PIPE, text=True)
    import time
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and (not rec.exists() or rec.stat().st_size < 10_000):
        time.sleep(0.05)
    proc.terminate()
    _, err = proc.communicate(timeout=20)
    assert proc.returncode == 143, err
    assert "INTERRUPTED" in err
    ems = list(read_record(rec))           # complete lines only, faults drained
    assert len(ems) > 0 and "pending=0" in err


# ------------------------------------------------------------------------------------------------- pipe safety

def _read_lines_then_close(argv, n=3, timeout=60):
    proc = subprocess.Popen([sys.executable, "-m", "generator", *argv], cwd=ROOT, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)
    got = [proc.stdout.readline() for _ in range(n)]
    proc.stdout.close()                        # reader goes away early
    _, err = proc.communicate(timeout=timeout)
    return proc.returncode, got, err


@pytest.mark.parametrize("argv", [
    ["run", "--site", SITE, "--no-kafka", "--speed", "0", "--duration-s", "600", "--warmup-min", "0"],
    ["run", "--site", SITE, "--no-kafka", "--speed", "0", "--warmup-min", "0"],          # endless: must stop
    ["run", "--site", SITE, "--no-kafka", "--speed", "200", "--warmup-min", "0"],        # paced, line-flushed
])
def test_run_stdout_reader_closes_early(argv):
    code, got, err = _read_lines_then_close(argv)
    assert code == 0, err
    assert all(json.loads(x)["key"] for x in got)
    assert "Exception ignored" not in err and "Traceback" not in err and "BrokenPipeError" not in err
    assert "STOPPED: reader closed stdout" in err and "pending=0" in err


def test_replay_stdout_reader_closes_early(record):
    code, got, err = _read_lines_then_close(["replay", "--record", str(record), "--no-kafka"], n=1)
    assert code == 0 and "Traceback" not in err and "Exception ignored" not in err
    assert "STOPPED: reader closed stdout" in err


def test_pipe_into_head_is_silent():
    cmd = (f"set -o pipefail; {sys.executable} -m generator run --site {SITE} --no-kafka --speed 0 "
           f"--duration-s {{d}} --quiet | head -3")
    for d in (2, 2, 2, 300):
        proc = subprocess.run(["bash", "-c", cmd.format(d=d)], cwd=ROOT, capture_output=True, text=True, timeout=60)
        assert (proc.returncode, proc.stderr) == (0, ""), proc.stderr
        assert len(proc.stdout.splitlines()) == 3
