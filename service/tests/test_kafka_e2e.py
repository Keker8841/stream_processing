"""End-to-end against a real broker: generator -> telemetry -> `python -m vqservice run` -> queue.v1 / decision.v1.

Skipped unless KAFKA_BOOTSTRAP is set (`../gen/scripts/kafka_local.sh start`). Every test uses its own topics,
consumer group, transactional.id and state key, so tests never see each other's data. The oracle is the offline
pipeline (`python -m vqservice process`) on the same input in the same order: read_committed outputs must equal it
exactly (order per topic, keys, values byte for byte, decision-id headers). Run with `-s` to see the measurements.

(a) record replayed, then processed (backlog throughput); (b) kill -9 at random moments while the record streams in;
(c) closed loop with `generator run --follow-decisions` + ground truth; (d) shadow mode with the open-loop generator;
(e) a paused instance fenced by its replacement.

Env knobs: E2E_CRASH_SEED (kill moments), E2E_CLOSED_LOOP_SPEED / E2E_CLOSED_LOOP_S (closed-loop run).
"""

from __future__ import annotations

import json
import os
import random
import re
import signal
import statistics
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from vqservice.config import EngineConfig
from vqservice.engine import Engine
from vqservice.offline import dumps_compact, read_values
from vqservice.site import load_site
from vqservice.timefmt import parse_iso

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP")
pytestmark = [pytest.mark.kafka, pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_BOOTSTRAP not set")]
gen_cli = pytest.importorskip("generator.cli")
from generator.constants import DEFAULT_START_TS  # noqa: E402

SERVICE = Path(__file__).resolve().parents[1]
SITE_FILE = SERVICE.parent / "site.yaml"
RECORD_S = 3600
CRASH_SEED = int(os.environ.get("E2E_CRASH_SEED", "20260916"))
CRASH_KILLS = 3
CRASH_REPLAY_SPEED = 120                      # the 1 h record streams in for ~30 s while the service is killed
CLOSED_LOOP_SPEED = float(os.environ.get("E2E_CLOSED_LOOP_SPEED", "20"))
CLOSED_LOOP_S = int(os.environ.get("E2E_CLOSED_LOOP_S", "1800"))
SHADOW_SPEED, SHADOW_S = 20.0, 900
MAX_LATENESS_S = 60
POLL_S = 0.02
COORDINATOR_PENDING = {KafkaError.NOT_COORDINATOR, KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
                       KafkaError.COORDINATOR_NOT_AVAILABLE}

Outputs = dict[str, list[tuple[str, bytes, dict[str, str]]]]     # kind -> [(key, value, headers)]


# ------------------------------------------------------------------------------------------------ harness

@dataclass(frozen=True)
class Deployment:
    """One service instance with private topics, group and transactional.id."""
    prefix: str
    config: Path

    def topic(self, kind: str) -> str:
        return f"{self.prefix}.{kind}"


def deploy(tmp_path: Path, name: str, **engine) -> Deployment:
    prefix = f"e2e-{name}-{uuid.uuid4().hex[:8]}"
    cfg = tmp_path / f"{prefix}.yaml"
    cfg.write_text(yaml.safe_dump({"site_file": str(SITE_FILE), "engine": engine, "kafka": {
        "bootstrap": BOOTSTRAP, "telemetry_topic": f"{prefix}.telemetry", "queue_topic": f"{prefix}.queue",
        "decision_topic": f"{prefix}.decision", "state_topic": f"{prefix}.state", "group_id": prefix,
        "transactional_id": prefix, "state_key": prefix}}))
    return Deployment(prefix, cfg)


class Service:
    """`python -m vqservice run` subprocess. Only ever signals its own PID."""

    def __init__(self, dep: Deployment, log: Path, *args: str) -> None:
        self.log = log
        self._out = open(log, "ab")
        self.started = time.monotonic()
        self.proc = subprocess.Popen([sys.executable, "-m", "vqservice", "run", "--config", str(dep.config), *args],
                                     cwd=SERVICE, stdout=self._out, stderr=subprocess.STDOUT)

    def alive(self) -> bool:
        return self.proc.poll() is None

    def kill9(self) -> None:
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(10)
        self._out.close()

    def stop(self) -> int:
        """SIGTERM: the service commits the open transaction and exits."""
        self.proc.send_signal(signal.SIGTERM)
        code = self.proc.wait(60)
        self._out.close()
        return code

    def text(self) -> str:
        return self.log.read_text(errors="replace")

    def summary(self) -> dict[str, int]:
        """Counters of the final `stopped:` line."""
        m = re.search(r"stopped: consumed (\d+),.* commits (\d+), aborts (\d+)", self.text())
        assert m, self.text()[-2000:]
        return {"consumed": int(m[1]), "commits": int(m[2]), "aborts": int(m[3])}


@pytest.fixture
def services():
    started: list[Service] = []

    def start(dep: Deployment, log: Path, *args: str) -> Service:
        svc = Service(dep, log, *args)
        started.append(svc)
        return svc
    yield start
    for svc in started:
        if svc.alive():
            svc.kill9()


class Committed:
    """Committed input offset of the service's group (0 if none). Uses read_uncommitted: a read_committed
    OffsetFetch would block while a killed producer's transactional offset commit is still pending."""

    def __init__(self, dep: Deployment) -> None:
        self._c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": dep.prefix,
                            "isolation.level": "read_uncommitted", "enable.auto.commit": False})
        self._tp = TopicPartition(dep.topic("telemetry"), 0)

    def __call__(self) -> int:
        deadline = time.monotonic() + 30
        while True:
            try:
                return max(0, self._c.committed([self._tp], timeout=10)[0].offset)
            except KafkaException as e:  # a freshly started broker is still loading the group coordinator
                if e.args[0].code() not in COORDINATOR_PENDING or time.monotonic() > deadline:
                    raise
                time.sleep(0.2)

    def wait(self, target: int, timeout_s: float, svc: Service | None = None) -> float:
        """Poll until the committed offset reaches `target`; returns the monotonic time it did."""
        deadline = time.monotonic() + timeout_s
        while (now := time.monotonic()) < deadline:
            if self() >= target:
                return now
            if svc is not None and not svc.alive():
                pytest.fail(f"service exited with {svc.proc.returncode}:\n{svc.text()[-3000:]}")
            time.sleep(POLL_S)
        pytest.fail(f"committed offset {self()} did not reach {target} within {timeout_s} s")

    def close(self) -> None:
        self._c.close()


class OpenTxn:
    """True while a transaction has written outputs that are not committed yet: the high watermark of the queue
    topic is ahead of its last stable offset (what a read_committed watermark query returns)."""

    def __init__(self, dep: Deployment) -> None:
        self._tp = TopicPartition(dep.topic("queue"), 0)
        self._c = {iso: Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"{dep.prefix}-probe",
                                  "isolation.level": iso}) for iso in ("read_committed", "read_uncommitted")}

    def __call__(self) -> bool:
        lso = self._c["read_committed"].get_watermark_offsets(self._tp, timeout=10)[1]
        return self._c["read_uncommitted"].get_watermark_offsets(self._tp, timeout=10)[1] > lso

    def close(self) -> None:
        for c in self._c.values():
            c.close()


def read_topic(topic: str, isolation: str = "read_committed") -> list:
    """Every message of partition 0 up to the end (the LSO for read_committed)."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"e2e-reader-{uuid.uuid4().hex[:8]}",
                  "isolation.level": isolation, "enable.auto.commit": False, "enable.partition.eof": True})
    c.assign([TopicPartition(topic, 0, 0)])
    out = []
    deadline = time.monotonic() + 60
    try:
        while time.monotonic() < deadline:
            for m in c.consume(1000, timeout=1.0):
                if m.error() is None:
                    out.append(m)
                elif m.error().code() == KafkaError._PARTITION_EOF:
                    return out
        pytest.fail(f"no end of {topic} within 60 s")
    finally:
        c.close()


def kafka_outputs(dep: Deployment) -> Outputs:
    return {kind: [(m.key().decode(), m.value(), {k: v.decode() for k, v in (m.headers() or [])})
                   for m in read_topic(dep.topic(kind))] for kind in ("queue", "decision")}


def oracle(record: Path, **engine) -> Outputs:
    eng = Engine(load_site(SITE_FILE), EngineConfig(**engine))
    out: Outputs = {"queue": [], "decision": []}
    for value in read_values(record):
        for o in eng.process(value):
            out[o.kind].append((o.key, dumps_compact(o.value).encode(), dict(o.headers)))
    return out


def assert_same(got: Outputs, want: Outputs) -> None:
    for kind in ("queue", "decision"):
        assert len(got[kind]) == len(want[kind]), kind
        for i, (g, w) in enumerate(zip(got[kind], want[kind])):
            assert g == w, f"{kind}[{i}]: kafka {g} != offline {w}"


def lines(path: Path) -> int:
    return sum(1 for _ in read_values(path))


def replay(record: Path, topic: str, speed: float = 0) -> None:
    assert gen_cli.main(["replay", "--record", str(record), "--bootstrap", BOOTSTRAP, "--topic", topic,
                         "--speed", str(speed), "--quiet"]) == 0


def generator(args: list[str], log: Path) -> subprocess.Popen:
    with open(log, "ab") as f:
        return subprocess.Popen([sys.executable, "-m", "generator", "run", "--site", str(SITE_FILE),
                                 "--bootstrap", BOOTSTRAP, "--quiet", *args],
                                cwd=SERVICE, stdout=f, stderr=subprocess.STDOUT)


def state_size(dep: Deployment) -> int:
    msgs = read_topic(dep.topic("state"))
    return len(msgs[-1].value())


def per_hour(n: int, seconds: int) -> float:
    return n * 3600 / seconds


@pytest.fixture(scope="module")
def record(tmp_path_factory) -> Path:
    """1 h of the repo site, default faults, 60 min warmup (cold start mid-cycle), fixed seed."""
    rec = tmp_path_factory.mktemp("e2e") / "rec.jsonl"
    assert gen_cli.main(["run", "--site", str(SITE_FILE), "--no-kafka", "--speed", "0", "--quiet",
                         "--warmup-min", "60", "--duration-s", str(RECORD_S), "--seed", "5",
                         "--record", str(rec)]) == 0
    return rec


@pytest.fixture(scope="module")
def record_oracle(record) -> Outputs:
    return oracle(record)


# ------------------------------------------------------------------------------------------------ (a) oracle

def test_pipeline_equals_offline_oracle(record, record_oracle, tmp_path, services):
    """The whole record is in the topic before the service starts: also measures backlog throughput."""
    dep = deploy(tmp_path, "oracle")
    n = lines(record)
    replay(record, dep.topic("telemetry"))
    committed = Committed(dep)
    try:
        svc = services(dep, tmp_path / "service.log")
        first = committed.wait(1, 60, svc)
        done = committed.wait(n, 300, svc)
    finally:
        committed.close()
    assert svc.stop() == 0
    s = svc.summary()
    assert s["consumed"] == n and s["aborts"] == 0
    assert_same(kafka_outputs(dep), record_oracle)
    rate = n / (done - first)
    print(f"\n(a) {n} messages: first commit {first - svc.started:.2f} s after start, then {done - first:.2f} s "
          f"-> {rate:.0f} msg/s, {s['commits']} commits ({s['commits'] / (done - first):.1f}/s), "
          f"snapshot {state_size(dep)} B, outputs queue {len(record_oracle['queue'])} "
          f"decision {len(record_oracle['decision'])}")


# --------------------------------------------------------------------------------------------- (b) crashes

def test_crash_restart_is_exactly_once(record, record_oracle, tmp_path, services):
    """kill -9 at random moments while the record streams in, restart each time: read_committed outputs equal the
    oracle (no duplicates, no gaps). Each kill waits for a random delay, then for an open transaction that has
    already written outputs, so the kill really lands mid-transaction. Restore time = process start -> first commit
    of the new process."""
    dep = deploy(tmp_path, "crash")
    n = lines(record)
    rng = random.Random(CRASH_SEED)
    feeder = threading.Thread(target=replay, args=(record, dep.topic("telemetry"), CRASH_REPLAY_SPEED))
    committed, open_txn = Committed(dep), OpenTxn(dep)
    report = []
    try:
        feeder.start()
        for k in range(CRASH_KILLS + 1):
            svc = services(dep, tmp_path / f"service{k}.log")
            before, first = committed(), None
            if k == CRASH_KILLS:
                first = committed.wait(before + 1, 60, svc) - svc.started
                committed.wait(n, 300, svc)
                break
            kill_at = svc.started + rng.uniform(0.3, 6.0)
            while time.monotonic() < kill_at or not open_txn():
                if first is None and committed() > before:
                    first = time.monotonic() - svc.started
                assert svc.alive(), svc.text()[-3000:]
                assert time.monotonic() < kill_at + 10, "no open transaction to kill"
                time.sleep(POLL_S)
            svc.kill9()
            at = committed()
            assert at < n, "the stream ended before the kill; lower CRASH_REPLAY_SPEED"
            report.append(f"kill {k + 1} at {time.monotonic() - svc.started:.2f} s (committed {before} -> {at}), "
                          f"first commit {'-' if first is None else f'{first:.2f} s'}")
        feeder.join(120)
    finally:
        committed.close()
        open_txn.close()
    assert not feeder.is_alive()
    assert svc.stop() == 0
    assert_same(kafka_outputs(dep), record_oracle)
    restored = [i for i in range(1, CRASH_KILLS + 1) if "engine restored from snapshot" in
                (tmp_path / f"service{i}.log").read_text()]
    queue_all = len(read_topic(dep.topic("queue"), "read_uncommitted"))
    print(f"\n(b) seed {CRASH_SEED}, {n} messages at {CRASH_REPLAY_SPEED}x: " + "; ".join(report)
          + f"; final restart: first commit {first:.2f} s; restored from snapshot in runs {restored}; "
          f"aborted queue records hidden by read_committed: {queue_all - len(record_oracle['queue'])}")
    assert restored                                     # at least one restart resumed from a snapshot
    assert queue_all > len(record_oracle["queue"])      # outputs of killed transactions were written, then aborted


# ------------------------------------------------------------------------------------------- (c) closed loop

def occupancies(queue: list) -> dict[tuple[str, str], list[int]]:
    occ: dict[tuple[str, str], set[int]] = {}
    for key, value, _ in queue:
        q = json.loads(value)["queue"]
        if q and q[0]["eta"] is None:
            occ.setdefault((key, q[0]["unit_uuid"]), set()).add(parse_iso(q[0]["service_start"]))
    return {k: sorted(v) for k, v in occ.items()}


def test_closed_loop_follow_decisions(tmp_path, services):
    """Generator applies the service's recommendations (--follow-decisions) at CLOSED_LOOP_SPEED x real time."""
    dep = deploy(tmp_path, "closed")
    svc = services(dep, tmp_path / "service.log")
    rec, truth_path = tmp_path / "rec.jsonl", tmp_path / "truth.jsonl"
    deadline = time.monotonic() + 60
    while "assigned" not in svc.text():                 # topics exist with the service's settings
        assert time.monotonic() < deadline and svc.alive(), svc.text()
        time.sleep(0.1)
    t0 = time.monotonic()
    gen = generator(["--topic", dep.topic("telemetry"), "--follow-decisions", "--decision-topic",
                     dep.topic("decision"), "--speed", str(CLOSED_LOOP_SPEED), "--duration-s", str(CLOSED_LOOP_S),
                     "--seed", "3", "--truth", str(truth_path), "--record", str(rec)], tmp_path / "gen.log")
    try:
        assert gen.wait(CLOSED_LOOP_S / CLOSED_LOOP_SPEED + 180) == 0, (tmp_path / "gen.log").read_text()
    finally:
        if gen.poll() is None:
            gen.kill()
    wall = time.monotonic() - t0
    committed = Committed(dep)
    try:
        committed.wait(lines(rec), 60, svc)
    finally:
        committed.close()
    assert svc.stop() == 0
    got = kafka_outputs(dep)
    assert_same(got, oracle(rec))                       # the closed loop's input order replays to the same outputs

    truth = [json.loads(x) for x in truth_path.read_text().splitlines()]
    decisions = [json.loads(v) for _, v, _ in got["decision"]]
    results = Counter(d.get("reason", d["result"]) for d in decisions)
    recs = [(d["unit_uuid"], d["to_station"], parse_iso(d["at"])) for d in decisions if d["result"] == "recommended"]
    applied = [e for e in truth if e["event"] == "redirect_applied"]
    ignored = Counter(e["detail"]["reason"] for e in truth if e["event"] == "redirect_ignored")
    lag = []
    for e in applied:                                   # every applied redirect is a published recommendation
        ats = [at for u, to, at in recs if (u, to) == (e["unit_uuid"], e["detail"]["to_station"]) and at < e["ts"]]
        assert ats, e
        lag.append(e["ts"] - max(ats))
    end = DEFAULT_START_TS + CLOSED_LOOP_S - 1                   # last simulated tick
    due = [r for r in recs if r[2] <= end - 30]                  # the generator had time to act on these
    assert due and len(applied) >= 0.9 * len(due), (len(applied), len(due), ignored)

    occ = occupancies(got["queue"])
    served = 0
    for a in applied:
        unit, to = a["unit_uuid"], a["detail"]["to_station"]
        starts = [e for e in truth if e["unit_uuid"] == unit and e["event"] == "service_start" and e["ts"] > a["ts"]]
        if starts:
            assert starts[0]["station_uuid"] == to, (a, starts[0])
            assert any(t >= a["ts"] for t in occ.get((to, unit), [])), a
            served += 1
    deltas = []
    for e in (e for e in truth if e["event"] == "service_start"):
        times = [t - e["ts"] for t in occ.get((e["station_uuid"], e["unit_uuid"]), []) if t >= e["ts"]]
        assert times and min(times) <= MAX_LATENESS_S, e
        deltas.append(min(times))
    queue_by_station = Counter(k for k, _, _ in got["queue"])
    s = svc.summary()
    print(f"\n(c) closed loop {CLOSED_LOOP_S} s at {CLOSED_LOOP_SPEED:g}x ({wall:.0f} s wall): decisions "
          f"{dict(results)}; applied {len(applied)}/{len(recs)} = {len(applied) / len(recs):.0%}, ignored "
          f"{dict(ignored)}; apply lag {min(lag)}..{max(lag)} sim s (median {statistics.median(lag)}); "
          f"redirected units served at the recommended station {served} (rest still driving at the end); "
          f"occupancy engine-truth {sorted(Counter(deltas).items())}; queue.v1 per station-hour "
          f"{ {k: round(per_hour(v, CLOSED_LOOP_S)) for k, v in sorted(queue_by_station.items())} }; "
          f"decisions/h {per_hour(len(decisions), CLOSED_LOOP_S):.0f}; commits {s['commits']} "
          f"({s['commits'] / wall:.1f}/s); snapshot {state_size(dep)} B")


# --------------------------------------------------------------------------------------- (d) shadow open loop

def test_shadow_open_loop_smoke(tmp_path, services):
    dep = deploy(tmp_path, "shadow")
    svc = services(dep, tmp_path / "service.log", "--shadow")
    rec = tmp_path / "rec.jsonl"
    gen = generator(["--topic", dep.topic("telemetry"), "--speed", str(SHADOW_SPEED), "--duration-s",
                     str(SHADOW_S), "--seed", "4", "--record", str(rec)], tmp_path / "gen.log")
    try:
        assert gen.wait(SHADOW_S / SHADOW_SPEED + 120) == 0, (tmp_path / "gen.log").read_text()
    finally:
        if gen.poll() is None:
            gen.kill()
    committed = Committed(dep)
    try:
        committed.wait(lines(rec), 60, svc)
    finally:
        committed.close()
    assert svc.stop() == 0
    assert "shadow mode" in svc.text()
    got = kafka_outputs(dep)
    assert_same(got, oracle(rec, apply_decisions=False))
    per_station = Counter(k for k, _, _ in got["queue"])
    assert set(per_station) == {"S1", "S2", "S3", "S4"}
    results = Counter(json.loads(v).get("reason", json.loads(v)["result"]) for _, v, _ in got["decision"])
    minutes = SHADOW_S / 60
    print(f"\n(d) shadow open loop {SHADOW_S} s at {SHADOW_SPEED:g}x: queue.v1 per station per minute "
          f"{ {k: round(v / minutes, 1) for k, v in sorted(per_station.items())} } "
          f"(total {sum(per_station.values()) / minutes:.0f}/min), decisions {dict(results)}")


# ------------------------------------------------------------------------------------------- (e) zombie fencing

def test_paused_instance_is_fenced_by_its_replacement(record, record_oracle, tmp_path, services):
    """A paused (not dead) instance is replaced by a new one with the same transactional.id / group.instance.id.
    When it wakes up it is fenced (producer epoch, static member id) and exits with the fatal code, and the
    replacement's outputs stay exactly-once."""
    dep = deploy(tmp_path, "zombie")
    n = lines(record)
    replay(record, dep.topic("telemetry"))
    committed = Committed(dep)
    try:
        zombie = services(dep, tmp_path / "zombie.log")
        committed.wait(n // 5, 120, zombie)
        zombie.proc.send_signal(signal.SIGSTOP)
        paused_at = committed()
        successor = services(dep, tmp_path / "successor.log")
        committed.wait(n, 300, successor)
        zombie.proc.send_signal(signal.SIGCONT)
        code = zombie.proc.wait(60)
    finally:
        committed.close()
    assert successor.stop() == 0
    assert code == 1, zombie.text()[-3000:]                      # EXIT_FATAL
    assert_same(kafka_outputs(dep), record_oracle)
    fenced = [x for x in zombie.text().splitlines() if "fatal" in x.lower()]
    print(f"\n(e) paused at committed {paused_at}/{n}; zombie exit {code}: {fenced[-1] if fenced else '-'}")
