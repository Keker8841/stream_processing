"""In-process integration (no broker): generator record -> offline pipeline -> queue.v1 / decision.v1 lines.

One 20 min record of the repo site with the generator's default faults (duplicates, late records, lag episodes),
60 min warmup (cold start mid-cycle) and a fixed seed feeds every check: determinism, mid-stream snapshot/restore,
a time-shifted replay, shadow mode vs applied decisions, payload invariants of every published message, and every
decision re-derived from the published queues.
"""

from __future__ import annotations

import functools
import json
from collections import Counter
from pathlib import Path

import pytest

import vqservice.cli as cli
from vqservice.config import EngineConfig
from vqservice.engine import Engine
from vqservice.geo import haversine_m
from vqservice.offline import dumps_compact, output_line, read_values
from vqservice.site import load_site
from vqservice.timefmt import iso, parse_iso, round_half_up

gen_cli = pytest.importorskip("generator.cli")
from generator.recording import JsonlWriter, emission_to_line  # noqa: E402
from generator.replay import replay  # noqa: E402

SERVICE = Path(__file__).resolve().parents[1]
SITE_FILE = SERVICE.parent / "site.yaml"
DURATION_S = 1200
SEED = 7
SHIFT_S = 3 * 86400 + 12345            # not day-aligned: date, hour, minute and second of every ISO time change
RESTORE_EVERY = 4999                   # snapshot -> JSON -> restore every N record lines
NEVER = 10**9                          # min_gain_s that no gain reaches: decisions computed, none recommended

Call = list[str]                       # output lines (compact JSON) of one engine.process() call


@pytest.fixture(scope="module")
def site():
    return load_site(SITE_FILE)


@pytest.fixture(scope="module")
def record(tmp_path_factory) -> Path:
    rec = tmp_path_factory.mktemp("integration") / "rec.jsonl"
    assert gen_cli.main(["run", "--site", str(SITE_FILE), "--no-kafka", "--speed", "0", "--quiet",
                         "--warmup-min", "60", "--duration-s", str(DURATION_S), "--seed", str(SEED),
                         "--record", str(rec)]) == 0
    return rec


def run_calls(record: Path, engine: Engine, restore_every: int | None = None) -> list[Call]:
    """Outputs per input line; with `restore_every` the engine is rebuilt from a JSON snapshot every N lines."""
    calls = []
    for n, value in enumerate(read_values(record), start=1):
        calls.append([dumps_compact(output_line(o)) for o in engine.process(value)])
        if restore_every and n % restore_every == 0:
            engine = Engine.restore(engine.site, engine.cfg, json.loads(json.dumps(engine.snapshot())))
    return calls


def flat(calls: list[Call]) -> list[str]:
    return [line for call in calls for line in call]


@pytest.fixture(scope="module")
def runs(record, site) -> dict[str, list[Call]]:
    """Per-call outputs of the record under several engine configs, computed on first use."""
    configs = {"applied": EngineConfig(), "shadow": EngineConfig(apply_decisions=False),
               "never": EngineConfig(min_gain_s=NEVER)}

    class Runs(dict):
        def __missing__(self, mode: str) -> list[Call]:
            self[mode] = run_calls(record, Engine(site, configs[mode]))
            return self[mode]
    return Runs()


def decisions(lines: list[str]) -> list[dict]:
    return [d for d in map(json.loads, lines) if d["topic"] == "decision.v1"]


def queues(lines: list[str]) -> list[str]:
    return [x for x in lines if x.startswith('{"topic":"queue.v1"')]


# ----------------------------------------------------------------------------------------------- determinism

def test_record_exercises_faults_and_all_outputs(runs, record):
    seen: set[tuple[str, int]] = set()
    last: dict[str, int] = {}
    dups = late = 0
    for v in read_values(record):
        key = (v["unit_uuid"], v["ts"])
        dups += key in seen
        late += key not in seen and v["ts"] < last.get(v["unit_uuid"], v["ts"])
        seen.add(key)
        last[v["unit_uuid"]] = max(last.get(v["unit_uuid"], v["ts"]), v["ts"])
    assert dups > 0 and late > 0
    lines = flat(runs["applied"])
    results = Counter(d["value"].get("reason", d["value"]["result"]) for d in decisions(lines))
    assert results["recommended"] > 0 and results["no_gain"] > 0
    assert {json.loads(x)["key"] for x in queues(lines)} == {"S1", "S2", "S3", "S4"}


def test_offline_process_twice_is_identical(runs, record, tmp_path):
    """`python -m vqservice process` (CLI, file output) and an in-process run produce byte-identical lines."""
    out = tmp_path / "out.jsonl"
    assert cli.main(["process", "--config", str(SERVICE / "service.yaml"), "--record", str(record),
                     "--out", str(out), "--quiet"]) == 0
    assert out.read_text(encoding="utf-8").splitlines() == flat(runs["applied"])


def test_snapshot_restore_mid_stream_is_seamless(runs, record, site):
    """What the Kafka app does on every restart: JSON snapshot -> restore, here every RESTORE_EVERY lines."""
    restored = run_calls(record, Engine(site, EngineConfig()), restore_every=RESTORE_EVERY)
    assert restored == runs["applied"]


# ------------------------------------------------------------------------------------------------ time shift

@functools.cache
def _back(text: str, shift: int) -> str:
    return iso(parse_iso(text) - shift)


def unshift(line: str, shift: int) -> str:
    """Move every time of an output line back by `shift` seconds."""
    doc = json.loads(line)
    v = doc["value"]
    v["at"] = _back(v["at"], shift)
    for item in v.get("queue", ()):
        for k in ("eta", "service_start", "free_at"):
            if item[k] is not None:
                item[k] = _back(item[k], shift)
    if "decision-id" in doc["headers"]:
        unit, ts = doc["headers"]["decision-id"].rsplit(":", 1)
        doc["headers"]["decision-id"] = f"{unit}:{int(ts) - shift}"
    return dumps_compact(doc)


def test_shifted_replay_gives_shifted_outputs(runs, record, site, tmp_path):
    """`generator replay --shift-ts N` of the same record: the same outputs, every time moved by N."""
    shifted = tmp_path / "shifted.jsonl"
    with JsonlWriter(shifted) as w:
        n = replay(record, lambda em: w.write(emission_to_line(em)), shift_ts=SHIFT_S)
    assert n == sum(1 for _ in read_values(record))
    lines = flat(run_calls(shifted, Engine(site, EngineConfig())))
    base = flat(runs["applied"])
    assert len(lines) == len(base)
    assert lines != base                                              # the times really moved
    assert [unshift(x, SHIFT_S) for x in lines] == base


# ----------------------------------------------------------------------------------------------- shadow mode

def decision_ids(calls: list[Call]) -> list[str]:
    return [d["headers"]["decision-id"] for d in decisions(flat(calls))]


def test_decision_triggers_do_not_depend_on_the_mode(runs):
    """Recommendation triggers depend on telemetry only: same decision ids, same order, in every mode."""
    ids = decision_ids(runs["applied"])
    assert ids and ids == decision_ids(runs["shadow"]) == decision_ids(runs["never"])
    assert len(set(ids)) == len(ids)                                  # once per trip, ids unique
    never = Counter(d["value"].get("reason") for d in decisions(flat(runs["never"])))
    assert set(never) <= {"no_gain", "stale_telemetry"}


def test_shadow_mode_publishes_decisions_without_queue_effect(runs):
    shadow, never = flat(runs["shadow"]), flat(runs["never"])
    recommended = [d for d in decisions(shadow) if d["value"]["result"] == "recommended"]
    assert recommended                                                # decisions are computed and published
    assert queues(shadow) == queues(never)                            # ...but no queue ever reflects them


def unit_in(line: str, unit: str) -> bool:
    return any(item["unit_uuid"] == unit for item in json.loads(line)["value"]["queue"])


def test_applied_recommendation_moves_the_unit_in_the_same_message(runs):
    """Applied mode: the queues published with the decision already show the redirect (task example at 12:00:00):
    the unit leaves its own station's queue and joins the recommended one. Shadow mode: it never joins. A unit whose
    very first message triggers the decision was in no queue before, so its own station may not be republished."""
    checked = 0
    for mode in ("applied", "shadow"):
        current: dict[str, str] = {}
        for call in runs[mode]:
            for x in call:
                if '"topic":"queue.v1"' in x:
                    current[json.loads(x)["key"]] = x
            if not call or '"result":"recommended"' not in call[0]:
                continue
            d = json.loads(call[0])["value"]
            pub = {json.loads(x)["key"]: x for x in call[1:]}
            if mode == "applied":
                assert d["to_station"] in pub and unit_in(pub[d["to_station"]], d["unit_uuid"]), d
                assert d["from_station"] not in current or not unit_in(current[d["from_station"]], d["unit_uuid"]), d
                checked += 1
            else:
                assert d["to_station"] not in pub or not unit_in(pub[d["to_station"]], d["unit_uuid"]), d
                assert d["from_station"] not in pub or unit_in(pub[d["from_station"]], d["unit_uuid"]), d
    assert checked > 0


# ------------------------------------------------------------------------------------------ payload invariants

def test_published_payloads_are_consistent(runs, site):
    """Every queue.v1 / decision.v1 payload of the real record, in applied and shadow mode, obeys the task rules:
    occupant first (eta null, free_at = start + 230), then (eta, unit) order with the cascade
    start = max(eta, previous free_at or the station's last release), wait = start - eta, eta within the 30 min
    horizon; `at` never goes back; a unit is in at most one station's current queue; decisions carry the right
    fields, ids and thresholds."""
    cfg = EngineConfig()
    occ = cfg.occupancy_s
    for mode in ("applied", "shadow"):
        current: dict[str, list[str]] = {}
        last_at = 0
        for call in runs[mode]:
            for line in call:
                doc = json.loads(line)
                v = doc["value"]
                at = parse_iso(v["at"])
                assert at >= last_at
                last_at = at
                if doc["topic"] == "decision.v1":
                    unit, ts = doc["headers"]["decision-id"].rsplit(":", 1)
                    assert unit == v["unit_uuid"] == doc["key"] and int(ts) <= at
                    if v["result"] == "recommended":
                        assert list(v) == ["unit_uuid", "at", "result", "from_station", "to_station", "gain_seconds"]
                        assert v["from_station"] == site.assignment[unit] != v["to_station"]
                        assert v["gain_seconds"] >= cfg.min_gain_s and at - int(ts) <= cfg.fresh_s
                    else:
                        assert list(v) == ["unit_uuid", "at", "result", "reason"]
                        stale = at - int(ts) > cfg.fresh_s
                        assert v["reason"] == ("stale_telemetry" if stale else "no_gain")
                    continue
                assert list(v) == ["station_uuid", "at", "queue"] and v["station_uuid"] == doc["key"]
                prev_free, prev_key = None, None
                for i, item in enumerate(v["queue"]):
                    assert list(item) == ["unit_uuid", "eta", "service_start", "free_at", "wait_seconds"]
                    start, free = parse_iso(item["service_start"]), parse_iso(item["free_at"])
                    assert free == start + occ
                    if item["eta"] is None:                           # the occupant
                        assert i == 0 and item["wait_seconds"] == 0 and start <= at < free
                    else:
                        eta = parse_iso(item["eta"])
                        assert eta <= at + cfg.horizon_s
                        assert (eta, item["unit_uuid"]) > (prev_key or (eta - 1, ""))
                        if prev_free is None:                         # no occupant: from the last release
                            assert start == eta or eta < start <= at
                        else:
                            assert start == max(eta, prev_free)
                        assert item["wait_seconds"] == start - eta >= 0
                        prev_key = (eta, item["unit_uuid"])
                    prev_free = free
                current[doc["key"]] = [item["unit_uuid"] for item in v["queue"]]
            members = [u for units in current.values() for u in units]
            assert len(members) == len(set(members)), current


def test_shadow_decisions_follow_from_the_published_queues(runs, record, site):
    """Re-derive every fresh decision from the outside: in shadow mode the queues published up to and including the
    decision's message are exactly the state the decision saw. For each station: our eta = max(ts + d/speed, now),
    release = free_at of the last queue item ahead of us (occupant always ahead), wait = max(release - eta, 0);
    min wait wins, ties: own station, then nearest; gain = own wait - chosen wait, recommended if >= 60 s."""
    cfg = EngineConfig()
    current: dict[str, list[dict]] = {}
    checked = 0
    for value, call in zip(read_values(record), runs["shadow"]):
        docs = [json.loads(x) for x in call]
        for d in docs:
            if d["topic"] == "queue.v1":
                current[d["key"]] = d["value"]["queue"]
        if not docs or docs[0]["topic"] != "decision.v1" or docs[0]["value"].get("reason") == "stale_telemetry":
            continue
        dec, unit, ts = docs[0]["value"], value["unit_uuid"], value["ts"]
        assert dec["unit_uuid"] == unit and docs[0]["headers"]["decision-id"] == f"{unit}:{ts}"
        now, own = parse_iso(dec["at"]), site.assignment[unit]
        best, waits = None, {}
        for st in site.stations:
            dist = haversine_m(value["lat"], value["lon"], st.lat, st.lon)
            eta = max(ts + round_half_up(dist / cfg.speed_mps), now)
            release = None
            for item in current.get(st.uuid, []):
                if item["unit_uuid"] == unit:
                    continue
                if item["eta"] is None or (parse_iso(item["eta"]), item["unit_uuid"]) < (eta, unit):
                    release = parse_iso(item["free_at"])
            waits[st.uuid] = max(0, release - eta) if release is not None else 0
            key = (waits[st.uuid], st.uuid != own, dist, st.uuid)
            best = key if best is None or key < best else best
        chosen = best[3]
        gain = waits[own] - waits[chosen]
        if chosen == own or gain < cfg.min_gain_s:
            assert dec["result"] == "rejected" and dec["reason"] == "no_gain", (dec, waits)
        else:
            assert (dec["result"], dec["to_station"], dec["gain_seconds"]) == ("recommended", chosen, gain), \
                (dec, waits)
        checked += 1
    assert checked >= 20
