"""Offline processing of generator records (no Kafka)."""

import io
import json
from pathlib import Path

import pytest

from fakes import StubEngine
from vqservice.offline import JsonlSink, dumps_compact, output_line, process_record, read_values

REPO = Path(__file__).resolve().parents[2]


def write_record(path: Path, values: list[dict], blank_lines: bool = False) -> Path:
    lines = [dumps_compact({"emit_at": v["ts"], "key": v["unit_uuid"], "value": v}) for v in values]
    path.write_text(("\n\n" if blank_lines else "\n").join(lines) + "\n")
    return path


def tel(unit: str, ts: int, **kw) -> dict:
    return {"unit_uuid": unit, "ts": ts, "lat": 49.1, "lon": 142.6, "speed_kmh": 0.0, **kw}


class ListSink:
    def __init__(self) -> None:
        self.items: list[dict] = []

    def write(self, obj: dict) -> None:
        self.items.append(obj)


def test_process_record_in_file_order(tmp_path):
    rec = write_record(tmp_path / "r.jsonl", [tel("T-01", 10), tel("T-02", 5, decide=True), tel("T-01", 9),
                                              tel("T-01", 11)], blank_lines=True)
    engine, sink = StubEngine(), ListSink()
    stats = process_record(rec, engine, sink)
    assert [v["ts"] for v in engine.seen] == [10, 5, 9, 11]          # file order, nothing reordered
    assert sink.items == [
        {"topic": "queue.v1", "key": "S1", "value": {"station_uuid": "S1", "n": 1}, "headers": {}},
        {"topic": "decision.v1", "key": "T-02",
         "value": {"unit_uuid": "T-02", "at": 5, "result": "rejected", "reason": "no_gain"},
         "headers": {"decision-id": "T-02:5"}},
        {"topic": "queue.v1", "key": "S1", "value": {"station_uuid": "S1", "n": 2}, "headers": {}},
        {"topic": "queue.v1", "key": "S1", "value": {"station_uuid": "S1", "n": 3}, "headers": {}},
    ]
    assert stats == {"lines": 4, "outputs": 4, "queue": 3, "decision": 1, "stopped": False,
                     "engine": {"accepted": 3, "ignored_old": 1}}


def test_custom_topic_names(tmp_path):
    rec = write_record(tmp_path / "r.jsonl", [tel("T-01", 1, decide=True)])
    sink = ListSink()
    process_record(rec, StubEngine(), sink, topics={"queue": "q", "decision": "d"})
    assert [o["topic"] for o in sink.items] == ["d", "q"]


def test_output_line_value_is_untouched():
    class Out:
        kind, key, value, headers = "decision", "T4", {"b": 1, "a": 2}, (("decision-id", "T4:1"),)
    line = output_line(Out())
    assert list(line) == ["topic", "key", "value", "headers"] and list(line["value"]) == ["b", "a"]


@pytest.mark.parametrize("text,match", [
    ('{"emit_at": 1, "key": "T-01", "value": {"ts": 1}}\nnot json\n', r"r\.jsonl:2: malformed record line"),
    ('{"emit_at": 1, "key": "T-01"}\n', r"r\.jsonl:1: malformed record line"),
])
def test_malformed_record_line(tmp_path, text, match):
    rec = tmp_path / "r.jsonl"
    rec.write_text(text)
    with pytest.raises(ValueError, match=match):
        list(read_values(rec))


def test_jsonl_sink_file_is_compact(tmp_path):
    out = tmp_path / "sub" / "o.jsonl"
    sink = JsonlSink(out)
    sink.write({"topic": "queue.v1", "key": "S1", "value": {"a": [1, 2], "s": "ü"}, "headers": {}})
    sink.close()
    assert out.read_text(encoding="utf-8") == '{"topic":"queue.v1","key":"S1","value":{"a":[1,2],"s":"ü"},"headers":{}}\n'
    assert sink.lines == 1 and not sink.gone


class BrokenStdout(io.StringIO):
    def __init__(self, ok_writes: int) -> None:
        super().__init__()
        self.ok = ok_writes

    def write(self, s: str) -> int:
        if self.ok <= 0:
            raise BrokenPipeError
        self.ok -= 1
        return super().write(s)


def test_stdout_reader_gone_stops_processing(tmp_path, monkeypatch):
    monkeypatch.setattr("sys.stdout", BrokenStdout(ok_writes=2))
    rec = write_record(tmp_path / "r.jsonl", [tel("T-01", ts) for ts in range(1, 20)])
    sink = JsonlSink("-")
    engine = StubEngine()
    stats = process_record(rec, engine, sink)
    sink.close()
    assert sink.gone and stats["stopped"]
    assert stats["lines"] == 3 and len(engine.seen) == 3          # the 3rd line's write failed, then stop


def test_real_engine_on_a_generator_record(tmp_path):
    """Wiring smoke test with the real engine and a short generator run on the repo site (deterministic)."""
    gen_cli = pytest.importorskip("generator.cli")
    from vqservice.config import EngineConfig
    from vqservice.engine import Engine
    from vqservice.site import load_site

    rec = tmp_path / "rec.jsonl"
    assert gen_cli.main(["run", "--site", str(REPO / "site.yaml"), "--no-kafka", "--speed", "0", "--quiet",
                         "--warmup-min", "30", "--duration-s", "120", "--record", str(rec)]) == 0
    site = load_site(REPO / "site.yaml")
    runs = []
    for _ in range(2):
        out = tmp_path / f"out{len(runs)}.jsonl"
        sink = JsonlSink(out)
        stats = process_record(rec, Engine(site, EngineConfig()), sink)
        sink.close()
        runs.append(out.read_bytes())
    assert runs[0] == runs[1]                                      # deterministic
    lines = [json.loads(x) for x in runs[0].splitlines()]
    assert stats["lines"] >= 40 * 120 and stats["outputs"] == len(lines)
    assert {x["topic"] for x in lines} <= {"queue.v1", "decision.v1"}
    queues = [x for x in lines if x["topic"] == "queue.v1"]
    assert {x["key"] for x in queues} == {"S1", "S2", "S3", "S4"}       # every station published at start
    assert all(x["value"]["station_uuid"] == x["key"] and x["headers"] == {} for x in queues)
    for d in (x for x in lines if x["topic"] == "decision.v1"):
        assert d["headers"]["decision-id"].startswith(d["key"] + ":")
