"""In-process closed loop (no broker): the generator's simulator and the engine, with zero reaction latency.

The generator's run loop hands every emission (faults included) straight to the engine; recommended decisions go
back through the generator's follower interface and are applied on the tick after their `at`, exactly as with
`generator run --follow-decisions` against a service that answers instantly. The generator's ground truth
(`--truth` events) is then compared with what the service published.
"""

from __future__ import annotations

import statistics
from collections import Counter
from pathlib import Path

import pytest

from vqservice.config import EngineConfig
from vqservice.engine import Engine
from vqservice.site import load_site
from vqservice.timefmt import parse_iso

pytest.importorskip("generator.runner")
from generator.constants import DEFAULT_START_TS  # noqa: E402
from generator.model import Redirect  # noqa: E402
from generator.runner import RunConfig, run  # noqa: E402
from generator.site import load_site as gen_load_site  # noqa: E402

SITE_FILE = Path(__file__).resolve().parents[2] / "site.yaml"
DURATION_S = 900
SEED = 11
MAX_LATENESS_S = 60


class EngineLoop:
    """Generator sink and decision follower at once."""
    name = "engine"

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.outputs: list = []
        self.first_ts: dict[str, int] = {}       # unit -> ts of its first message (cold start)
        self._pending: list[Redirect] = []

    def send(self, em) -> None:
        self.first_ts.setdefault(em.key, em.value["ts"])
        for out in self.engine.process(em.value):
            self.outputs.append(out)
            if out.kind == "decision" and out.value["result"] == "recommended":
                self._pending.append(Redirect(out.key, out.value["to_station"], parse_iso(out.value["at"])))

    def poll(self) -> list[Redirect]:
        got, self._pending = self._pending, []
        return got

    def tick(self) -> None:
        pass

    def close(self) -> dict:
        return {}


class TruthLog:
    path = "truth"

    def __init__(self) -> None:
        self.events: list[dict] = []

    def write(self, obj: dict) -> None:
        self.events.append(obj)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


@pytest.fixture(scope="module")
def loop() -> tuple[EngineLoop, list[dict]]:
    engine_loop, truth = EngineLoop(Engine(load_site(SITE_FILE), EngineConfig())), TruthLog()
    cfg = RunConfig(site=gen_load_site(SITE_FILE), start_ts=DEFAULT_START_TS, duration_s=DURATION_S,
                    warmup_s=3600, seed=SEED, speed=0, run_id="closed-loop")
    summary = run(cfg, sinks=[engine_loop], truth=truth, follower=engine_loop)
    assert summary.ok and summary.ticks == DURATION_S
    return engine_loop, truth.events


def recommended(engine_loop: EngineLoop) -> list:
    return [o for o in engine_loop.outputs if o.kind == "decision" and o.value["result"] == "recommended"]


def occupancies(engine_loop: EngineLoop) -> dict[tuple[str, str], list[int]]:
    """(station, unit) -> occupancy times as published: the queue head with eta null."""
    occ: dict[tuple[str, str], set[int]] = {}
    for o in engine_loop.outputs:
        if o.kind == "queue" and o.value["queue"] and o.value["queue"][0]["eta"] is None:
            head = o.value["queue"][0]
            occ.setdefault((o.key, head["unit_uuid"]), set()).add(parse_iso(head["service_start"]))
    return {k: sorted(v) for k, v in occ.items()}


def is_cold(engine_loop: EngineLoop, decision) -> bool:
    """Triggered by the unit's first message: the unit may be heading anywhere ("to station" is only its default)."""
    unit, ts = dict(decision.headers)["decision-id"].rsplit(":", 1)
    return engine_loop.first_ts[unit] == int(ts)


def test_every_recommendation_is_applied(loop):
    """Every recommendation is applied, except cold-start ones for units that are not driving to their station
    (on the way to the unload point, or already inside the station radius): the generator ignores those."""
    engine_loop, truth = loop
    recs = recommended(engine_loop)
    applied = [e for e in truth if e["event"] == "redirect_applied"]
    ignored = [e for e in truth if e["event"] == "redirect_ignored"]
    print(f"\nclosed loop {DURATION_S} s: decisions {engine_loop.engine.stats['decisions']}, "
          f"recommended {len(recs)}, applied {len(applied)}, ignored {len(ignored)}")
    ignored_keys = {(e["unit_uuid"], e["detail"]["to_station"]) for e in ignored}
    assert all(e["detail"]["reason"] in ("not_to_station", "inside_radius") for e in ignored)
    kept = []
    for o in recs:
        if (o.key, o.value["to_station"]) in ignored_keys:
            assert is_cold(engine_loop, o), o
        else:
            kept.append(o)
    assert len(kept) >= 8 and len(ignored) == len(recs) - len(kept)
    assert [(e["unit_uuid"], e["detail"]["to_station"]) for e in applied] == \
        [(o.key, o.value["to_station"]) for o in kept]
    for e, o in zip(applied, kept):
        assert e["detail"]["from_station"] == o.value["from_station"]
        assert 1 <= e["ts"] - parse_iso(o.value["at"]) <= MAX_LATENESS_S


def test_redirected_units_are_served_at_the_recommended_station(loop):
    engine_loop, truth = loop
    occ = occupancies(engine_loop)
    served = 0
    for a in (e for e in truth if e["event"] == "redirect_applied"):
        unit, to = a["unit_uuid"], a["detail"]["to_station"]
        starts = [e for e in truth if e["unit_uuid"] == unit and e["event"] == "service_start" and e["ts"] > a["ts"]]
        if not starts:
            continue                                                  # still driving when the run ended
        assert starts[0]["station_uuid"] == to, (a, starts[0])
        assert any(t >= a["ts"] for t in occ.get((to, unit), [])), a  # the service saw it occupy `to`
        served += 1
    assert served >= 8


def test_engine_occupancy_matches_truth(loop):
    """Every service start of the generator is an occupancy of the same unit at the same station in queue.v1:
    at the same second at a free station, a few seconds later after a queue (the engine frees the station when
    the occupant leaves the 50 m radius, the generator at departure), never later than the max lateness."""
    engine_loop, truth = loop
    occ = occupancies(engine_loop)
    deltas = []
    for e in (e for e in truth if e["event"] == "service_start"):
        times = [t - e["ts"] for t in occ.get((e["station_uuid"], e["unit_uuid"]), []) if t >= e["ts"]]
        assert times and min(times) <= MAX_LATENESS_S, e
        deltas.append(min(times))
    print(f"\noccupancy engine - truth, s: {sorted(Counter(deltas).items())}")
    assert len(deltas) >= 12 and statistics.median(deltas) <= 5
    # and the other way round: no phantom occupancy (initial occupants of the cold start aside)
    starts = {(e["station_uuid"], e["unit_uuid"], e["ts"]) for e in truth if e["event"] == "service_start"}
    for (station, unit), times in occ.items():
        for t in times:
            if t > DEFAULT_START_TS + MAX_LATENESS_S:
                assert any((station, unit, t - d) in starts for d in range(MAX_LATENESS_S + 1)), (station, unit, t)
