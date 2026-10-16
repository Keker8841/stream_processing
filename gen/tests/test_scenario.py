import math
import re
from pathlib import Path

import pytest
import yaml

from generator.geo import Projection, dist_xy, projection_for
from generator.model import ScriptedDelay, UnitInit
from generator.scenario import Scenario, load_scenario, parse_ts
from generator.site import generate_site, save_site, validate_site

EXAMPLE = Path(__file__).resolve().parent.parent / "scenarios" / "task_example.yaml"
T_1200 = 1789473600   # 2026-09-15T12:00:00Z
CENTER = (49.0, 142.0)


def _site_doc() -> dict:
    """Unload at the center, S1..S4 3 km east/north/west/south of it."""
    p = Projection(*CENTER)
    xy = {"S1": (3000.0, 0.0), "S2": (0.0, 3000.0), "S3": (-3000.0, 0.0), "S4": (0.0, -3000.0)}
    stations = []
    for sid, (x, y) in xy.items():
        lat, lon = p.to_latlon(x, y)
        stations.append({"uuid": sid, "lat": round(lat, 6), "lon": round(lon, 6)})
    return {
        "center": {"lat": CENTER[0], "lon": CENTER[1]},
        "unload": {"uuid": "U1", "lat": CENTER[0], "lon": CENTER[1]},
        "stations": stations,
        "assignment": {"T-01": "S1", "T-02": "S2", "T-03": "S3", "T-04": "S4", "T-05": "S1"},
    }


def _doc(**kw) -> dict:
    d = {"name": "t", "start_ts": T_1200, "site": _site_doc(), "units": []}
    d.update(kw)
    return d


def _write(tmp_path: Path, doc: dict, name: str = "s.yaml") -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    return path


def _xy(sc: Scenario, lat: float, lon: float) -> tuple[float, float]:
    return projection_for(sc.site).to_xy(lat, lon)


def _point(sc: Scenario, uuid: str) -> tuple[float, float]:
    site = sc.site
    p = site.unload if uuid == site.unload.uuid else site.station(uuid)
    return _xy(sc, p.lat, p.lon)


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * 6_371_000.0 * math.asin(math.sqrt(a))


def test_inline_site_minimal(tmp_path):
    sc = load_scenario(_write(tmp_path, _doc(units=None)))
    assert sc.name == "t" and sc.description == "" and sc.start_ts == T_1200
    assert sc.initial == ()
    assert sc.site.station_ids == ["S1", "S2", "S3", "S4"]
    assert sc.site.unit_ids == ["T-01", "T-02", "T-03", "T-04", "T-05"]
    assert (sc.coord_precision, sc.duration_s, sc.expect, sc.service_s, sc.maneuver_s) == (None,) * 5
    assert validate_site(sc.site) == []
    hash(sc)


def test_optional_fields_and_expect_passthrough(tmp_path):
    expect = {"queue": [{"unit_uuid": "T-01", "wait_s": 5}], "note": "x"}
    sc = load_scenario(_write(tmp_path, _doc(description="d", coord_precision=6, duration_s=300, service_s=180,
                                             maneuver_s=30, expect=expect)))
    assert (sc.description, sc.coord_precision, sc.duration_s, sc.service_s, sc.maneuver_s) == ("d", 6, 300, 180, 30)
    assert sc.expect == expect
    hash(sc)  # expect does not take part in hashing


def test_relative_positions_resolve(tmp_path):
    units = [
        {"unit_uuid": "T-01", "phase": "to_station",
         "position": {"from_station": "S1", "distance_m": 1800, "toward": "U1"}},
        {"unit_uuid": "T-02", "phase": "to_station", "target_station": "S1",
         "position": {"from_station": "S2", "distance_m": 500.5, "toward": "S1"}},
        {"unit_uuid": "T-03", "phase": "to_unload",
         "position": {"from_station": "S3", "distance_m": 1000, "toward": 90}},
        {"unit_uuid": "T-04", "phase": "to_station",
         "position": {"from_station": "S4", "distance_m": 2500, "toward": 225.0}},
    ]
    sc = load_scenario(_write(tmp_path, _doc(units=units)))
    init = {u.unit_uuid: u for u in sc.initial}
    assert [u.unit_uuid for u in sc.initial] == ["T-01", "T-02", "T-03", "T-04"]

    p1 = _xy(sc, init["T-01"].lat, init["T-01"].lon)
    s1, u1 = _point(sc, "S1"), _point(sc, "U1")
    assert dist_xy(p1, s1) == pytest.approx(1800.0, abs=0.5)
    assert dist_xy(p1, u1) == pytest.approx(dist_xy(s1, u1) - 1800.0, abs=0.5)   # between S1 and U1

    p2 = _xy(sc, init["T-02"].lat, init["T-02"].lon)
    s2 = _point(sc, "S2")
    assert dist_xy(p2, s2) == pytest.approx(500.5, abs=0.5)
    assert dist_xy(p2, s1) == pytest.approx(dist_xy(s1, s2) - 500.5, abs=0.5)
    assert init["T-02"].target_station == "S1"

    p3, s3 = _xy(sc, init["T-03"].lat, init["T-03"].lon), _point(sc, "S3")
    assert p3[0] - s3[0] == pytest.approx(1000.0, abs=0.5)    # due east
    assert p3[1] - s3[1] == pytest.approx(0.0, abs=0.5)
    assert init["T-03"].phase == "to_unload" and init["T-03"].target_station is None

    p4, s4 = _xy(sc, init["T-04"].lat, init["T-04"].lon), _point(sc, "S4")
    h = 2500.0 / math.sqrt(2)                                  # south-west
    assert (p4[0] - s4[0], p4[1] - s4[1]) == (pytest.approx(-h, abs=0.5), pytest.approx(-h, abs=0.5))


def test_phases_and_defaults(tmp_path):
    units = [
        {"unit_uuid": "T-01", "phase": "waiting", "target_station": "S1", "radius_enter_ts": "2026-09-15T11:59:30Z"},
        {"unit_uuid": "T-02", "phase": "in_service", "target_station": "S1", "service_elapsed_s": 120},
        {"unit_uuid": "T-03", "phase": "in_service"},
        {"unit_uuid": "T-04", "phase": "to_unload", "position": {"lat": 49.01, "lon": 142.02}},
        {"unit_uuid": "T-05", "phase": "to_station", "lat": 49.005, "lon": 142.001},
    ]
    sc = load_scenario(_write(tmp_path, _doc(units=units)))
    assert sc.initial == (
        UnitInit("T-01", "waiting", None, radius_enter_ts=T_1200 - 30),  # own station -> None
        UnitInit("T-02", "in_service", "S1", service_elapsed_s=120),
        UnitInit("T-03", "in_service"),
        UnitInit("T-04", "to_unload", lat=49.01, lon=142.02),
        UnitInit("T-05", "to_station", lat=49.005, lon=142.001),
    )


def test_site_file_relative_to_scenario(tmp_path):
    (tmp_path / "sites").mkdir()
    (tmp_path / "scen").mkdir()
    save_site(generate_site(42), tmp_path / "sites" / "site.yaml")
    doc = _doc(units=[{"unit_uuid": "T-11", "phase": "to_station",
                       "position": {"from_station": "S2", "distance_m": 1600, "toward": "U1"}}])
    del doc["site"]
    doc["site_file"] = "../sites/site.yaml"
    sc = load_scenario(_write(tmp_path / "scen", doc))
    assert sc.site == generate_site(42)
    (u,) = sc.initial
    s2 = sc.site.station("S2")
    assert projection_for(sc.site).distance_m(u.lat, u.lon, s2.lat, s2.lon) == pytest.approx(1600.0, abs=0.5)


@pytest.mark.parametrize("text", ["center: [unclosed\n", "- a list\n", "center: {lat: 1, lon: 2}\n"])
def test_bad_site_file(tmp_path, text):
    (tmp_path / "site.yaml").write_text(text)
    doc = _doc()
    del doc["site"]
    doc["site_file"] = "site.yaml"
    with pytest.raises(ValueError, match="site"):
        load_scenario(_write(tmp_path, doc))


def test_repo_site_file(tmp_path):
    repo_site = Path(__file__).resolve().parents[2] / "site.yaml"
    if not repo_site.exists():
        pytest.skip("site.yaml not generated")
    doc = _doc()
    del doc["site"]
    doc["site_file"] = str(repo_site)       # absolute paths work too
    assert load_scenario(_write(tmp_path, doc)).site.unit_ids[0] == "T-01"


@pytest.mark.parametrize("text, expected", [
    ('"2026-09-15T12:00:00Z"', T_1200),
    ("2026-09-15T12:00:00Z", T_1200),                 # unquoted: YAML timestamp
    ('"2026-09-15T12:00:00+00:00"', T_1200),
    ('"2026-09-15T15:00:00+03:00"', T_1200),
    ("1789473600", T_1200),
    ('"1789473600"', T_1200),
])
def test_start_ts_forms(tmp_path, text, expected):
    path = _write(tmp_path, _doc(start_ts=0))
    path.write_text(path.read_text().replace("start_ts: 0", f"start_ts: {text}"))
    assert load_scenario(path).start_ts == expected


def test_parse_ts():
    assert parse_ts(5) == 5
    assert parse_ts(" -7 ") == -7
    assert parse_ts("2026-09-15T12:00:00.900Z") == T_1200
    for bad in ("2026-09-15T12:00:00", "2026-09-15", "noon", True, 1.5, None, "--5"):
        with pytest.raises(ValueError):
            parse_ts(bad)


def _delay(**kw):
    d = {"unit_uuid": "T-01", "from_ts": T_1200, "to_ts": T_1200 + 9, "emit_at": T_1200 + 10}
    d.update(kw)
    return d


def test_delays_parsed(tmp_path):
    sc = load_scenario(_write(tmp_path, _doc(delays=[_delay(), _delay(unit_uuid="T-02", to_ts="2026-09-15T12:00:30Z",
                                                                   emit_at=T_1200 + 60)])))
    assert sc.delays == (ScriptedDelay("T-01", T_1200, T_1200 + 9, T_1200 + 10),
                         ScriptedDelay("T-02", T_1200, T_1200 + 30, T_1200 + 60))
    assert load_scenario(_write(tmp_path, _doc(), "plain.yaml")).delays == ()


def _set_units(*units):
    return lambda d: d.update(units=list(units))


def _to_station(unit="T-01", **pos):
    return {"unit_uuid": unit, "phase": "to_station",
            "position": pos or {"from_station": "S1", "distance_m": 1800, "toward": "U1"}}


ERROR_CASES = [
    ("unknown top key", lambda d: d.update(nmae="x"), "unknown key(s) nmae"),
    ("missing name", lambda d: d.pop("name"), "name must be a non-empty string"),
    ("missing start_ts", lambda d: d.pop("start_ts"), "start_ts is required"),
    ("naive start_ts", lambda d: d.update(start_ts="2026-09-15T12:00:00"), "has no timezone"),
    ("site and site_file", lambda d: d.update(site_file="x.yaml"), "exactly one of 'site'"),
    ("no site", lambda d: d.pop("site"), "exactly one of 'site'"),
    ("missing site file", lambda d: (d.pop("site"), d.update(site_file="nope.yaml")), "site_file not found"),
    ("three stations", lambda d: d["site"]["stations"].pop(), "invalid site: need at least 4 stations"),
    ("station at unload", lambda d: d["site"]["stations"][0].update(lat=CENTER[0], lon=CENTER[1]),
     "station S1 is 0.0 m from unload"),
    ("site key missing", lambda d: d["site"].pop("assignment"), "site.assignment is required"),
    ("site key unknown", lambda d: d["site"].update(units=[]), "site: unknown key(s) units"),
    ("assigned to unknown", lambda d: d["site"]["assignment"].update({"T-09": "S9"}), "unknown station S9"),
    ("units not list", lambda d: d.update(units={"T-01": "x"}), "units must be a list"),
    ("expect not mapping", lambda d: d.update(expect=[1]), "expect must be a mapping"),
    ("coord_precision str", lambda d: d.update(coord_precision="4"), "coord_precision must be an integer"),
    ("duration zero", lambda d: d.update(duration_s=0), "duration_s must be an integer >= 1"),
    ("maneuver negative", lambda d: d.update(maneuver_s=-1), "maneuver_s must be an integer >= 0"),
    ("unit not assigned", _set_units(_to_station("T-99")), "T-99): unit is not in the site assignment"),
    ("unit twice", _set_units(_to_station(), _to_station()), "unit listed twice"),
    ("unknown unit key", _set_units({**_to_station(), "speed": 1}), "unknown key(s) speed"),
    ("bad phase", _set_units({"unit_uuid": "T-01", "phase": "driving"}), "phase must be one of"),
    ("no position", _set_units({"unit_uuid": "T-01", "phase": "to_station"}), "position is required"),
    ("no position to_unload", _set_units({"unit_uuid": "T-01", "phase": "to_unload"}), "position is required"),
    ("waiting with position", _set_units({"unit_uuid": "T-01", "phase": "waiting", "lat": 49.0, "lon": 142.0}),
     "position not allowed for phase waiting"),
    ("elapsed on to_station", _set_units({**_to_station(), "service_elapsed_s": 5}),
     "service_elapsed_s not allowed for phase to_station"),
    ("target on to_unload", _set_units({"unit_uuid": "T-01", "phase": "to_unload", "target_station": "S1",
                                        "lat": 49.0, "lon": 142.0}), "target_station not allowed"),
    ("unknown target", _set_units({**_to_station(), "target_station": "S9"}), "unknown target_station 'S9'"),
    ("unknown from_station", _set_units(_to_station(from_station="S9", distance_m=10, toward="U1")),
     "unknown from_station 'S9'"),
    ("unknown toward", _set_units(_to_station(from_station="S1", distance_m=100, toward="U2")),
     "unknown toward point 'U2'"),
    ("toward self", _set_units(_to_station(from_station="S1", distance_m=100, toward="S1")), "must differ"),
    ("negative distance", _set_units(_to_station(from_station="S1", distance_m=-1, toward="U1")),
     "distance_m must be >= 0"),
    ("bool bearing", _set_units(_to_station(from_station="S1", distance_m=100, toward=True)), "finite number"),
    ("incomplete position", _set_units(_to_station(from_station="S1", distance_m=100)),
     "must be {lat, lon} or {from_station, distance_m, toward}"),
    ("position and lat", _set_units({**_to_station(), "lat": 49.0, "lon": 142.0}), "not both"),
    ("lat out of range", _set_units(_to_station(lat=91.0, lon=142.0)), "out of range"),
    ("to_station at the station", _set_units(_to_station(from_station="S1", distance_m=10, toward="U1")),
     "must be > 15 m"),
    ("two in service", _set_units({"unit_uuid": "T-01", "phase": "in_service"},
                                  {"unit_uuid": "T-05", "phase": "in_service", "target_station": "S1"}),
     "more than one unit in_service at station(s) S1"),
    ("enter after start", _set_units({"unit_uuid": "T-01", "phase": "waiting",
                                      "radius_enter_ts": T_1200 + 1}), "is after start_ts"),
    ("negative elapsed", _set_units({"unit_uuid": "T-01", "phase": "in_service", "service_elapsed_s": -1}),
     "integer >= 0"),
    ("elapsed >= service_s", lambda d: d.update(service_s=200, units=[
        {"unit_uuid": "T-01", "phase": "in_service", "service_elapsed_s": 200}]), "must be < service_s 200"),
    ("delays not a list", lambda d: d.update(delays={"unit_uuid": "T-01"}), "delays must be a list"),
    ("delay unknown key", lambda d: d.update(delays=[_delay(lag=1)]), "unknown key(s) lag"),
    ("delay missing key", lambda d: d.update(delays=[{"unit_uuid": "T-01", "from_ts": T_1200}]),
     "missing emit_at, to_ts"),
    ("delay unknown unit", lambda d: d.update(delays=[_delay(unit_uuid="T-99")]), "not in the site assignment"),
    ("delay before start", lambda d: d.update(delays=[_delay(from_ts=T_1200 - 1)]), "start_ts <= from_ts"),
    ("delay emit not after", lambda d: d.update(delays=[_delay(emit_at=T_1200 + 9)]), "to_ts < emit_at"),
    ("delay over 60 s", lambda d: d.update(delays=[_delay(emit_at=T_1200 + 61)]), "emit_at <= from_ts + 60"),
    ("delays overlap", lambda d: d.update(delays=[_delay(), _delay(from_ts=T_1200 + 9, emit_at=T_1200 + 20)]),
     "overlaps another delay of T-01"),
]


@pytest.mark.parametrize("mutate, message", [c[1:] for c in ERROR_CASES], ids=[c[0] for c in ERROR_CASES])
def test_validation_errors(tmp_path, mutate, message):
    doc = _doc()
    mutate(doc)
    with pytest.raises(ValueError, match=re.escape(message)):
        load_scenario(_write(tmp_path, doc))


def test_invalid_yaml(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("name: [unclosed\n")
    with pytest.raises(ValueError, match="invalid YAML"):
        load_scenario(path)
    path.write_text("- just a list\n")
    with pytest.raises(ValueError, match="must be a YAML mapping"):
        load_scenario(path)


# --- the task example -------------------------------------------------------------------------------------

OCCUPANCY_S = 230
SPEED = 10.0


def _plan(entries: list[tuple[str, int]]) -> list[dict]:
    """Service model: FIFO by eta, service_start = max(eta, previous free_at), occupancy 230 s."""
    rows, free = [], None
    for unit, eta in sorted(entries, key=lambda e: (e[1], e[0])):
        start = eta if free is None else max(eta, free)
        free = start + OCCUPANCY_S
        rows.append({"unit_uuid": unit, "eta": eta, "service_start": start, "free_at": free, "wait_s": start - eta})
    return rows


def _expected_rows(rows: list[dict]) -> list[dict]:
    return [{k: (parse_ts(v) if k in ("eta", "service_start", "free_at") else v) for k, v in r.items()}
            for r in rows]


class _Example:
    """Planned straight-line motion of the task example units (10 m/s toward the S1 center).

    UnitInit positions are the state at start_ts - 1, so the record at tick t is 10 * (t - start_ts + 1) m along.
    """

    def __init__(self) -> None:
        self.sc = load_scenario(EXAMPLE)
        self.proj = projection_for(self.sc.site)
        self.init = {u.unit_uuid: u for u in self.sc.initial}
        self.pts = {s.uuid: self.proj.to_xy(s.lat, s.lon) for s in self.sc.site.stations}
        u = self.sc.site.unload
        self.pts[u.uuid] = self.proj.to_xy(u.lat, u.lon)

    def pos(self, unit: str, t: int) -> tuple[float, float]:
        u = self.init[unit]
        x, y = self.proj.to_xy(u.lat, u.lon)
        tx, ty = self.pts["S1"]
        d = math.hypot(tx - x, ty - y)
        k = SPEED * (t - self.sc.start_ts + 1) / d
        return x + (tx - x) * k, y + (ty - y) * k

    def dist(self, unit: str, t: int, point: str) -> float:
        return dist_xy(self.pos(unit, t), self.pts[point])

    def eta(self, unit: str, t: int, station: str) -> int:
        return t + round(self.dist(unit, t, station) / SPEED)


@pytest.fixture(scope="module")
def example() -> _Example:
    return _Example()


def test_task_example_loads(example):
    sc = example.sc
    assert sc.name == "task_example"
    assert sc.start_ts == parse_ts("2026-09-15T11:59:00Z") == T_1200 - 60
    assert (sc.coord_precision, sc.service_s, sc.maneuver_s) == (6, 200, 30)
    assert sc.duration_s >= 900
    assert validate_site(sc.site, min_station_gap_m=500.0) == []
    assert sc.site.station_ids == ["S1", "S2", "S3", "S4"]
    assert dict(sc.site.assignment) == {"T1": "S1", "T2": "S1", "T3": "S1", "T4": "S1"}
    assert sc.initial[0] == UnitInit("T1", "in_service", service_elapsed_s=0)
    assert [(u.unit_uuid, u.phase, u.target_station) for u in sc.initial[1:]] == [
        ("T2", "to_station", None), ("T3", "to_station", None), ("T4", "to_station", None)]
    # T4's records of 11:59:01..11:59:59 arrive at 12:00:01, after its 12:00:00 record (the first one is 60 s late)
    assert sc.delays == (ScriptedDelay("T4", T_1200 - 59, T_1200 - 1, T_1200 + 1),)


def test_task_example_geometry(example):
    t0 = example.sc.start_ts
    t12 = t0 + 60
    for unit, d_start, d_noon in (("T2", 1802, 1202), ("T3", 3602, 3002), ("T4", 1502, 902)):
        assert example.dist(unit, t0 - 1, "S1") == pytest.approx(d_start + 10, abs=0.5)   # UnitInit position
        assert example.dist(unit, t0, "S1") == pytest.approx(d_start, abs=0.5)            # first record
        assert example.dist(unit, t12, "S1") == pytest.approx(d_noon, abs=0.5)
        # on the U1 -> S1 road
        assert example.dist(unit, t0, "U1") + example.dist(unit, t0, "S1") == pytest.approx(
            dist_xy(example.pts["U1"], example.pts["S1"]), abs=0.5)
    assert example.dist("T4", t12, "S2") == pytest.approx(2400.0, abs=0.5)
    last = example.sc.expect["t4_last_seen_before_1200"]
    assert parse_ts(last["at"]) == t0 and example.dist("T4", t0, "S1") == pytest.approx(last["S1"], abs=0.5)
    for unit, dists in example.sc.expect["distances_m"].items():
        if unit == "T1":
            continue
        for sid, d in dists.items():
            assert example.dist(unit, t12, sid) == pytest.approx(d, abs=0.5), (unit, sid)


def test_task_example_rounded_telemetry_crossings(example):
    """With 6-decimal coordinates and haversine, the 1500 m crossings fall on whole seconds without ambiguity:
    T4 is outside at 11:59:00 and 902 m away at 12:00:00, T2 crosses between 11:59:30 and 11:59:31."""
    st = {s.uuid: s for s in example.sc.site.stations}

    def seen(unit: str, t: int, sid: str) -> float:
        lat, lon = example.proj.to_latlon(*example.pos(unit, t))
        return _haversine(round(lat, 6), round(lon, 6), st[sid].lat, st[sid].lon)

    t12 = example.sc.start_ts + 60
    assert seen("T4", t12 - 60, "S1") > 1501.0
    assert seen("T4", t12, "S1") == pytest.approx(902.0, abs=0.1)
    assert seen("T4", t12, "S2") == pytest.approx(2400.0, abs=0.1)
    assert seen("T2", t12 - 30, "S1") > 1501.0
    assert seen("T2", t12 - 29, "S1") < 1499.0
    assert seen("T2", t12, "S1") == pytest.approx(1202.0, abs=0.1)
    assert seen("T3", t12, "S1") == pytest.approx(3002.0, abs=0.1)


def test_task_example_expected_values(example):
    exp = example.sc.expect
    t0 = example.sc.start_ts
    t12 = t0 + 60
    assert parse_ts(exp["snapshot_at"]) == t12
    assert exp["service_model"]["occupancy_s"] == OCCUPANCY_S

    # At 12:00:00 T4's last position is from 11:59:00 (60 s old): out of the queue. T1 occupies S1 since 11:59:00.
    s1_entries = [("T1", t0)] + [(u, example.eta(u, t12, "S1")) for u in ("T2", "T3")]
    s1 = _plan(s1_entries)
    assert _expected_rows(exp["queues"]["S1"]) == s1
    assert [r["wait_s"] for r in s1] == [0, 50, 100]                    # the task's table
    assert exp["queues"]["S2"] == exp["queues"]["S3"] == exp["queues"]["S4"] == []

    # T4's decision on its 12:00:00 record: wait at each station = release before T4's eta minus that eta
    dec = exp["decision"]
    eta = {sid: example.eta("T4", t12, sid) for sid in ("S1", "S2", "S3", "S4")}
    wait = {"S1": _plan(s1_entries + [("T4", eta["S1"])])[1]["wait_s"]}
    assert _plan(s1_entries + [("T4", eta["S1"])])[1]["unit_uuid"] == "T4"   # right after T1, ahead of T2
    for sid in ("S2", "S3", "S4"):
        wait[sid] = _plan([("T4", eta[sid])])[0]["wait_s"]
    best = min(("S2", "S3", "S4"), key=lambda s: (wait[s], eta[s]))
    gain = wait["S1"] - wait[best]
    assert (dec["unit_uuid"], parse_ts(dec["at"]), dec["from_station"]) == ("T4", t12, "S1")
    assert dec["wait_s"] == wait
    assert {k: parse_ts(v) for k, v in dec["eta"].items()} == eta
    assert (dec["to_station"], dec["gain_seconds"], dec["result"]) == (best, gain, "recommended")
    assert (wait["S1"], gain, best) == (80, 80, "S2")                  # the task's numbers
    assert (eta["S1"], eta["S2"]) == (t12 + 90, t12 + 240)            # 12:01:30, 12:04:00
    starts = {k: parse_ts(v) for k, v in dec["service_start"].items()}
    assert starts == {"S1": s1[0]["free_at"], "S2": eta["S2"]}

    # service_end metric: S1 ends first -> no_gain
    ends = {"S1": starts["S1"] + OCCUPANCY_S, **{sid: eta[sid] + OCCUPANCY_S for sid in ("S2", "S3", "S4")}}
    assert min(ends, key=ends.get) == "S1"
    assert exp["decision_service_end"] == {"unit_uuid": "T4", "at": dec["at"], "result": "rejected",
                                           "reason": "no_gain"}

    # applied: T4 moves to S2, S1 keeps the task's table; shadow: T4 joins S1 ahead of T2
    after = exp["queues_after_decision"]
    assert _expected_rows(after["S1"]) == s1
    assert _expected_rows(after["S2"]) == _plan([("T4", eta["S2"])])
    shadow = exp["queues_shadow"]
    assert _expected_rows(shadow["S1"]) == _plan(s1_entries + [("T4", eta["S1"])])
    assert shadow["S2"] == []

    # T2's own crossing at 11:59:31: T4 (last seen 11:59:00) is stale by then
    (t2,) = exp["earlier_decisions"]
    t_cross = parse_ts(t2["at"])
    assert t_cross - t0 > 30
    assert example.dist("T2", t_cross - 1, "S1") > 1500.0 > example.dist("T2", t_cross, "S1")
    s1_then = _plan([("T1", t0)] + [(u, example.eta(u, t_cross, "S1")) for u in ("T2", "T3")])
    t2_row = next(r for r in s1_then if r["unit_uuid"] == "T2")
    t2_eta = {"S1": t2_row["eta"], "S2": example.eta("T2", t_cross, "S2")}
    assert {k: parse_ts(v) for k, v in t2["eta"].items()} == t2_eta
    assert t2["wait_s"] == {"S1": t2_row["wait_s"], "S2": 0} == {"S1": 50, "S2": 0}
    assert t2_row["free_at"] < t2_eta["S2"] + OCCUPANCY_S              # by end of service S1 is better too
    assert (t2["result"], t2["reason"]) == ("rejected", "no_gain")


def test_task_example_generator_truth_arithmetic(example):
    """The documented ground truth follows the contract rules (stop 15 m before the center, maneuver 30 s,
    FIFO by radius entry: T4 arrives first)."""
    sc = example.sc
    truth = {u: {k: parse_ts(v) for k, v in ev.items()} for u, ev in sc.expect["generator_truth_open_loop"].items()}
    assert truth["T1"] == {"service_start": sc.start_ts, "depart": sc.start_ts + sc.service_s}
    release = truth["T1"]["depart"]
    for unit in ("T4", "T2", "T3"):
        arrive = sc.start_ts + math.ceil((example.dist(unit, sc.start_ts, "S1") - 15.0) / SPEED - 1e-6)
        start = max(arrive, release + sc.maneuver_s)
        assert truth[unit] == {"arrive": arrive, "service_start": start, "depart": start + sc.service_s}
        release = start + sc.service_s
    shadow = {r["unit_uuid"]: parse_ts(r["service_start"]) for r in sc.expect["queues_shadow"]["S1"]}
    assert shadow == {u: truth[u]["service_start"] for u in ("T1", "T4", "T2", "T3")}
