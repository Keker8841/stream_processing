import copy
import dataclasses
import json
import pickle

import pytest

from generator.model import (TRUTH_EVENTS, UNIT_PHASES, FrozenMap, Site, Station, Telemetry, TruthEvent,
                             UnitInit, UnloadPoint)


def small_site(assignment=None) -> Site:
    return Site(
        center_lat=49.0, center_lon=142.0,
        unload=UnloadPoint("U1", 49.0, 142.0),
        stations=(Station("S2", 49.05, 142.0), Station("S1", 48.95, 142.0)),
        assignment=assignment if assignment is not None else {"T-02": "S2", "T-01": "S1"},
    )


def test_site_normalized_immutable_and_hashable():
    site = small_site()
    assert site.station_ids == ["S1", "S2"]
    assert list(site.assignment) == ["T-01", "T-02"]
    assert site.unit_ids == ["T-01", "T-02"]
    assert site.station("S2") == Station("S2", 49.05, 142.0)
    assert site.station_of("T-01").uuid == "S1"
    with pytest.raises(KeyError):
        site.station("S9")
    with pytest.raises(TypeError):
        site.assignment["T-01"] = "S2"  # type: ignore[index]
    with pytest.raises(dataclasses.FrozenInstanceError):
        site.center_lat = 0.0  # type: ignore[misc]
    other = small_site({"T-01": "S1", "T-02": "S2"})
    assert site == other and hash(site) == hash(other)
    assert len({site, other}) == 1
    assert site.assignment == {"T-01": "S1", "T-02": "S2"}


def test_site_copy_pickle_asdict():
    site = small_site()
    assert copy.deepcopy(site) == site
    assert pickle.loads(pickle.dumps(site)) == site
    assert dataclasses.replace(site, assignment={"T-01": "S2"}).assignment == {"T-01": "S2"}
    assert dataclasses.asdict(site)["assignment"] == {"T-01": "S1", "T-02": "S2"}


def test_frozen_map():
    m = FrozenMap({"b": "2", "a": "1"})
    assert list(m.items()) == [("a", "1"), ("b", "2")]
    assert "a" in m and "z" not in m and m.get("z") is None
    assert m == FrozenMap([("a", "1"), ("b", "2")]) and hash(m) == hash(FrozenMap(m))


def test_telemetry_dict_round_trip():
    t = Telemetry("T-12", 1789473600, 49.1288, 142.6601, 36.0)
    d = t.to_dict()
    assert list(d) == ["unit_uuid", "ts", "lat", "lon", "speed_kmh"]
    assert json.dumps(d, separators=(",", ":")) == \
        '{"unit_uuid":"T-12","ts":1789473600,"lat":49.1288,"lon":142.6601,"speed_kmh":36.0}'
    assert Telemetry.from_dict(json.loads(json.dumps(d))) == t


def test_truth_event_dict_omits_none():
    e = TruthEvent(5, "T-01", "unload")
    assert e.to_dict() == {"ts": 5, "unit_uuid": "T-01", "event": "unload"}
    e = TruthEvent(6, "T-01", "redirect_applied", detail={"from_station": "S1", "to_station": "S2"})
    assert list(e.to_dict()) == ["ts", "unit_uuid", "event", "detail"]
    e = TruthEvent(7, "T-01", "depart", "S1")
    assert e.to_dict() == {"ts": 7, "unit_uuid": "T-01", "event": "depart", "station_uuid": "S1"}
    assert TruthEvent.from_dict(e.to_dict()) == e
    assert set(TRUTH_EVENTS) >= {"radius_enter", "service_start", "depart", "radius_exit", "unload"}


def test_unit_init_defaults():
    u = UnitInit("T-01", "waiting")
    assert (u.target_station, u.lat, u.lon, u.service_elapsed_s, u.radius_enter_ts) == (None, None, None, 0, None)
    assert UNIT_PHASES == ("to_station", "waiting", "in_service", "to_unload")
