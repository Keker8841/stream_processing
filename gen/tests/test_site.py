import itertools
from dataclasses import replace

import pytest
import yaml

from generator.constants import DECISION_RADIUS_M
from generator.geo import projection_for
from generator.model import Site, Station
from generator.site import (generate_site, load_site, pairwise_distances, save_site, unload_distances,
                            validate_site)


def station_dists(site: Site) -> tuple[list[float], list[float]]:
    p = projection_for(site)
    u = site.unload
    to_unload = [p.distance_m(s.lat, s.lon, u.lat, u.lon) for s in site.stations]
    pairwise = [p.distance_m(a.lat, a.lon, b.lat, b.lon) for a, b in itertools.combinations(site.stations, 2)]
    return to_unload, pairwise


def test_deterministic_for_seed():
    assert generate_site(42) == generate_site(42)
    assert hash(generate_site(42)) == hash(generate_site(42))
    assert generate_site(42) != generate_site(43)


def test_default_layout():
    site = generate_site(42)
    assert (site.center_lat, site.center_lon) == (49.1288, 142.6601)
    assert (site.unload.lat, site.unload.lon) == (site.center_lat, site.center_lon)
    assert site.station_ids == ["S1", "S2", "S3", "S4"]
    assert site.unit_ids == [f"T-{i:02d}" for i in range(1, 41)]
    for i, uid in enumerate(site.unit_ids):
        assert site.assignment[uid] == f"S{i // 10 + 1}"
    assert validate_site(site, min_station_gap_m=500.0) == []


@pytest.mark.parametrize("seed", range(30))
def test_constraints_hold(seed):
    site = generate_site(seed)
    to_unload, pairwise = station_dists(site)
    assert all(3000.0 - 1.0 <= d <= 10000.0 + 1.0 for d in to_unload)  # rounding to 6 decimals ~ 0.1 m
    assert all(d > DECISION_RADIUS_M for d in to_unload)
    assert min(pairwise) >= 800.0                                       # stations of one pit: 0.8 .. 3.5 km apart
    assert max(pairwise) <= 3500.0
    assert max(to_unload) - min(to_unload) >= 1000.0                    # distances to unload differ noticeably
    for s in site.stations:  # stored at 6 decimals
        assert round(s.lat, 6) == s.lat and round(s.lon, 6) == s.lon


def test_custom_split_and_params():
    site = generate_site(7, split=(5, 15, 0, 10, 2), unload_dist_m=(2000.0, 4000.0), min_station_gap_m=800.0)
    assert site.station_ids == ["S1", "S2", "S3", "S4", "S5"]
    counts = {sid: sum(1 for v in site.assignment.values() if v == sid) for sid in site.station_ids}
    assert counts == {"S1": 5, "S2": 15, "S3": 0, "S4": 10, "S5": 2}
    assert site.assignment["T-01"] == "S1" and site.assignment["T-06"] == "S2" and site.assignment["T-32"] == "S5"
    to_unload, pairwise = station_dists(site)
    assert all(1999.0 <= d <= 4001.0 for d in to_unload)
    assert min(pairwise) >= 800.0


def test_pit_cluster_shape():
    site = generate_site(42)
    to_unload = unload_distances(site)
    pairwise = pairwise_distances(site)
    assert sorted(to_unload) == ["S1", "S2", "S3", "S4"] and len(pairwise) == 6
    # all stations within one pit of radius 1500 m: no pair farther apart than the pit diameter
    assert max(pairwise.values()) <= 3000.0 + 1.0
    # the pit is far from unload compared to its own size
    assert min(to_unload.values()) > max(pairwise.values())
    assert validate_site(site, min_station_gap_m=800.0, max_station_gap_m=3500.0) == []


def test_pit_params():
    site = generate_site(3, pit_radius_m=1000.0, min_station_gap_m=600.0, max_station_gap_m=2000.0,
                         min_unload_spread_m=500.0)
    pairwise = pairwise_distances(site).values()
    assert 600.0 <= min(pairwise) and max(pairwise) <= 2000.0
    spread = unload_distances(site).values()
    assert max(spread) - min(spread) >= 500.0


def test_validate_max_gap():
    site = generate_site(42)
    far = max(pairwise_distances(site).values())
    assert validate_site(site, max_station_gap_m=far + 1.0) == []
    assert any("must be <=" in m for m in validate_site(site, max_station_gap_m=far - 1.0))


def test_wide_unit_ids_stay_sortable():
    site = generate_site(1, split=(30, 30, 30, 30))
    assert site.unit_ids[0] == "T-001" and site.unit_ids[-1] == "T-120"
    assert site.unit_ids == sorted(site.unit_ids, key=lambda u: int(u[2:]))


@pytest.mark.parametrize("kwargs", [
    {"unload_dist_m": (500.0, 1400.0)},           # every station inside the decision radius
    {"min_station_gap_m": 50_000.0},              # gap larger than the annulus allows
    {"split": (10, 10, 10)},                      # fewer than 4 stations
    {"unload_dist_m": (5000.0, 3000.0)},
    {"min_station_gap_m": 900.0, "max_station_gap_m": 800.0},
    {"pit_radius_m": 300.0},                      # pit diameter < default min gap
    {"pit_radius_m": 0.0},
    {"min_unload_spread_m": 8000.0},              # more than the unload_dist_m range
    {"split": (10,) * 30},                        # 30 stations do not fit into one pit with 800 m gaps
])
def test_impossible_constraints_raise(kwargs):
    with pytest.raises(ValueError):
        generate_site(42, **kwargs)


def test_validate_reports_problems():
    site = generate_site(42)
    s1, s2, s3, s4 = site.stations
    p = projection_for(site)

    near_unload = replace(site, stations=(replace(s1, lat=site.center_lat + 0.01, lon=site.center_lon), s2, s3, s4))
    assert any("S1" in m and "unload" in m for m in validate_site(near_unload))

    lat, lon = p.to_latlon(*(a + 60.0 for a in p.to_xy(s1.lat, s1.lon)))  # ~85 m from S1
    close = replace(site, stations=(s1, Station("S2", lat, lon), s3, s4))
    assert any("S1 and S2" in m for m in validate_site(close))
    lat, lon = p.to_latlon(*(a + 250.0 for a in p.to_xy(s1.lat, s1.lon)))  # ~354 m from S1
    medium = replace(site, stations=(s1, Station("S2", lat, lon), s3, s4))
    assert validate_site(medium) == []
    assert any("S1 and S2" in m for m in validate_site(medium, min_station_gap_m=500.0))

    dup = replace(site, stations=(s1, s2, s3, replace(s4, uuid="S3")))
    assert any("duplicate" in m for m in validate_site(dup))

    three = replace(site, stations=(s1, s2, s3), assignment={"T-01": "S1"})
    assert any("at least 4" in m for m in validate_site(three))

    bad_assign = replace(site, assignment={**site.assignment, "T-41": "S9"})
    assert validate_site(bad_assign) == ["unit T-41 assigned to unknown station S9"]

    assert any("no units" in m for m in validate_site(replace(site, assignment={})))


def test_yaml_round_trip(tmp_path):
    site = generate_site(42)
    path = tmp_path / "site.yaml"
    save_site(site, path)
    loaded = load_site(path)
    assert loaded == site
    assert hash(loaded) == hash(site)
    path2 = tmp_path / "site2.yaml"
    save_site(loaded, path2)
    assert path2.read_text() == path.read_text()


def test_yaml_round_trip_rounds_to_6_decimals(tmp_path):
    site = generate_site(42)
    s1, *rest = site.stations
    precise = replace(site, stations=(replace(s1, lat=s1.lat + 1.23456789e-7, lon=s1.lon - 9.87654321e-7), *rest))
    a, b = tmp_path / "a.yaml", tmp_path / "b.yaml"
    save_site(precise, a)
    once = load_site(a)
    save_site(once, b)
    assert load_site(b) == once
    assert once.station("S1") == Station("S1", round(s1.lat + 1.23456789e-7, 6), round(s1.lon - 9.87654321e-7, 6))


def test_yaml_layout(tmp_path):
    path = tmp_path / "site.yaml"
    save_site(generate_site(42), path)
    text = path.read_text()
    lines = text.splitlines()
    assert lines[0] == "center: {lat: 49.1288, lon: 142.6601}"
    assert lines[1] == "unload: {uuid: U1, lat: 49.1288, lon: 142.6601}"
    assert lines[2] == "stations:"
    assert lines[3].startswith("  - {uuid: S1, lat: ")
    assert "assignment:" in lines
    assert "  T-01: S1" in lines and "  T-40: S4" in lines
    doc = yaml.safe_load(text)
    assert list(doc) == ["center", "unload", "stations", "assignment"]


def test_repo_site_yaml_matches_seed_42():
    from pathlib import Path
    path = Path(__file__).resolve().parents[2] / "site.yaml"
    if not path.exists():
        pytest.skip("site.yaml not generated")
    assert load_site(path) == generate_site(42)
