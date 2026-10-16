"""Site loading in the generator's site.yaml format."""

import pytest

from generator.site import generate_site, save_site
from vqservice.site import Point, Site, load_site, site_from_dict

DOC = {
    "center": {"lat": 49.0, "lon": 142.0},
    "unload": {"uuid": "U1", "lat": 49.0, "lon": 142.0},
    "stations": [{"uuid": "S2", "lat": 49.01, "lon": 142.0}, {"uuid": "S1", "lat": 49.02, "lon": 142.0}],
    "assignment": {"T-02": "S2", "T-01": "S1"},
}


def test_site_from_dict_sorts_and_freezes():
    site = site_from_dict(DOC)
    assert [s.uuid for s in site.stations] == ["S1", "S2"]
    assert list(site.assignment.items()) == [("T-01", "S1"), ("T-02", "S2")]
    assert site.unload == Point("U1", 49.0, 142.0)
    assert site.station("S2") == Point("S2", 49.01, 142.0)
    with pytest.raises(KeyError):
        site.station("S9")
    with pytest.raises(TypeError):
        site.assignment["T-03"] = "S1"          # type: ignore[index]


def test_generator_site_file_round_trip(tmp_path):
    g = generate_site(42)
    save_site(g, tmp_path / "site.yaml")
    site = load_site(tmp_path / "site.yaml")
    assert site.unload == Point(g.unload.uuid, g.unload.lat, g.unload.lon)
    assert site.stations == tuple(Point(s.uuid, s.lat, s.lon) for s in g.stations)
    assert dict(site.assignment) == dict(g.assignment) and len(site.assignment) == 40


@pytest.mark.parametrize("change, match", [
    ({"assignment": {"T-01": "S9"}}, "unknown stations"),
    ({"stations": [{"uuid": "S1", "lat": 1, "lon": 2}] * 2}, "unique"),
    ({"stations": []}, "unique"),
    ({"unload": {"uuid": "U1", "lat": 49.0}}, "malformed"),
    ({"extra": 1}, "unknown key"),
])
def test_invalid_site_is_rejected(change, match):
    with pytest.raises(ValueError, match=match):
        site_from_dict({**DOC, **change})


def test_load_site_names_the_file(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("unload: {uuid: U1}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="bad.yaml"):
        load_site(p)
    assert isinstance(site_from_dict(DOC), Site)
