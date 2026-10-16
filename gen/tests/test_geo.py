import math
import random

import pytest

from generator.constants import EARTH_RADIUS_M
from generator.geo import Projection, dist_xy, projection_for
from generator.site import generate_site

LAT0, LON0 = 49.1288, 142.6601
KM_PER_DEG = 111.195  # 2*pi*R/360 for R = 6371 km


def test_origin_maps_to_zero():
    p = Projection(LAT0, LON0)
    assert p.to_xy(LAT0, LON0) == (0.0, 0.0)
    assert p.to_latlon(0.0, 0.0) == (LAT0, LON0)


def test_one_degree_of_latitude():
    p = Projection(LAT0, LON0)
    x, y = p.to_xy(LAT0 + 1.0, LON0)
    assert x == 0.0
    assert y / 1000 == pytest.approx(KM_PER_DEG, abs=0.001)
    assert y == pytest.approx(EARTH_RADIUS_M * math.pi / 180)
    assert p.distance_m(LAT0, LON0, LAT0 - 1.0, LON0) / 1000 == pytest.approx(KM_PER_DEG, abs=0.001)


def test_east_west_uses_cos_lat0():
    p = Projection(LAT0, LON0)
    x, y = p.to_xy(LAT0, LON0 + 1.0)
    assert y == 0.0
    assert x / 1000 == pytest.approx(KM_PER_DEG * math.cos(math.radians(LAT0)), abs=0.001)
    # cos(lat0) is fixed by the origin, not taken from the point's own latitude
    x_north, _ = p.to_xy(LAT0 + 0.1, LON0 + 1.0)
    assert x_north == pytest.approx(x)
    # at the equator a degree of longitude equals a degree of latitude
    assert Projection(0.0, 0.0).to_xy(0.0, 1.0)[0] / 1000 == pytest.approx(KM_PER_DEG, abs=0.001)


def test_axes_orientation():
    p = Projection(LAT0, LON0)
    x, y = p.to_xy(LAT0 + 0.01, LON0 + 0.01)
    assert x > 0 and y > 0  # north-east quadrant
    x, y = p.to_xy(LAT0 - 0.01, LON0 - 0.01)
    assert x < 0 and y < 0


def test_round_trip_is_exact():
    p = Projection(LAT0, LON0)
    rng = random.Random(1)
    for _ in range(2000):
        lat = LAT0 + rng.uniform(-0.2, 0.2)
        lon = LON0 + rng.uniform(-0.3, 0.3)
        lat2, lon2 = p.to_latlon(*p.to_xy(lat, lon))
        assert abs(lat2 - lat) < 1e-9 and abs(lon2 - lon) < 1e-9
        x, y = rng.uniform(-20000, 20000), rng.uniform(-20000, 20000)
        x2, y2 = p.to_xy(*p.to_latlon(x, y))
        assert abs(x2 - x) < 1e-6 and abs(y2 - y) < 1e-6


def test_distance_and_dist_xy():
    p = Projection(LAT0, LON0)
    assert dist_xy((0.0, 0.0), (3.0, 4.0)) == 5.0
    assert dist_xy((1.0, 1.0), (1.0, 1.0)) == 0.0
    a = (LAT0 + 0.01, LON0 - 0.02)
    b = (LAT0 - 0.03, LON0 + 0.01)
    d = p.distance_m(*a, *b)
    assert d == pytest.approx(p.distance_m(*b, *a))
    assert d == pytest.approx(dist_xy(p.to_xy(*a), p.to_xy(*b)))
    # 10 m step on the plane is 10 m in distance_m
    lat, lon = p.to_latlon(6.0, 8.0)
    assert p.distance_m(LAT0, LON0, lat, lon) == pytest.approx(10.0, abs=1e-6)


def test_projection_for_site_uses_center():
    site = generate_site(42)
    p = projection_for(site)
    assert (p.lat0, p.lon0) == (site.center_lat, site.center_lon)
    assert p.to_xy(site.unload.lat, site.unload.lon) == pytest.approx((0.0, 0.0), abs=1e-9)
