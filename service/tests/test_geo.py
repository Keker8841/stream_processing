"""Distance, time formatting and rounding helpers."""

import math

import pytest

from vqservice.geo import EARTH_RADIUS_M, haversine_m
from vqservice.timefmt import iso, parse_iso, round_half_up


def test_haversine():
    assert haversine_m(49.0, 142.0, 49.0, 142.0) == 0.0
    assert haversine_m(49.0, 142.0, 50.0, 142.0) == pytest.approx(EARTH_RADIUS_M * math.pi / 180, rel=1e-12)
    assert haversine_m(49.0, 142.0, 49.01, 142.02) == haversine_m(49.01, 142.02, 49.0, 142.0)
    # one degree of longitude shrinks with cos(latitude)
    assert haversine_m(60.0, 0.0, 60.0, 0.001) == pytest.approx(EARTH_RADIUS_M * math.radians(0.001) * 0.5, rel=1e-6)


def test_iso_round_trip():
    assert iso(1789473600) == "2026-09-15T12:00:00Z"
    assert parse_iso("2026-09-15T12:00:00Z") == 1789473600
    assert parse_iso("2026-09-15T15:00:00+03:00") == 1789473600
    assert parse_iso("2026-09-15T12:00:00") == 1789473600          # naive = UTC
    assert all(parse_iso(iso(t)) == t for t in (0, 1789473599, 1789473601, 2_000_000_000))


@pytest.mark.parametrize("x, n", [(0.5, 1), (1.5, 2), (2.5, 3), (149.5, 150), (149.49, 149), (-0.5, 0), (120.0, 120)])
def test_round_half_up(x, n):
    assert round_half_up(x) == n and isinstance(round_half_up(x), int)
