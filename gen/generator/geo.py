"""Local equirectangular projection shared by the whole simulation (meters, x east, y north)."""

from __future__ import annotations

import math

from .constants import EARTH_RADIUS_M
from .model import Site


class Projection:
    def __init__(self, lat0: float, lon0: float) -> None:
        self.lat0 = lat0
        self.lon0 = lon0
        self._cos0 = math.cos(math.radians(lat0))

    def to_xy(self, lat: float, lon: float) -> tuple[float, float]:
        x = EARTH_RADIUS_M * math.radians(lon - self.lon0) * self._cos0
        y = EARTH_RADIUS_M * math.radians(lat - self.lat0)
        return x, y

    def to_latlon(self, x: float, y: float) -> tuple[float, float]:
        lat = self.lat0 + math.degrees(y / EARTH_RADIUS_M)
        lon = self.lon0 + math.degrees(x / (EARTH_RADIUS_M * self._cos0))
        return lat, lon

    def distance_m(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        return dist_xy(self.to_xy(lat1, lon1), self.to_xy(lat2, lon2))


def projection_for(site: Site) -> Projection:
    return Projection(site.center_lat, site.center_lon)


def dist_xy(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.hypot(a[0] - b[0], a[1] - b[1])
