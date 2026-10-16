"""Integer epoch seconds <-> ISO-8601 UTC (YYYY-MM-DDTHH:MM:SSZ), and the rounding used for ETAs."""

from __future__ import annotations

import calendar
import math
import time
from datetime import datetime, timezone


def iso(ts: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def parse_iso(s: str) -> int:
    """ISO-8601 with an offset or 'Z' (naive = UTC) -> epoch seconds (fractions dropped)."""
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return calendar.timegm(dt.astimezone(timezone.utc).timetuple())


def round_half_up(x: float) -> int:
    """floor(x + 0.5): 0.5 -> 1, 1.5 -> 2 (Python's round() would give 0 and 2)."""
    return math.floor(x + 0.5)
