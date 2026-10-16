"""Wall-clock pacing of the simulation against absolute deadlines."""

from __future__ import annotations

import time
from collections.abc import Callable


class Pacer:
    """Maps simulation offsets (seconds since start) to wall-clock deadlines. speed <= 0 means no sleeping."""

    def __init__(self, speed: float, *, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.speed = float(speed)
        self._clock = clock
        self._sleep = sleep
        self.t0 = clock()

    def wait_until(self, sim_offset_s: float) -> None:
        """Sleep until t0 + sim_offset_s / speed. Returns immediately when already past the deadline."""
        if self.speed <= 0:
            return
        delay = self.t0 + sim_offset_s / self.speed - self._clock()
        if delay > 0:
            self._sleep(delay)

    def behind_s(self, sim_offset_s: float) -> float:
        """How far (wall seconds) the caller is behind the deadline of sim_offset_s; 0 when on time or unpaced."""
        if self.speed <= 0:
            return 0.0
        return max(0.0, self._clock() - (self.t0 + sim_offset_s / self.speed))
