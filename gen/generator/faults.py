"""Seeded fault injection in simulation time: duplicates, per-record delay and per-unit lag episodes.

Every record pushed at tick `now` gets an emit_at >= now; `pop_due(now)` hands out everything due, ordered by
(emit_at, push sequence). Lateness (emit_at - ts) never exceeds MAX_LATENESS_S.

Randomness: each fault type has its own stream per unit (`"{seed}:faults:{type}:{unit}"`) plus one stream for the
within-tick shuffle. A stream draws a fixed number of values per record (or per unit tick for lag) whenever its fault
is enabled, so changing one fault's parameters never reshuffles the decisions of another fault, a missing unit does
not shift the draws of the others, and raising a probability keeps every fault the lower probability produced.
"""

from __future__ import annotations

import heapq
import math
import random
from collections.abc import Sequence
from dataclasses import dataclass

from .constants import MAX_LATENESS_S
from .model import Emission, ScriptedDelay, Telemetry


def _is_int(v: object) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_number(v: object) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


@dataclass(frozen=True)
class FaultConfig:
    dup_prob: float = 0.01          # probability a record is duplicated
    dup_max_delay_s: int = 10       # duplicate emitted U[0, dup_max_delay_s] s after the original
    late_prob: float = 0.02         # probability a record is individually delayed
    late_max_s: int = 60            # per-record delay U[1, late_max_s]
    lag_rate_per_hour: float = 2.0  # per unit: Poisson rate of connection-loss episodes
    lag_min_s: int = 10             # episode duration U[lag_min_s, lag_max_s]
    lag_max_s: int = 60             # records of the unit in the episode are released together at its end, in ts order
    shuffle_within_tick: bool = False

    def __post_init__(self) -> None:
        problems = self.problems()
        if problems:
            raise ValueError("invalid FaultConfig: " + "; ".join(problems))

    def problems(self) -> list[str]:
        out: list[str] = []
        for name in ("dup_prob", "late_prob"):
            v = getattr(self, name)
            if not (_is_number(v) and 0.0 <= v <= 1.0):
                out.append(f"{name} must be a number in [0, 1], got {v!r}")
        v = self.lag_rate_per_hour
        if not (_is_number(v) and 0.0 <= v <= 3600.0):
            out.append(f"lag_rate_per_hour must be a number in [0, 3600], got {v!r}")
        for name, lo in (("dup_max_delay_s", 0), ("late_max_s", 1), ("lag_min_s", 1), ("lag_max_s", 1)):
            v = getattr(self, name)
            if not (_is_int(v) and lo <= v <= MAX_LATENESS_S):
                out.append(f"{name} must be an int in [{lo}, {MAX_LATENESS_S}], got {v!r}")
        if _is_int(self.lag_min_s) and _is_int(self.lag_max_s) and self.lag_min_s > self.lag_max_s:
            out.append(f"lag_min_s ({self.lag_min_s}) must be <= lag_max_s ({self.lag_max_s})")
        if not isinstance(self.shuffle_within_tick, bool):
            out.append(f"shuffle_within_tick must be a bool, got {self.shuffle_within_tick!r}")
        return out

    @staticmethod
    def none() -> FaultConfig:
        """All faults off: every record is emitted on its own tick, in input order."""
        return FaultConfig(dup_prob=0.0, late_prob=0.0, lag_rate_per_hour=0.0, shuffle_within_tick=False)

    @property
    def active(self) -> bool:
        return self.dup_prob > 0 or self.late_prob > 0 or self.lag_rate_per_hour > 0 or self.shuffle_within_tick


def _uniform_int(u: float, lo: int, hi: int) -> int:
    """Map u in [0, 1) to an integer uniform on [lo, hi]."""
    return min(hi, lo + int(u * (hi - lo + 1)))


class _UnitState:
    __slots__ = ("lag_rng", "late_rng", "dup_rng", "lag_end")

    def __init__(self, base: str, unit_uuid: str) -> None:
        self.lag_rng = random.Random(f"{base}:lag:{unit_uuid}")
        self.late_rng = random.Random(f"{base}:late:{unit_uuid}")
        self.dup_rng = random.Random(f"{base}:dup:{unit_uuid}")
        self.lag_end: int | None = None      # exclusive end tick of the current/last lag episode


class FaultInjector:
    def __init__(self, cfg: FaultConfig, seed: int, scripted: Sequence[ScriptedDelay] = ()) -> None:
        self.cfg = cfg
        self.seed = seed
        for d in scripted:
            if not d.from_ts <= d.to_ts < d.emit_at <= d.from_ts + MAX_LATENESS_S:
                raise ValueError(f"scripted delay {d}: need from_ts <= to_ts < emit_at <= from_ts + {MAX_LATENESS_S}")
        self.scripted = tuple(scripted)
        self._base = f"{seed}:faults"
        self._lag_p = cfg.lag_rate_per_hour / 3600.0
        self._shuffle_rng = random.Random(f"{self._base}:shuffle")
        self._units: dict[str, _UnitState] = {}
        self._heap: list[tuple[int, int, Emission]] = []
        self._seq = 0
        self._last_push: int | None = None
        self._records_in = 0
        self._emitted = 0
        self._duplicates = 0
        self._delayed = 0
        self._lag_episodes = 0
        self._held_in_lag = 0
        self._scripted = 0
        self._max_lateness = 0

    def _unit(self, unit_uuid: str) -> _UnitState:
        st = self._units.get(unit_uuid)
        if st is None:
            st = self._units[unit_uuid] = _UnitState(self._base, unit_uuid)
        return st

    def push(self, now: int, records: Sequence[Telemetry]) -> None:
        """Schedule the records produced at tick `now` (all with ts == now; units may be missing)."""
        if self._last_push is not None and now < self._last_push:
            raise ValueError(f"push(now={now}) after push(now={self._last_push}): time must not go backwards")
        self._last_push = now
        cfg = self.cfg
        on_time: list[Emission] = []
        later: list[Emission] = []
        dups: list[Emission] = []
        for rec in records:
            if rec.ts != now:
                raise ValueError(f"record {rec.unit_uuid} has ts={rec.ts}, expected the push tick {now}")
            self._records_in += 1
            st = self._unit(rec.unit_uuid)
            cap = rec.ts + MAX_LATENESS_S
            emit_at = now
            held = False

            if self._lag_p > 0:
                u, v = st.lag_rng.random(), st.lag_rng.random()
                in_lag = st.lag_end is not None and now < st.lag_end
                if not in_lag and u < self._lag_p:
                    st.lag_end = now + _uniform_int(v, cfg.lag_min_s, cfg.lag_max_s)
                    self._lag_episodes += 1
                    in_lag = True
                if in_lag:
                    emit_at = st.lag_end
                    held = True
                    self._held_in_lag += 1

            if cfg.late_prob > 0:
                u, v = st.late_rng.random(), st.late_rng.random()
                if not held and u < cfg.late_prob:
                    emit_at = now + _uniform_int(v, 1, cfg.late_max_s)
                    self._delayed += 1

            for d in self.scripted:          # scenario faults win over the random ones (draws are kept)
                if d.covers(rec.unit_uuid, rec.ts):
                    emit_at = d.emit_at
                    self._scripted += 1
                    break

            emit_at = min(emit_at, cap)
            em = Emission(emit_at, rec.unit_uuid, rec.to_dict())
            (on_time if emit_at == rec.ts else later).append(em)

            if cfg.dup_prob > 0:
                u, v = st.dup_rng.random(), st.dup_rng.random()
                if u < cfg.dup_prob:
                    dup_at = min(emit_at + _uniform_int(v, 0, cfg.dup_max_delay_s), cap)
                    dups.append(Emission(dup_at, rec.unit_uuid, rec.to_dict()))
                    self._duplicates += 1

        if cfg.shuffle_within_tick and len(on_time) > 1:
            self._shuffle_rng.shuffle(on_time)
        # Sequence order: on-time originals, then held/delayed originals, then duplicates (a duplicate with the same
        # emit_at as its original always follows it).
        for group in (on_time, later, dups):
            for em in group:
                heapq.heappush(self._heap, (em.emit_at, self._seq, em))
                self._seq += 1

    def _pop(self) -> Emission:
        em = heapq.heappop(self._heap)[2]
        self._emitted += 1
        lateness = em.emit_at - em.value["ts"]
        if lateness > self._max_lateness:
            self._max_lateness = lateness
        return em

    def pop_due(self, now: int) -> list[Emission]:
        """Everything with emit_at <= now, ordered by (emit_at, push sequence)."""
        heap = self._heap
        out: list[Emission] = []
        while heap and heap[0][0] <= now:
            out.append(self._pop())
        return out

    def drain(self) -> list[Emission]:
        """Everything still pending (shutdown), in the same order pop_due would have produced it."""
        out: list[Emission] = []
        while self._heap:
            out.append(self._pop())
        return out

    @property
    def pending(self) -> int:
        return len(self._heap)

    @property
    def stats(self) -> dict[str, int]:
        return {
            "records_in": self._records_in,
            "emitted": self._emitted,
            "duplicates": self._duplicates,
            "delayed": self._delayed,
            "lag_episodes": self._lag_episodes,
            "held_in_lag": self._held_in_lag,
            "scripted": self._scripted,
            "max_lateness": self._max_lateness,
            "pending": len(self._heap),
        }
