"""Service configuration: engine constants, Kafka settings and the site file (service.yaml)."""

from __future__ import annotations

from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

import yaml

METRICS = ("queue_wait", "service_end")


@dataclass(frozen=True)
class EngineConfig:
    service_s: int = 200
    maneuver_s: int = 30
    speed_mps: float = 10.0
    station_radius_m: float = 50.0
    unload_radius_m: float = 50.0
    standing_kmh: float = 1.0
    decision_radius_m: float = 1500.0
    horizon_s: int = 1800
    fresh_s: int = 30
    min_gain_s: int = 60
    metric: str = "queue_wait"          # "queue_wait" | "service_end"
    apply_decisions: bool = True        # False = shadow mode

    def __post_init__(self) -> None:
        if self.metric not in METRICS:
            raise ValueError(f"metric must be one of {', '.join(METRICS)}, got {self.metric!r}")
        if self.speed_mps <= 0:
            raise ValueError(f"speed_mps must be > 0, got {self.speed_mps}")

    @property
    def occupancy_s(self) -> int:
        return self.service_s + self.maneuver_s


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap: str = "localhost:9092"
    telemetry_topic: str = "telemetry.v1"
    queue_topic: str = "queue.v1"
    decision_topic: str = "decision.v1"
    state_topic: str = "vq.state.v1"
    group_id: str = "vq-service"
    transactional_id: str = "vq-service"
    state_key: str = "vq-service"
    commit_interval_ms: int = 100
    commit_max_messages: int = 500


@dataclass(frozen=True)
class ServiceConfig:
    site_file: Path
    engine: EngineConfig
    kafka: KafkaConfig


def _section(cls: type, d: Any, name: str) -> Any:
    """Build a config dataclass from a mapping; unknown keys and wrong types -> ValueError (int ok for float)."""
    if d is None:
        d = {}
    if not isinstance(d, dict):
        raise ValueError(f"{name} must be a mapping")
    known = {f.name: type(f.default) for f in fields(cls)}
    unknown = sorted(map(str, d.keys() - known.keys()))
    if unknown:
        raise ValueError(f"{name}: unknown key(s) {', '.join(unknown)}")
    kw = {}
    for key, v in d.items():
        want = known[key]
        if want is float and type(v) is int:
            v = float(v)
        if type(v) is not want:
            raise ValueError(f"{name}.{key} must be {want.__name__}, got {v!r}")
        kw[key] = v
    return cls(**kw)


def load_config(path: str | Path) -> ServiceConfig:
    """YAML `site_file` (relative to the config file), `engine: {...}`, `kafka: {...}`."""
    path = Path(path)
    doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(doc, dict):
        raise ValueError(f"{path}: config must be a mapping")
    unknown = sorted(map(str, doc.keys() - {"site_file", "engine", "kafka"}))
    if unknown:
        raise ValueError(f"{path}: unknown key(s) {', '.join(unknown)}")
    site_file = doc.get("site_file")
    if not isinstance(site_file, str) or not site_file:
        raise ValueError(f"{path}: site_file (string) is required")
    return ServiceConfig(site_file=(path.parent / site_file).resolve(),
                         engine=_section(EngineConfig, doc.get("engine"), "engine"),
                         kafka=_section(KafkaConfig, doc.get("kafka"), "kafka"))
