"""service.yaml loading: defaults, relative site_file, strict keys and types."""

from pathlib import Path

import pytest

from vqservice.config import EngineConfig, KafkaConfig, load_config

SERVICE_YAML = Path(__file__).resolve().parents[1] / "service.yaml"


def _write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "conf" / "service.yaml"
    p.parent.mkdir(exist_ok=True)
    p.write_text(text, encoding="utf-8")
    return p


def test_example_config_loads_with_task_constants():
    cfg = load_config(SERVICE_YAML)
    assert cfg.engine == EngineConfig() and cfg.engine.occupancy_s == 230
    assert cfg.kafka == KafkaConfig()
    assert cfg.site_file == (SERVICE_YAML.parent / "../site.yaml").resolve()


def test_defaults_relative_site_file_and_overrides(tmp_path):
    cfg = load_config(_write(tmp_path, "site_file: ../site.yaml\n"
                                       "engine: {speed_mps: 12, metric: service_end, apply_decisions: false}\n"
                                       "kafka: {bootstrap: 'k:9092'}\n"))
    assert cfg.site_file == (tmp_path / "site.yaml").resolve()
    assert cfg.engine == EngineConfig(speed_mps=12.0, metric="service_end", apply_decisions=False)
    assert isinstance(cfg.engine.speed_mps, float)
    assert cfg.kafka.bootstrap == "k:9092" and cfg.kafka.commit_max_messages == 500


@pytest.mark.parametrize("text, match", [
    ("site_file: s.yaml\nextra: 1\n", "unknown key"),
    ("site_file: s.yaml\nengine: {servic_s: 200}\n", "unknown key"),
    ("site_file: s.yaml\nkafka: {topic: x}\n", "unknown key"),
    ("engine: {}\n", "site_file"),
    ("site_file: s.yaml\nengine: {service_s: true}\n", "must be int"),
    ("site_file: s.yaml\nengine: {horizon_s: 1800.5}\n", "must be int"),
    ("site_file: s.yaml\nengine: {apply_decisions: 1}\n", "must be bool"),
    ("site_file: s.yaml\nengine: {metric: fastest}\n", "metric"),
    ("site_file: s.yaml\nengine: [1, 2]\n", "mapping"),
])
def test_invalid_config_is_rejected(tmp_path, text, match):
    with pytest.raises(ValueError, match=match):
        load_config(_write(tmp_path, text))
