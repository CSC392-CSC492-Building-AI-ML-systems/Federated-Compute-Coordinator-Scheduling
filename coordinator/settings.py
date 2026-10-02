"""Coordinator settings: the fields live here, the values live in config/coordinator.yaml."""

from pathlib import Path
from typing import Union

import yaml
from pydantic import BaseModel, ConfigDict, Field

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "coordinator.yaml"


class Settings(BaseModel):
    # No defaults on purpose: every value must come from the YAML file (or a test),
    # so there is one place to look. Typos and unknown keys fail at startup.
    model_config = ConfigDict(extra="forbid", frozen=True)

    heartbeat_timeout_seconds: float = Field(gt=0)
    monitor_interval_seconds: float = Field(gt=0)


def load_settings(path: Union[str, Path] = DEFAULT_CONFIG_PATH) -> Settings:
    """Read and validate a settings YAML file."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return Settings(**data)
