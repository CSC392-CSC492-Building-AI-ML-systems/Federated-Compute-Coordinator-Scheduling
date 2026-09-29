"""Harness scaffold checks only. Scenario checks run through harness.runner, not CI."""

import importlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULES = (
    "client",
    "provider",
    "runner",
    "behaviors.liveness",
    "behaviors.lease",
    "behaviors.execution",
    "checks.invariants",
    "checks.liveness",
    "checks.lease",
    "checks.execution",
)


def test_harness_modules_exist():
    for name in MODULES:
        assert (ROOT / "harness" / (name.replace(".", "/") + ".py")).is_file(), name


def test_harness_modules_import():
    for name in MODULES:
        importlib.import_module(f"harness.{name}")
