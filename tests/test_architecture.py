"""Scaffold checks only. Add behavioral tests when features are implemented."""

import importlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULES = (
    "app",
    "store",
    "scheduler",
    "monitor",
    "api.providers",
    "api.jobs",
    "api.leases",
    "api.status",
    "services.provider_service",
    "services.job_service",
    "services.lease_service",
    "policies.health",
    "policies.retry",
    "policies.matching",
    "models.provider",
    "models.job",
    "models.lease",
)


def test_architecture_modules_exist():
    for name in MODULES:
        assert (ROOT / "coordinator" / (name.replace(".", "/") + ".py")).is_file(), name


def test_modules_import_without_starting_business_work():
    for name in MODULES:
        importlib.import_module(f"coordinator.{name}")
