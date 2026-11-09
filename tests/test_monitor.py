"""Monitor skeleton: tick order, loop resilience, and lifespan start/stop.

The individual checks have their own service tests; here they are replaced by fakes.
"""

import asyncio
from datetime import datetime, timezone

import pytest
import yaml
from fastapi.testclient import TestClient

from coordinator import monitor
from coordinator.app import create_app
from coordinator.services import lease_service, provider_service
from coordinator.settings import DEFAULT_CONFIG_PATH, load_settings

NOW = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)

# Start from the repo config and override only what these tests care about, so
# adding a new setting does not break them.
SETTINGS = load_settings().model_copy(
    update={
        "heartbeat_timeout_seconds": 15.0,
        "monitor_interval_seconds": 0.01,
        "scheduler_interval_seconds": 0.01,
    }
)


def test_tick_runs_provider_checks_before_lease_timeouts(monkeypatch):
    calls = []

    async def fake_expire(store, now, timeout_seconds, max_attempts):
        calls.append(("expire_heartbeats", now, timeout_seconds, max_attempts))

    async def fake_finish(store, now, max_attempts):
        calls.append(("finish_drains", now, max_attempts))

    async def fake_offers(store, now, max_attempts, offer_timeouts_before_exclude):
        calls.append(("expire_offers", now, max_attempts, offer_timeouts_before_exclude))

    async def fake_execution(store, now, max_attempts):
        calls.append(("expire_execution_leases", now, max_attempts))

    monkeypatch.setattr(provider_service, "expire_heartbeats", fake_expire)
    monkeypatch.setattr(provider_service, "finish_drains", fake_finish)
    monkeypatch.setattr(lease_service, "expire_offers", fake_offers)
    monkeypatch.setattr(lease_service, "expire_execution_leases", fake_execution)

    asyncio.run(monitor.run_monitor_tick(store=object(), now=NOW, settings=SETTINGS))

    # Order matters: Providers that lost contact are handled before normal drain ends.
    assert calls == [
        ("expire_heartbeats", NOW, 15.0, SETTINGS.max_attempts),
        ("finish_drains", NOW, SETTINGS.max_attempts),
        ("expire_offers", NOW, SETTINGS.max_attempts, SETTINGS.offer_timeouts_before_exclude),
        ("expire_execution_leases", NOW, SETTINGS.max_attempts),
    ]


def test_loop_keeps_running_after_a_failed_tick(monkeypatch):
    ticks = []

    async def flaky_tick(store, now, settings):
        ticks.append(now)
        if len(ticks) == 1:
            raise RuntimeError("boom")

    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 3:
            raise asyncio.CancelledError  # stop the endless loop after 3 rounds

    monkeypatch.setattr(monitor, "run_monitor_tick", flaky_tick)
    monkeypatch.setattr(monitor.asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(monitor.monitor_loop(store=object(), clock=lambda: NOW, settings=SETTINGS))

    assert ticks == [NOW, NOW, NOW]  # the first failure did not stop the loop
    assert sleeps == [0.01, 0.01, 0.01]


# 2. Update lifespan test to assert both tasks start and stop cleanly
def test_app_starts_background_tasks_and_stops_them_on_shutdown():
    with TestClient(create_app(clock=lambda: NOW, settings=SETTINGS)) as client:
        monitor_task = client.app.state.monitor_task
        scheduler_task = client.app.state.scheduler_task

        assert monitor_task is not None
        assert scheduler_task is not None

        assert not monitor_task.done()
        assert not scheduler_task.done()

    assert monitor_task.done()
    assert scheduler_task.done()


# 3. Add scheduler interval to the config file validation checks
def test_repo_config_file_loads():
    settings = load_settings(DEFAULT_CONFIG_PATH)
    assert settings.heartbeat_timeout_seconds > 0
    assert settings.monitor_interval_seconds > 0
    assert settings.scheduler_interval_seconds > 0


def test_repo_config_follows_coordinator_timing_rule():
    settings = load_settings(DEFAULT_CONFIG_PATH)
    # A dead Provider must be noticed at most one monitor tick after its timeout.
    assert settings.monitor_interval_seconds <= settings.heartbeat_timeout_seconds / 3


def _config_with(change):
    """The repo config as a dict, with one deliberate mistake applied."""
    config = yaml.safe_load(DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    change(config)
    return config


@pytest.mark.parametrize(
    "change",
    [
        lambda config: config.pop("max_attempts"),
        lambda config: config.update(heartbeat_timeout_seconds=0),
        lambda config: config.update(max_attempts=0),
        lambda config: config.update(heartbeat_timeuot_seconds=5),  # typo
    ],
    ids=["missing-key", "zero-timeout", "zero-attempts", "unknown-key"],
)
def test_bad_config_fails_at_startup(tmp_path, change):
    path = tmp_path / "coordinator.yaml"
    path.write_text(yaml.safe_dump(_config_with(change)), encoding="utf-8")
    with pytest.raises(ValueError):
        load_settings(path)
