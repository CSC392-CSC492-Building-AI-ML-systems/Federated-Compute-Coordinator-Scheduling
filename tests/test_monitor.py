"""Monitor skeleton: tick order, loop resilience, and lifespan start/stop.

The individual checks (expire_heartbeats, finish_drains) get their own tests when
they are implemented; here they are replaced by fakes.
"""

import asyncio
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from coordinator import monitor
from coordinator.app import create_app
from coordinator.services import provider_service
from coordinator.settings import DEFAULT_CONFIG_PATH, Settings, load_settings

NOW = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)

# 1. Update SETTINGS fixture to include the scheduler interval
SETTINGS = Settings(
    heartbeat_timeout_seconds=15.0, monitor_interval_seconds=0.01, scheduler_interval_seconds=0.01
)


def test_tick_runs_heartbeat_check_then_drain_check(monkeypatch):
    calls = []

    async def fake_expire(store, now, timeout_seconds):
        calls.append(("expire_heartbeats", now, timeout_seconds))

    async def fake_finish(store, now):
        calls.append(("finish_drains", now))

    monkeypatch.setattr(provider_service, "expire_heartbeats", fake_expire)
    monkeypatch.setattr(provider_service, "finish_drains", fake_finish)

    asyncio.run(monitor.run_monitor_tick(store=object(), now=NOW, settings=SETTINGS))

    # Order matters: Providers that lost contact are handled before normal drain ends.
    assert calls == [("expire_heartbeats", NOW, 15.0), ("finish_drains", NOW)]


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


@pytest.mark.parametrize(
    "text",
    [
        "heartbeat_timeout_seconds: 15\n",  # missing keys
        (
            "heartbeat_timeout_seconds: 0\n"
            "monitor_interval_seconds: 1\n"
            "scheduler_interval_seconds: 1\n"
        ),
        (
            "heartbeat_timeout_seconds: 15\n"
            "monitor_interval_seconds: 1\n"
            "scheduler_interval_seconds: 1\n"
            "heartbeat_timeuot_seconds: 5\n"  # typo -> unknown key
        ),
    ],
    ids=["missing-key", "zero-timeout", "unknown-key"],
)
def test_bad_config_fails_at_startup(tmp_path, text):
    path = tmp_path / "coordinator.yaml"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ValueError):
        load_settings(path)
