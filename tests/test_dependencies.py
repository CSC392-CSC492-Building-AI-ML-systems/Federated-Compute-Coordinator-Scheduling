"""The shared route dependencies run on the event loop, not on a worker thread."""

import inspect

import pytest

from coordinator.api.tools import dependencies


@pytest.mark.parametrize(
    "dependency",
    [dependencies.get_store, dependencies.get_settings, dependencies.get_received_at],
)
def test_dependencies_run_on_the_event_loop(dependency):
    # A plain def would be sent to a worker thread and back on every request.
    assert inspect.iscoroutinefunction(dependency)
