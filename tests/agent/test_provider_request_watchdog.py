"""Tests for the provider request deadline error contract."""

import pytest

from agent.provider_request_watchdog import (
    PROVIDER_REQUEST_STALLED,
    ProviderRequestMonitor,
    ProviderRequestStalledError,
)


def make_error() -> ProviderRequestStalledError:
    return ProviderRequestStalledError(
        provider="openrouter",
        model="anthropic/claude-sonnet-4",
        timeout_seconds=30.0,
        elapsed_seconds=30.125,
        api_request_id="req_123",
        retry_count=2,
        bytes_received=4096,
    )


def test_provider_request_stalled_error_has_safe_structured_metadata():
    error = make_error()

    assert isinstance(error, TimeoutError)
    assert error.error_code == PROVIDER_REQUEST_STALLED == "provider_request_stalled"
    assert error.retryable is True
    assert error.provider == "openrouter"
    assert error.model == "anthropic/claude-sonnet-4"
    assert error.timeout_seconds == 30.0
    assert error.elapsed_seconds == 30.125
    assert error.api_request_id == "req_123"
    assert error.retry_count == 2
    assert error.bytes_received == 4096

    diagnostic = str(error)
    assert len(diagnostic) <= 500
    assert "request body" not in diagnostic.lower()
    assert "response payload" not in diagnostic.lower()
    assert not hasattr(error, "request_body")
    assert not hasattr(error, "response_payload")


def test_provider_request_stalled_error_optional_metadata_defaults():
    # Callers that have no request id yet (pre-flight stalls) must still be
    # able to raise the contract with only the required deadline metadata.
    error = ProviderRequestStalledError(
        provider="openrouter",
        model="anthropic/claude-sonnet-4",
        timeout_seconds=30.0,
        elapsed_seconds=30.125,
    )

    assert error.api_request_id == ""
    assert error.retry_count == 0
    assert error.bytes_received == 0


def test_provider_request_stalled_error_diagnostic_bounds_untrusted_metadata():
    # Provider/model/request-id strings come from upstream responses; a long
    # value must not make the diagnostic unbounded.
    error = ProviderRequestStalledError(
        provider="p" * 5000,
        model="m" * 5000,
        timeout_seconds=30.0,
        elapsed_seconds=30.125,
        api_request_id="r" * 5000,
    )

    assert len(str(error)) <= 500


def test_provider_request_stalled_error_escapes_control_characters():
    error = ProviderRequestStalledError(
        provider="provider\nforged",
        model="model\rforged",
        timeout_seconds=30.0,
        elapsed_seconds=30.125,
        api_request_id="request\tforged",
    )

    diagnostic = str(error)
    assert "\n" not in diagnostic
    assert "\r" not in diagnostic
    assert "\t" not in diagnostic
    assert r"provider\nforged" in diagnostic
    assert r"model\rforged" in diagnostic
    assert r"request\tforged" in diagnostic


class _Clock:
    def __init__(self) -> None:
        self.now = 10.0

    def __call__(self) -> float:
        return self.now


def test_monitor_deadline_is_fixed_and_progress_only_counts_bytes():
    clock = _Clock()
    events = []
    monitor = ProviderRequestMonitor(
        provider="p",
        model="m",
        timeout_seconds=5.0,
        event_callback=lambda name, payload: events.append((name, payload)),
        clock=clock,
        progress_interval_seconds=1.0,
    )
    monitor.begin_attempt(api_request_id="req", retry_count=2)
    assert monitor.deadline == 15.0

    clock.now = 11.0
    monitor.record_progress(7)
    clock.now = 14.9
    monitor.record_progress(3)
    assert monitor.deadline == 15.0
    assert monitor.bytes_received == 10
    monitor.check_deadline()

    clock.now = 15.0
    try:
        monitor.check_deadline()
    except ProviderRequestStalledError as error:
        assert error.elapsed_seconds == 5.0
        assert error.bytes_received == 10
        assert error.retry_count == 2
    else:
        raise AssertionError("deadline did not raise")

    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.progress",
        "provider_request.progress",
        "provider_request.failed",
    ]
    assert monitor.terminal_kind == "failed"


def test_monitor_new_attempt_gets_fresh_deadline_and_terminal_event_once():
    clock = _Clock()
    events = []
    monitor = ProviderRequestMonitor(
        provider="p",
        model="m",
        timeout_seconds=5.0,
        event_callback=lambda name, payload: events.append((name, payload)),
        clock=clock,
    )
    monitor.begin_attempt(retry_count=0)
    first_deadline = monitor.deadline
    monitor.fail(RuntimeError("secret provider payload"))
    monitor.complete()

    clock.now = 20.0
    monitor.begin_attempt(retry_count=1)
    assert monitor.deadline == 25.0
    assert monitor.deadline != first_deadline
    monitor.complete()
    monitor.fail(RuntimeError("late"))
    assert monitor.terminal_kind == "completed"

    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.failed",
        "provider_request.started",
        "provider_request.completed",
    ]
    assert events[1][1]["error_code"] == "RuntimeError"
    assert "secret provider payload" not in repr(events)


def test_monitor_callback_failure_is_fail_soft_and_payload_is_allowlisted():
    clock = _Clock()
    seen = []

    def callback(name, payload):
        seen.append((name, payload))
        raise RuntimeError("sink failed")

    monitor = ProviderRequestMonitor(
        provider="p",
        model="m",
        timeout_seconds=1.0,
        event_callback=callback,
        clock=clock,
        progress_interval_seconds=10.0,
    )
    monitor.begin_attempt()
    monitor.record_progress(4)
    assert monitor.bytes_received == 4
    clock.now = 11.0
    try:
        monitor.check_deadline()
    except ProviderRequestStalledError:
        pass
    else:
        raise AssertionError("deadline did not raise")

    assert [name for name, _ in seen] == [
        "provider_request.started",
        "provider_request.failed",
    ]
    assert set(seen[-1][1]) == {
        "provider",
        "model",
        "api_request_id",
        "retry_count",
        "elapsed_seconds",
        "timeout_seconds",
        "bytes_received",
        "error_code",
    }


@pytest.mark.parametrize("terminal", ["complete", "fail"])
def test_terminal_transition_after_deadline_becomes_structured_stall(terminal):
    clock = _Clock()
    events = []
    monitor = ProviderRequestMonitor(
        provider="p",
        model="m",
        timeout_seconds=5,
        clock=clock,
        event_callback=lambda event, payload: events.append((event, payload)),
    )
    monitor.begin_attempt(api_request_id="req-late-terminal")
    clock.now += 6

    with pytest.raises(ProviderRequestStalledError):
        if terminal == "complete":
            monitor.complete()
        else:
            monitor.fail(OSError("late transport failure"))

    assert [event for event, _ in events] == [
        "provider_request.started",
        "provider_request.failed",
    ]
    assert events[-1][1]["error_code"] == "provider_request_stalled"


def test_explicit_cancellation_wins_over_elapsed_deadline():
    clock = _Clock()
    events = []
    monitor = ProviderRequestMonitor(
        provider="p",
        model="m",
        timeout_seconds=5,
        clock=clock,
        event_callback=lambda event, payload: events.append((event, payload)),
    )
    monitor.begin_attempt()
    clock.now += 6

    assert monitor.cancel(InterruptedError("stopped")) is True
    assert monitor.terminal_kind == "cancelled"
    assert events[-1][0] == "provider_request.failed"
    assert events[-1][1]["error_code"] == "InterruptedError"


def test_progress_arriving_after_deadline_is_rejected_terminally():
    clock = _Clock()
    events = []
    monitor = ProviderRequestMonitor(
        provider="p",
        model="m",
        timeout_seconds=5,
        clock=clock,
        event_callback=lambda event, payload: events.append((event, payload)),
    )
    monitor.begin_attempt()
    monitor.record_progress(3)
    clock.now += 5

    with pytest.raises(ProviderRequestStalledError) as exc_info:
        monitor.record_progress(7)

    assert exc_info.value.bytes_received == 3
    assert events[-1][0] == "provider_request.failed"
    assert events[-1][1]["bytes_received"] == 3
    assert monitor.complete() is False
