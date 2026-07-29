from __future__ import annotations

import threading
import time
from types import SimpleNamespace

import pytest

from agent.provider_request_watchdog import ProviderRequestStalledError


class _DeadlineAgent:
    def __init__(self, platform: str) -> None:
        self.platform = platform
        self.api_mode = "chat_completions"
        self.provider = "test-provider"
        self.model = "test-model"
        self.base_url = "https://example.invalid/v1"
        self._base_url = self.base_url
        self._interrupt_requested = False
        self._active_request_abort = None
        self._current_api_request_id = "req-deadline-test"
        self._current_api_retry_count = 3
        self._abort_event = threading.Event()
        self.events = []

    def _resolved_api_call_timeout(self) -> float:
        return 0.05

    def _compute_non_stream_stale_timeout(self, _api_kwargs) -> float:
        return 10.0

    def _create_request_openai_client(self, **_kwargs):
        return SimpleNamespace()

    def _abort_request_openai_client(self, _client, *, reason: str) -> None:
        assert reason == "provider_request_deadline"
        self._abort_event.set()

    def _close_request_openai_client(self, _client, *, reason: str) -> None:
        pass

    def _touch_activity(self, _message: str) -> None:
        pass

    def _emit_wait_notice(self, _message: str) -> None:
        pass

    def _buffer_status(self, _message: str) -> None:
        pass

    def event_callback(self, name, payload) -> None:
        self.events.append((name, payload))


@pytest.mark.parametrize("platform", ["cli", "cron"])
def test_absolute_nonstream_deadline_aborts_and_raises_structured_error(
    monkeypatch, platform
):
    from agent import chat_completion_helpers as helpers

    agent = _DeadlineAgent(platform)

    def blocked_dispatch(_agent, _api_kwargs, *, make_client):
        make_client("deadline-test")
        if not agent._abort_event.wait(timeout=2.0):
            raise AssertionError("provider request was not aborted at its deadline")
        raise OSError("socket closed by watchdog")

    monkeypatch.setattr(
        helpers,
        "_dispatch_nonstreaming_api_request",
        blocked_dispatch,
    )

    started = time.monotonic()
    with pytest.raises(ProviderRequestStalledError) as exc_info:
        helpers.interruptible_api_call(agent, {"model": agent.model, "messages": []})
    elapsed = time.monotonic() - started

    error = exc_info.value
    assert error.error_code == "provider_request_stalled"
    assert error.retryable is True
    assert error.provider == agent.provider
    assert error.model == agent.model
    assert error.timeout_seconds == 0.05
    assert elapsed < 1.0
    assert agent._abort_event.is_set()
    assert [name for name, _ in agent.events] == [
        "provider_request.started",
        "provider_request.failed",
    ]
    assert agent.events[0][1]["api_request_id"] == "req-deadline-test"
    assert agent.events[0][1]["retry_count"] == 3


def test_late_nonstream_response_cannot_overwrite_deadline(monkeypatch):
    from agent import chat_completion_helpers as helpers

    agent = _DeadlineAgent("cli")
    late_response = object()

    def late_dispatch(_agent, _api_kwargs, *, make_client):
        make_client("deadline-test")
        assert agent._abort_event.wait(timeout=2.0)
        return late_response

    monkeypatch.setattr(
        helpers,
        "_dispatch_nonstreaming_api_request",
        late_dispatch,
    )

    with pytest.raises(ProviderRequestStalledError):
        helpers.interruptible_api_call(agent, {"model": agent.model, "messages": []})

    assert [name for name, _ in agent.events] == [
        "provider_request.started",
        "provider_request.failed",
    ]


@pytest.mark.parametrize("platform", ["cli", "cron"])
def test_response_returning_after_deadline_before_poll_is_still_rejected(
    monkeypatch, platform,
):
    from agent import chat_completion_helpers as helpers

    agent = _DeadlineAgent(platform)

    def late_dispatch(*_args, **_kwargs):
        time.sleep(0.08)
        return object()

    monkeypatch.setattr(
        helpers,
        "_dispatch_nonstreaming_api_request",
        late_dispatch,
    )

    with pytest.raises(ProviderRequestStalledError):
        helpers.interruptible_api_call(agent, {"model": agent.model, "messages": []})

    assert [name for name, _ in agent.events] == [
        "provider_request.started",
        "provider_request.failed",
    ]


@pytest.mark.parametrize("platform", ["cli", "cron"])
def test_nonstream_response_before_deadline_is_preserved(monkeypatch, platform):
    from agent import chat_completion_helpers as helpers

    agent = _DeadlineAgent(platform)
    agent._resolved_api_call_timeout = lambda: 0.5
    response = object()

    monkeypatch.setattr(
        helpers,
        "_dispatch_nonstreaming_api_request",
        lambda *_args, **_kwargs: response,
    )

    assert helpers.interruptible_api_call(
        agent, {"model": agent.model, "messages": []}
    ) is response
    assert not agent._abort_event.is_set()
    assert [name for name, _ in agent.events] == [
        "provider_request.started",
        "provider_request.completed",
    ]


def test_nonstream_worker_observed_interrupt_terminalizes_as_cancel(monkeypatch):
    from agent import chat_completion_helpers as helpers

    agent = _DeadlineAgent("cli")
    agent._resolved_api_call_timeout = lambda: 0.5

    def interrupted_dispatch(*_args, **_kwargs):
        agent._interrupt_requested = True
        return object()

    monkeypatch.setattr(
        helpers,
        "_dispatch_nonstreaming_api_request",
        interrupted_dispatch,
    )

    with pytest.raises(InterruptedError):
        helpers.interruptible_api_call(
            agent, {"model": agent.model, "messages": []}
        )
    assert [name for name, _ in agent.events] == [
        "provider_request.started",
        "provider_request.failed",
    ]
    assert agent.events[-1][1]["error_code"] == "InterruptedError"


def test_nonstream_completed_terminal_winner_survives_late_interrupt(monkeypatch):
    from agent import chat_completion_helpers as helpers

    agent = _DeadlineAgent("cli")
    agent._resolved_api_call_timeout = lambda: 0.5
    response = object()

    def record_and_interrupt(name, payload):
        agent.events.append((name, payload))
        if name == "provider_request.completed":
            agent._interrupt_requested = True

    agent.event_callback = record_and_interrupt
    monkeypatch.setattr(
        helpers,
        "_dispatch_nonstreaming_api_request",
        lambda *_args, **_kwargs: response,
    )

    assert helpers.interruptible_api_call(
        agent, {"model": agent.model, "messages": []}
    ) is response
    assert [name for name, _ in agent.events] == [
        "provider_request.started",
        "provider_request.completed",
    ]


def test_nonstream_stale_detector_does_not_overwrite_worker_error(monkeypatch):
    from agent import chat_completion_helpers as helpers

    agent = _DeadlineAgent("cli")
    agent._resolved_api_call_timeout = lambda: 1.0
    agent._compute_non_stream_stale_timeout = lambda _kwargs: 0.01

    def worker_failure(_agent, _api_kwargs, *, make_client):
        make_client("stale-race")
        # The first 300ms poll returns while this request is still alive, so
        # the stale path closes the client and joins this worker before trying
        # to synthesize its own timeout.
        time.sleep(0.35)
        raise ValueError("canonical worker failure")

    monkeypatch.setattr(
        helpers,
        "_dispatch_nonstreaming_api_request",
        worker_failure,
    )

    with pytest.raises(ValueError, match="canonical worker failure"):
        helpers.interruptible_api_call(
            agent, {"model": agent.model, "messages": []}
        )
    assert [name for name, _ in agent.events] == [
        "provider_request.started",
        "provider_request.failed",
    ]
    assert agent.events[-1][1]["error_code"] == "ValueError"
