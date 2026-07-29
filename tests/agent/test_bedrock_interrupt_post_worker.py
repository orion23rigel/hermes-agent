"""Regression: /stop must not be swallowed on the Bedrock streaming path.

Companion to the OpenAI/Anthropic streaming post-worker guard. The Bedrock
Converse stream callback (bedrock_adapter.stream_converse_with_callbacks) breaks
out of its event loop on interrupt and returns a PARTIAL response WITHOUT
raising. The worker thread then sets result["response"] and exits cleanly with
agent._interrupt_requested still True. Without a post-worker re-check in the
poll loop, interruptible_streaming_api_call would return that partial response
and silently swallow the /stop signal.
"""
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from agent import chat_completion_helpers as cch
from agent.provider_request_watchdog import ProviderRequestStalledError


class _FakeAgent:
    api_mode = "bedrock_converse"
    _interrupt_requested = False  # not interrupted at entry (passes pre-flight)
    _disable_streaming = False
    reasoning_callback = None
    stream_delta_callback = None
    # Real AIAgent always carries these; the streaming stale-timeout derivation
    # (chat_completion_helpers._derive_stream_stale_timeout) reads them.
    provider = "bedrock"
    model = "anthropic.claude-3-sonnet-20240229-v1:0"
    _consecutive_stale_streams = 0

    def _has_stream_consumers(self):
        return False

    def _buffer_status(self, *a, **k):
        pass

    def _claim_stream_writer(self):
        return 1

    def _fire_stream_delta(self, text):
        pass

    def _fire_tool_gen_started(self, name):
        pass

    def _fire_reasoning_delta(self, text):
        pass

    def _safe_print(self, *a, **k):
        pass


def test_bedrock_stream_interrupt_not_swallowed_post_worker():
    """A /stop arriving MID-stream: the pre-flight check (top of function) has
    already passed, the worker's stream callback breaks and returns a partial
    response WITHOUT raising, leaving _interrupt_requested True. The post-worker
    re-check must raise InterruptedError instead of returning the partial."""
    agent = _FakeAgent()

    partial = SimpleNamespace(choices=[], usage=None, stop_reason="interrupted")

    # Simulate the real adapter: on interrupt it breaks out and returns a
    # partial response WITHOUT raising. Flip the interrupt flag here to model
    # /stop arriving mid-stream (after the pre-flight check, during the worker).
    def _fake_stream(*args, **kwargs):
        agent._interrupt_requested = True
        return partial

    fake_client = SimpleNamespace(converse_stream=lambda **kw: {"stream": []})

    with patch("agent.bedrock_adapter._get_bedrock_runtime_client", return_value=fake_client), \
         patch("agent.bedrock_adapter.stream_converse_with_callbacks", side_effect=_fake_stream), \
         patch("agent.bedrock_adapter.normalize_converse_response", side_effect=lambda r: r), \
         patch("agent.bedrock_adapter.is_stale_connection_error", return_value=False), \
         patch("agent.bedrock_adapter.is_streaming_access_denied_error", return_value=False), \
         patch("agent.bedrock_adapter.invalidate_runtime_client", lambda *a, **k: None):
        api_kwargs = {"__bedrock_region__": "us-east-1", "__bedrock_converse__": True}
        with pytest.raises(InterruptedError):
            cch.interruptible_streaming_api_call(agent, api_kwargs)


def test_bedrock_stream_returns_normally_when_not_interrupted():
    """Sanity: with no interrupt, the same path returns the response (guard
    must not fire spuriously)."""
    agent = _FakeAgent()
    agent._interrupt_requested = False

    resp = SimpleNamespace(choices=[], usage=None, stop_reason="end_turn")

    class ProviderStream:
        def __init__(self):
            self.closed = False

        def __iter__(self):
            return iter(())

        def close(self):
            self.closed = True

    provider_stream = ProviderStream()
    fake_client = SimpleNamespace(
        converse_stream=lambda **kw: {"stream": provider_stream}
    )

    with patch("agent.bedrock_adapter._get_bedrock_runtime_client", return_value=fake_client), \
         patch("agent.bedrock_adapter.stream_converse_with_callbacks", return_value=resp), \
         patch("agent.bedrock_adapter.normalize_converse_response", side_effect=lambda r: r), \
         patch("agent.bedrock_adapter.is_stale_connection_error", return_value=False), \
         patch("agent.bedrock_adapter.is_streaming_access_denied_error", return_value=False), \
         patch("agent.bedrock_adapter.invalidate_runtime_client", lambda *a, **k: None):
        api_kwargs = {"__bedrock_region__": "us-east-1", "__bedrock_converse__": True}
        out = cch.interruptible_streaming_api_call(agent, api_kwargs)
        assert out is resp
        assert provider_stream.closed is True


def test_bedrock_stream_absolute_deadline_is_structured_and_bounded():
    agent = _FakeAgent()
    agent._interrupt_requested = False
    agent._resolved_api_call_timeout = lambda: 0.05
    agent._current_api_request_id = "req-bedrock-deadline"
    events = []
    agent.event_callback = lambda name, payload: events.append((name, payload))
    unblock = threading.Event()

    response = SimpleNamespace(choices=[], usage=None, stop_reason="end_turn")

    def _blocked_adapter(*_args, **_kwargs):
        unblock.wait(timeout=2.0)
        return response

    fake_client = SimpleNamespace(
        converse_stream=lambda **_kwargs: {"stream": []}
    )

    with patch(
        "agent.bedrock_adapter._get_bedrock_runtime_client",
        return_value=fake_client,
    ), patch(
        "agent.bedrock_adapter.stream_converse_with_callbacks",
        side_effect=_blocked_adapter,
    ), patch(
        "agent.bedrock_adapter.normalize_converse_response",
        side_effect=lambda value: value,
    ), patch(
        "agent.bedrock_adapter.is_stale_connection_error",
        return_value=False,
    ), patch(
        "agent.bedrock_adapter.is_streaming_access_denied_error",
        return_value=False,
    ), patch(
        "agent.bedrock_adapter.invalidate_runtime_client",
        side_effect=lambda *_args, **_kwargs: unblock.set(),
    ):
        api_kwargs = {
            "__bedrock_region__": "us-east-1",
            "__bedrock_converse__": True,
        }
        with pytest.raises(ProviderRequestStalledError):
            cch.interruptible_streaming_api_call(agent, api_kwargs)

    assert unblock.is_set()
    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.failed",
    ]


def test_bedrock_partial_tool_start_names_unexecuted_action_in_stub():
    agent = _FakeAgent()
    agent._interrupt_requested = False
    agent._resolved_api_call_timeout = lambda: 0.05
    tool_starts = []
    agent._fire_tool_gen_started = tool_starts.append
    unblock = threading.Event()
    response = SimpleNamespace(choices=[], usage=None, stop_reason="end_turn")

    def _tool_then_block(*_args, **kwargs):
        kwargs["on_tool_start"]("terminal")
        unblock.wait(timeout=2.0)
        return response

    fake_client = SimpleNamespace(converse_stream=lambda **_kwargs: {"stream": []})
    with patch(
        "agent.bedrock_adapter._get_bedrock_runtime_client", return_value=fake_client
    ), patch(
        "agent.bedrock_adapter.stream_converse_with_callbacks",
        side_effect=_tool_then_block,
    ), patch(
        "agent.bedrock_adapter.normalize_converse_response", side_effect=lambda value: value
    ), patch(
        "agent.bedrock_adapter.is_stale_connection_error", return_value=False
    ), patch(
        "agent.bedrock_adapter.is_streaming_access_denied_error", return_value=False
    ), patch(
        "agent.bedrock_adapter.invalidate_runtime_client",
        side_effect=lambda *_args, **_kwargs: unblock.set(),
    ):
        response = cch.interruptible_streaming_api_call(
            agent,
            {"__bedrock_region__": "us-east-1", "__bedrock_converse__": True},
        )

    assert tool_starts == ["terminal"]
    choice = response.choices[0]
    assert choice.message.tool_calls is None
    assert "terminal" in choice.message.content
    assert "action was not executed" in choice.message.content


def test_bedrock_transport_error_after_tool_start_returns_safe_partial_stub():
    agent = _FakeAgent()
    agent._interrupt_requested = False
    tool_starts = []
    agent._fire_tool_gen_started = tool_starts.append

    def _tool_then_fail(*_args, **kwargs):
        kwargs["on_tool_start"]("terminal")
        raise OSError("connection dropped")

    fake_client = SimpleNamespace(converse_stream=lambda **_kwargs: {"stream": []})
    with patch(
        "agent.bedrock_adapter._get_bedrock_runtime_client", return_value=fake_client
    ), patch(
        "agent.bedrock_adapter.stream_converse_with_callbacks",
        side_effect=_tool_then_fail,
    ), patch(
        "agent.bedrock_adapter.normalize_converse_response", side_effect=lambda value: value
    ), patch(
        "agent.bedrock_adapter.is_stale_connection_error", return_value=False
    ), patch(
        "agent.bedrock_adapter.is_streaming_access_denied_error", return_value=False
    ), patch(
        "agent.bedrock_adapter.invalidate_runtime_client", lambda *_args, **_kwargs: None
    ):
        response = cch.interruptible_streaming_api_call(
            agent,
            {"__bedrock_region__": "us-east-1", "__bedrock_converse__": True},
        )

    assert tool_starts == ["terminal"]
    choice = response.choices[0]
    assert choice.message.tool_calls is None
    assert "terminal" in choice.message.content
    assert "action was not executed" in choice.message.content


def test_bedrock_deadline_terminal_winner_is_not_overridden_by_late_interrupt():
    agent = _FakeAgent()
    agent._interrupt_requested = False
    agent._resolved_api_call_timeout = lambda: 0.05
    unblock = threading.Event()
    response = SimpleNamespace(choices=[], usage=None, stop_reason="end_turn")

    def _blocked_adapter(*_args, **_kwargs):
        unblock.wait(timeout=2.0)
        return response

    def _deadline_abort(*_args, **_kwargs):
        # Simulate /stop arriving only after the deadline has already won and
        # emitted provider_request.failed.
        agent._interrupt_requested = True
        unblock.set()

    fake_client = SimpleNamespace(converse_stream=lambda **_kwargs: {"stream": []})
    with patch(
        "agent.bedrock_adapter._get_bedrock_runtime_client", return_value=fake_client
    ), patch(
        "agent.bedrock_adapter.stream_converse_with_callbacks",
        side_effect=_blocked_adapter,
    ), patch(
        "agent.bedrock_adapter.normalize_converse_response", side_effect=lambda value: value
    ), patch(
        "agent.bedrock_adapter.is_stale_connection_error", return_value=False
    ), patch(
        "agent.bedrock_adapter.is_streaming_access_denied_error", return_value=False
    ), patch(
        "agent.bedrock_adapter.invalidate_runtime_client", side_effect=_deadline_abort
    ):
        with pytest.raises(ProviderRequestStalledError):
            cch.interruptible_streaming_api_call(
                agent,
                {"__bedrock_region__": "us-east-1", "__bedrock_converse__": True},
            )


def test_bedrock_completed_terminal_winner_survives_late_interrupt():
    agent = _FakeAgent()
    agent._interrupt_requested = False
    response = SimpleNamespace(choices=[], usage=None, stop_reason="end_turn")
    events = []

    def record_and_interrupt(name, payload):
        events.append((name, payload))
        if name == "provider_request.completed":
            agent._interrupt_requested = True

    agent.event_callback = record_and_interrupt
    fake_client = SimpleNamespace(converse_stream=lambda **_kwargs: {"stream": []})

    with patch(
        "agent.bedrock_adapter._get_bedrock_runtime_client", return_value=fake_client
    ), patch(
        "agent.bedrock_adapter.stream_converse_with_callbacks",
        return_value=response,
    ), patch(
        "agent.bedrock_adapter.normalize_converse_response", side_effect=lambda value: value
    ), patch(
        "agent.bedrock_adapter.is_stale_connection_error", return_value=False
    ), patch(
        "agent.bedrock_adapter.is_streaming_access_denied_error", return_value=False
    ), patch(
        "agent.bedrock_adapter.invalidate_runtime_client", lambda *_args, **_kwargs: None
    ):
        actual = cch.interruptible_streaming_api_call(
            agent,
            {"__bedrock_region__": "us-east-1", "__bedrock_converse__": True},
        )

    assert actual is response
    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.completed",
    ]


def test_bedrock_stale_giveup_terminalizes_and_fences_late_worker(monkeypatch):
    monkeypatch.setenv("HERMES_STREAM_STALE_TIMEOUT", "0.05")
    monkeypatch.setenv("HERMES_STREAM_STALE_GIVEUP", "1")
    agent = _FakeAgent()
    agent._interrupt_requested = False
    agent._resolved_api_call_timeout = lambda: 1.0
    agent._current_api_request_id = "req-bedrock-stale-giveup"
    events = []
    delivered = []
    agent.event_callback = lambda name, payload: events.append((name, payload))
    agent._has_stream_consumers = lambda: True
    agent._fire_stream_delta = delivered.append
    unblock = threading.Event()
    worker_observed_release = threading.Event()
    late_response = SimpleNamespace(choices=[], usage=None, stop_reason="end_turn")

    def blocked_adapter(*_args, **kwargs):
        unblock.wait(timeout=2.0)
        kwargs["on_text_delta"]("late")
        worker_observed_release.set()
        return late_response

    def release_worker_during_invalidation(*_args, **_kwargs):
        unblock.set()
        assert worker_observed_release.wait(timeout=1.0)

    fake_client = SimpleNamespace(converse_stream=lambda **_kwargs: {"stream": []})
    with patch(
        "agent.bedrock_adapter._get_bedrock_runtime_client", return_value=fake_client
    ), patch(
        "agent.bedrock_adapter.stream_converse_with_callbacks",
        side_effect=blocked_adapter,
    ), patch(
        "agent.bedrock_adapter.normalize_converse_response", side_effect=lambda value: value
    ), patch(
        "agent.bedrock_adapter.is_stale_connection_error", return_value=False
    ), patch(
        "agent.bedrock_adapter.is_streaming_access_denied_error", return_value=False
    ), patch(
        "agent.bedrock_adapter.invalidate_runtime_client",
        side_effect=release_worker_during_invalidation,
    ):
        with pytest.raises(ProviderRequestStalledError) as exc_info:
            cch.interruptible_streaming_api_call(
                agent,
                {"__bedrock_region__": "us-east-1", "__bedrock_converse__": True},
            )

    assert exc_info.value.error_code == "provider_request_stalled"
    assert exc_info.value.retryable is True
    assert unblock.is_set()
    assert worker_observed_release.is_set()
    time.sleep(0.05)
    assert delivered == []
    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.failed",
    ]
