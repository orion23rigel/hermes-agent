from __future__ import annotations

import time
from types import SimpleNamespace

import httpx
import pytest

from agent.provider_request_watchdog import ProviderRequestStalledError
from hermes_constants import PARTIAL_STREAM_STUB_ID

_STOP = object()


def _chunk(text: str, *, finish_reason=None):
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                index=0,
                delta=SimpleNamespace(
                    content=text,
                    tool_calls=None,
                    reasoning_content=None,
                    reasoning=None,
                ),
                finish_reason=finish_reason,
            )
        ],
        model="test-model",
        usage=None,
    )


def _tool_chunk(*, name=None, arguments=None, tool_id=None):
    tool_delta = SimpleNamespace(
        index=0,
        id=tool_id,
        function=SimpleNamespace(name=name, arguments=arguments),
    )
    chunk = _chunk("")
    chunk.choices[0].delta.tool_calls = [tool_delta]
    return chunk


class _DelayedStream:
    def __init__(self, schedule):
        self.schedule = list(schedule)
        self.closed = False

    def __iter__(self):
        for delay, item in self.schedule:
            time.sleep(delay)
            if item is _STOP:
                return
            if isinstance(item, BaseException):
                raise item
            yield item

    def close(self):
        self.closed = True


class _Completions:
    def __init__(self, streams):
        self.streams = iter(streams)

    def create(self, **_kwargs):
        return next(self.streams)


class _InterruptingStream:
    def __init__(self):
        self.agent = None
        self.closed = False

    def __iter__(self):
        yield _chunk("partial")
        self.agent._interrupt_requested = True

    def close(self):
        self.closed = True


def _agent_with_streams(streams, *, timeout=0.05, stream_callback=None):
    from run_agent import AIAgent

    completions = _Completions(streams)
    client = SimpleNamespace(chat=SimpleNamespace(completions=completions))
    events = []
    agent = AIAgent(
        api_key="test-key",
        base_url="https://openrouter.ai/api/v1",
        model="test/model",
        provider="openrouter",
        quiet_mode=True,
        skip_context_files=True,
        skip_memory=True,
    )
    agent.api_mode = "chat_completions"
    agent._interrupt_requested = False
    agent._current_api_request_id = "req-stream-deadline"
    agent._current_api_retry_count = 2
    agent._resolved_api_call_timeout = lambda: timeout
    agent._create_request_openai_client = lambda **_kwargs: client
    agent._close_request_openai_client = lambda *_args, **_kwargs: None
    agent._abort_request_openai_client = lambda *_args, **_kwargs: None
    agent.event_callback = lambda name, payload: events.append((name, payload))
    if stream_callback is not None:
        agent.stream_delta_callback = stream_callback
    return agent, events


def test_stream_no_byte_deadline_raises_structured_stall():
    stream = _DelayedStream([(0.08, _STOP)])
    agent, events = _agent_with_streams([stream])

    with pytest.raises(ProviderRequestStalledError) as exc_info:
        agent._interruptible_streaming_api_call({})

    assert exc_info.value.error_code == "provider_request_stalled"
    assert exc_info.value.bytes_received == 0
    assert stream.closed is True
    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.failed",
    ]


def test_trickling_stream_cannot_extend_deadline_and_returns_partial_stub():
    deltas = []
    stream = _DelayedStream(
        [
            (0.03, _chunk("one")),
            (0.03, _chunk(" two")),
            (0.03, _chunk(" three", finish_reason="stop")),
        ]
    )
    agent, events = _agent_with_streams(
        [stream], stream_callback=deltas.append
    )

    response = agent._interruptible_streaming_api_call({})

    assert response.id == PARTIAL_STREAM_STUB_ID
    assert response.choices[0].finish_reason == "length"
    assert response.choices[0].message.content == "one"
    assert deltas
    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.failed",
    ]
    assert events[-1][1]["bytes_received"] > 0


def test_stream_retry_gets_fresh_absolute_deadline():
    first = _DelayedStream(
        [(0.03, httpx.ConnectError("first attempt dropped"))]
    )
    second = _DelayedStream(
        [(0.04, _chunk("ok", finish_reason="stop"))]
    )
    agent, events = _agent_with_streams([first, second])

    response = agent._interruptible_streaming_api_call({})

    assert response.choices[0].message.content == "ok"
    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.failed",
        "provider_request.started",
        "provider_request.completed",
    ]
    starts = [payload for name, payload in events if name == "provider_request.started"]
    assert [payload["retry_count"] for payload in starts] == [2, 3]


def test_deadline_after_partial_tool_fragment_returns_non_retryable_stub():
    stream = _DelayedStream(
        [
            (0.03, _tool_chunk(name="terminal", tool_id="call-1")),
            (0.03, _tool_chunk(arguments='{"command": "sleep 1"}')),
        ]
    )
    agent, events = _agent_with_streams([stream])

    response = agent._interruptible_streaming_api_call({})

    assert response.id == PARTIAL_STREAM_STUB_ID
    assert response.choices[0].message.tool_calls is None
    assert "terminal" in response.choices[0].message.content
    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.failed",
    ]


def test_stream_finishing_before_deadline_is_preserved():
    stream = _DelayedStream(
        [(0.01, _chunk("ok", finish_reason="stop"))]
    )
    agent, events = _agent_with_streams([stream], timeout=0.2)

    response = agent._interruptible_streaming_api_call({})

    assert response.choices[0].message.content == "ok"
    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.completed",
    ]


def test_worker_observed_interrupt_cancels_instead_of_completing_monitor():
    stream = _InterruptingStream()
    agent, events = _agent_with_streams([stream], timeout=0.2)
    stream.agent = agent

    with pytest.raises(InterruptedError):
        agent._interruptible_streaming_api_call({})

    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.failed",
    ]
    assert events[-1][1]["error_code"] == "InterruptedError"
    assert stream.closed is True


def test_interrupt_arriving_after_monitor_completion_does_not_relabel_request():
    stream = _DelayedStream(
        [(0.01, _chunk("ok", finish_reason="stop"))]
    )
    agent, events = _agent_with_streams([stream], timeout=0.2)

    def observe_lifecycle(name, payload):
        events.append((name, payload))
        if name == "provider_request.completed":
            agent._interrupt_requested = True

    agent.event_callback = observe_lifecycle
    response = agent._interruptible_streaming_api_call({})

    assert response.choices[0].message.content == "ok"
    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.completed",
    ]


def test_completed_response_fallback_cannot_publish_after_deadline():
    delivered = []

    class SlowMessage:
        reasoning_content = None
        reasoning = None

        @property
        def content(self):
            # Let the poll thread terminalize the attempt before fallback
            # publication reaches the shared delivery gate.
            time.sleep(0.08)
            return "late completed response"

    final_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SlowMessage())],
        usage=None,
        model="test-model",
    )
    agent, events = _agent_with_streams(
        [final_response],
        timeout=0.05,
        stream_callback=delivered.append,
    )

    with pytest.raises(ProviderRequestStalledError):
        agent._interruptible_streaming_api_call({})

    assert delivered == []
    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.failed",
    ]


def test_suppressed_content_callback_cannot_swallow_deadline_winner():
    class SlowSuppressedDelta:
        tool_calls = None
        reasoning_content = None
        reasoning = None

        def __init__(self):
            self.reads = 0

        @property
        def content(self):
            self.reads += 1
            if self.reads >= 2:
                # The content-presence check succeeds before the deadline;
                # accumulation then crosses it before the optional callback.
                time.sleep(0.08)
            return "suppressed"

    suppressed_chunk = _chunk("")
    suppressed_chunk.choices[0].delta = SlowSuppressedDelta()
    stream = _DelayedStream(
        [
            (0.0, _tool_chunk(arguments='{"unfinished":')),
            (0.0, suppressed_chunk),
        ]
    )
    agent, events = _agent_with_streams(
        [stream],
        timeout=0.05,
        stream_callback=lambda _text: None,
    )

    with pytest.raises(ProviderRequestStalledError):
        agent._interruptible_streaming_api_call({})

    assert [name for name, _ in events] == [
        "provider_request.started",
        "provider_request.failed",
    ]
