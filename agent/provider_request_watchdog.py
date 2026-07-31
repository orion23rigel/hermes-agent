"""Provider request deadline contracts and lifecycle monitoring."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from agent.admission_controller import AdmissionToken


logger = logging.getLogger(__name__)


PROVIDER_REQUEST_STALLED = "provider_request_stalled"


def _diagnostic_label(value: str) -> str:
    """Bound and escape an untrusted label before placing it in log text."""
    return ascii(value)[1:-1][:100]


class ProviderRequestStalledError(TimeoutError):
    """A provider request exceeded its absolute, attempt-local deadline."""

    error_code = PROVIDER_REQUEST_STALLED
    retryable = True

    def __init__(
        self,
        *,
        provider: str,
        model: str,
        timeout_seconds: float,
        elapsed_seconds: float,
        api_request_id: str = "",
        retry_count: int = 0,
        bytes_received: int = 0,
    ) -> None:
        self.provider = provider
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.elapsed_seconds = elapsed_seconds
        self.api_request_id = api_request_id
        self.retry_count = retry_count
        self.bytes_received = bytes_received
        super().__init__(
            "Provider request stalled after "
            f"{elapsed_seconds:.3f}s (deadline {timeout_seconds:.3f}s; "
            f"provider={_diagnostic_label(provider)}; "
            f"model={_diagnostic_label(model)}; "
            f"api_request_id={_diagnostic_label(api_request_id)}; "
            f"retry_count={retry_count}; "
            f"bytes_received={bytes_received})"
        )


class ProviderRequestMonitor:
    """Thread-safe, attempt-local absolute deadline and lifecycle recorder."""

    def __init__(
        self,
        *,
        provider: str,
        model: str,
        timeout_seconds: float,
        event_callback: Callable[[str, dict[str, Any]], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
        progress_interval_seconds: float = 5.0,
        admission_token: AdmissionToken | None = None,
        release_fn: Callable[[AdmissionToken], None] | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.provider = provider
        self.model = model
        self.timeout_seconds = float(timeout_seconds)
        self._event_callback = event_callback
        self._clock = clock
        self._progress_interval_seconds = max(0.0, float(progress_interval_seconds))
        self._lock = threading.Lock()
        self._started_at: float | None = None
        self._deadline: float | None = None
        self._last_progress_event_at: float | None = None
        self._api_request_id = ""
        self._retry_count = 0
        self._bytes_received = 0
        self._terminal = False
        self._terminal_kind: str | None = None
        # Admission token lifecycle
        self._admission_token = admission_token
        self._release_fn = release_fn

    @property
    def deadline(self) -> float | None:
        with self._lock:
            return self._deadline

    @property
    def bytes_received(self) -> int:
        with self._lock:
            return self._bytes_received

    @property
    def terminal(self) -> bool:
        with self._lock:
            return self._terminal

    @property
    def terminal_kind(self) -> str | None:
        with self._lock:
            return self._terminal_kind

    def begin_attempt(
        self,
        *,
        api_request_id: str = "",
        retry_count: int = 0,
    ) -> None:
        now = self._clock()
        with self._lock:
            self._started_at = now
            self._deadline = now + self.timeout_seconds
            self._last_progress_event_at = now
            self._api_request_id = api_request_id
            self._retry_count = max(0, int(retry_count))
            self._bytes_received = 0
            self._terminal = False
            self._terminal_kind = None
            payload = self._payload_locked(now)
        self._emit("provider_request.started", payload)

    def record_progress(self, bytes_received: int = 0) -> None:
        """Record accepted bytes, rejecting progress after the absolute deadline."""
        now = self._clock()
        payload: dict[str, Any] | None = None
        stall_error: ProviderRequestStalledError | None = None
        event = "provider_request.progress"
        with self._lock:
            if self._started_at is None or self._terminal:
                return
            if self._deadline is not None and now >= self._deadline:
                stall_error = self._stall_error_locked(now)
                self._terminal = True
                self._terminal_kind = "failed"
                payload = self._payload_locked(
                    now, error_code=PROVIDER_REQUEST_STALLED
                )
                event = "provider_request.failed"
            else:
                self._bytes_received += max(0, int(bytes_received))
                last = self._last_progress_event_at
                if last is None or now - last >= self._progress_interval_seconds:
                    self._last_progress_event_at = now
                    payload = self._payload_locked(now)
        if stall_error is not None:
            self._release_admission()
        if payload is not None:
            self._emit(event, payload)
        if stall_error is not None:
            raise stall_error

    def check_deadline(self) -> None:
        now = self._clock()
        error: ProviderRequestStalledError | None = None
        payload: dict[str, Any] | None = None
        with self._lock:
            if (
                self._started_at is None
                or self._deadline is None
                or self._terminal
                or now < self._deadline
            ):
                return
            error = self._stall_error_locked(now)
            self._terminal = True
            self._terminal_kind = "failed"
            payload = self._payload_locked(now, error_code=PROVIDER_REQUEST_STALLED)
        self._release_admission()
        self._emit("provider_request.failed", payload)
        raise error

    def complete(self) -> bool:
        """Finish successfully unless the absolute deadline already elapsed."""
        now = self._clock()
        stall_error = None
        with self._lock:
            if self._started_at is None or self._terminal:
                return False
            self._terminal = True
            if now - self._started_at >= self.timeout_seconds:
                self._terminal_kind = "failed"
                stall_error = self._stall_error_locked(now)
                payload = self._payload_locked(
                    now, error_code=stall_error.error_code
                )
                event = "provider_request.failed"
            else:
                self._terminal_kind = "completed"
                payload = self._payload_locked(now)
                event = "provider_request.completed"
        self._release_admission()
        self._emit(event, payload)
        if stall_error is not None:
            raise stall_error
        return True

    def fail(self, error: BaseException) -> bool:
        """Finish with an error, giving an elapsed deadline precedence."""
        now = self._clock()
        stall_error = None
        error_code = _diagnostic_label(
            str(getattr(error, "error_code", "") or type(error).__name__)
        )
        with self._lock:
            if self._started_at is None or self._terminal:
                return False
            self._terminal = True
            self._terminal_kind = "failed"
            if now - self._started_at >= self.timeout_seconds:
                stall_error = self._stall_error_locked(now)
                error_code = stall_error.error_code
            payload = self._payload_locked(now, error_code=error_code)
        self._release_admission()
        self._emit("provider_request.failed", payload)
        if stall_error is not None:
            raise stall_error
        return True

    def cancel(self, error: BaseException) -> bool:
        """Terminate an explicitly cancelled attempt without deadline precedence."""
        now = self._clock()
        error_code = _diagnostic_label(
            str(getattr(error, "error_code", "") or type(error).__name__)
        )
        with self._lock:
            if self._started_at is None or self._terminal:
                return False
            self._terminal = True
            self._terminal_kind = "cancelled"
            payload = self._payload_locked(now, error_code=error_code)
        self._release_admission()
        self._emit("provider_request.failed", payload)
        return True

    def _stall_error_locked(self, now: float) -> ProviderRequestStalledError:
        assert self._started_at is not None
        return ProviderRequestStalledError(
            provider=self.provider,
            model=self.model,
            timeout_seconds=self.timeout_seconds,
            elapsed_seconds=max(0.0, now - self._started_at),
            api_request_id=self._api_request_id,
            retry_count=self._retry_count,
            bytes_received=self._bytes_received,
        )

    def _payload_locked(
        self,
        now: float,
        *,
        error_code: str = "",
    ) -> dict[str, Any]:
        started_at = self._started_at if self._started_at is not None else now
        return {
            "provider": self.provider,
            "model": self.model,
            "api_request_id": self._api_request_id,
            "retry_count": self._retry_count,
            "elapsed_seconds": max(0.0, now - started_at),
            "timeout_seconds": self.timeout_seconds,
            "bytes_received": self._bytes_received,
            "error_code": error_code,
        }

    def _emit(self, event: str, payload: dict[str, Any]) -> None:
        callback = self._event_callback
        if callback is None:
            return
        try:
            callback(event, payload)
        except Exception:
            logger.debug("Provider request lifecycle callback failed", exc_info=True)

    # ── Admission token lifecycle ───────────────────────────────────────────

    def _release_admission(self) -> None:
        """Release the admission token if one was provided.

        Called from all terminal paths (complete, fail, cancel) to ensure
        the admission slot is freed exactly once.
        """
        token = self._admission_token
        release_fn = self._release_fn
        if token is not None and release_fn is not None:
            self._admission_token = None  # Prevent double-release
            try:
                release_fn(token)
            except Exception:
                logger.debug("Admission token release failed", exc_info=True)
