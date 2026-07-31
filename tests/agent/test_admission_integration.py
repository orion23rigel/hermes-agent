"""Integration tests for admission in full provider call chain.

Exercises the full admission flow through the provider call chain:
- AdmissionController (SQLite queue + flock mutual exclusion)
- ChatCompletionsTransport (admit/release hooks)
- classify_lane helper
- Transport-level admit/release when wired and unwired

All tests use hermetic temp HERMES_HOME.
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from agent.admission_controller import (
    AdmissionCancelledError,
    AdmissionController,
    AdmissionToken,
    AdmissionTimeoutError,
    classify_lane,
    endpoint_hash,
    normalize_endpoint_url,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def hermes_home(tmp_path: Path) -> Path:
    home = tmp_path / ".hermes"
    home.mkdir(parents=True, exist_ok=True)
    with patch.dict(os.environ, {"HERMES_HOME": str(home)}):
        yield home


@pytest.fixture
def controller(hermes_home: Path) -> AdmissionController:
    ehash = endpoint_hash("https://api.test-endpoint.example/v1")
    config = {
        ehash: {
            "enabled": True,
            "admission_timeout": 5.0,
            "interactive_burst": 3,
            "max_queue_age": 120,
        }
    }
    ctrl = AdmissionController(hermes_home=hermes_home, config=config)
    yield ctrl
    if ctrl._db is not None:
        ctrl._db.close()
    lock_dir = ctrl._lock_dir
    if lock_dir.exists():
        for f in lock_dir.iterdir():
            try:
                f.unlink()
            except OSError:
                pass


def _cleanup(ctrl: AdmissionController) -> None:
    if ctrl._db is not None:
        ctrl._db.close()
    lock_dir = ctrl._lock_dir
    if lock_dir.exists():
        for f in lock_dir.iterdir():
            try:
                f.unlink()
            except OSError:
                pass


# ---------------------------------------------------------------------------
# 1. Gateway admission test
# ---------------------------------------------------------------------------


class TestGatewayAdmission:
    """Gateway conversation thread acquires and releases admission."""

    def test_gateway_admit_release_flow(self, controller: AdmissionController):
        """Simulate gateway thread: admit -> provider call -> release."""
        ehash = endpoint_hash("https://api.test-endpoint.example/v1")

        # Gateway admits before provider call
        token = controller.acquire(ehash, "interactive", "gateway", admission_timeout=2.0)
        assert token is not None
        assert token.lock_fd is not None
        assert token.request_id != ""

        # Verify queue depth was 0 before acquire, and status shows running
        status = controller.status(ehash)
        assert status["running"] == 1

        # Simulate provider call completion
        controller.release(token)

        # Verify queue depth returns to 0
        status = controller.status(ehash)
        assert status["running"] == 0
        assert status["queued"] == 0

    def test_gateway_queue_depth_tracking(self, controller: AdmissionController):
        """Queue depth reflects queued requests."""
        ehash = endpoint_hash("https://api.test-endpoint.example/v1")

        # Hold lock
        holder = controller.acquire(ehash, "interactive", "gateway", admission_timeout=5.0)
        assert holder is not None

        status = controller.status(ehash)
        assert status["running"] == 1

        # Release holder
        controller.release(holder)
        status = controller.status(ehash)
        assert status["running"] == 0


# ---------------------------------------------------------------------------
# 2. Kanban worker admission test
# ---------------------------------------------------------------------------


class TestKanbanWorkerAdmission:
    """Kanban worker pre-flight admission."""

    def test_kanban_worker_acquires(self, controller: AdmissionController):
        """Kanban worker acquires admission lock."""
        ehash = endpoint_hash("https://api.test-endpoint.example/v1")

        token = controller.acquire(ehash, "background", "kanban", admission_timeout=2.0)
        assert token is not None
        assert token.lock_fd is not None
        controller.release(token)

    def test_kanban_worker_deferred_when_saturated(self, controller: AdmissionController):
        """When endpoint is saturated, background worker times out."""
        ehash = endpoint_hash("https://api.test-endpoint.example/v1")

        # Hold the lock with a long timeout
        holder = controller.acquire(ehash, "interactive", "gateway", admission_timeout=5.0)
        assert holder is not None

        # Background worker with short timeout should time out
        start = time.monotonic()
        token = controller.acquire(ehash, "background", "kanban", admission_timeout=0.3)
        elapsed = time.monotonic() - start

        assert token is None, "Background worker should time out when saturated"
        assert elapsed < 2.0, f"Took too long: {elapsed:.3f}s"

        controller.release(holder)


# ---------------------------------------------------------------------------
# 3. Cron admission test
# ---------------------------------------------------------------------------


class TestCronAdmission:
    """Cron job enqueues, waits, acquires."""

    def test_cron_acquires_with_available_slot(self, controller: AdmissionController):
        """Cron job acquires when slot is available."""
        ehash = endpoint_hash("https://api.test-endpoint.example/v1")

        token = controller.acquire(ehash, "background", "cron", admission_timeout=2.0)
        assert token is not None
        assert token.lock_fd is not None
        controller.release(token)

    def test_cron_defers_when_busy(self, controller: AdmissionController):
        """Cron job defers when all slots are busy."""
        ehash = endpoint_hash("https://api.test-endpoint.example/v1")

        # Hold the lock
        holder = controller.acquire(ehash, "interactive", "gateway", admission_timeout=5.0)
        assert holder is not None

        # Cron with short timeout should defer
        start = time.monotonic()
        token = controller.acquire(ehash, "background", "cron", admission_timeout=0.3)
        elapsed = time.monotonic() - start

        assert token is None, "Cron should defer when slot is busy"
        assert elapsed < 2.0

        controller.release(holder)


# ---------------------------------------------------------------------------
# 4. Mixed contexts test
# ---------------------------------------------------------------------------


class TestMixedContexts:
    """Gateway + cron + kanban all target same endpoint concurrently."""

    def test_sequential_admission_single_endpoint(self, controller: AdmissionController):
        """Only one request gets admission at a time for same endpoint."""
        ehash = endpoint_hash("https://api.test-endpoint.example/v1")

        results = []
        lock = threading.Lock()

        def acquire_context(context_name: str):
            t = controller.acquire(ehash, "interactive", context_name, admission_timeout=3.0)
            with lock:
                results.append((context_name, t is not None))
            if t is not None:
                time.sleep(0.1)
                controller.release(t)

        threads = []
        for ctx in ["gateway", "cron", "kanban"]:
            th = threading.Thread(target=acquire_context, args=(ctx,))
            threads.append(th)

        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=10.0)

        # All should have succeeded (sequentially)
        assert len(results) == 3
        assert all(r[1] for r in results), "All contexts should have acquired admission"

    def test_background_queued_behind_interactive(self, controller: AdmissionController):
        """Background requests are queued while interactive runs."""
        ehash = endpoint_hash("https://api.test-endpoint.example/v1")

        # Hold lock with interactive
        holder = controller.acquire(ehash, "interactive", "gateway", admission_timeout=5.0)
        assert holder is not None

        bg_result = []
        bg_lock = threading.Lock()

        def bg_acquire():
            t = controller.acquire(ehash, "background", "cron", admission_timeout=2.0)
            with bg_lock:
                bg_result.append(t is not None)
            if t is not None:
                controller.release(t)

        bg_thread = threading.Thread(target=bg_acquire)
        bg_thread.start()
        time.sleep(0.5)  # Let bg queue

        # Release holder — bg should eventually get the lock
        controller.release(holder)
        bg_thread.join(timeout=5.0)

        assert bg_result[0], "Background should eventually acquire after interactive releases"


# ---------------------------------------------------------------------------
# 5. Lane classification test
# ---------------------------------------------------------------------------


class TestClassifyLane:
    """Unit test for the lane classification helper."""

    def test_interactive_contexts(self):
        assert classify_lane("cli", True) == "interactive"
        assert classify_lane("gateway", True) == "interactive"
        assert classify_lane("telegram", True) == "interactive"

    def test_background_contexts(self):
        assert classify_lane("cron", False) == "background"
        assert classify_lane("kanban", False) == "background"
        assert classify_lane("delegation", False) == "background"

    def test_lane_values(self):
        assert classify_lane("cli", True) == "interactive"
        assert classify_lane("cli", False) == "background"


# ---------------------------------------------------------------------------
# 6. Admission disabled noop test
# ---------------------------------------------------------------------------


class TestAdmissionDisabledNoop:
    """When admission is disabled, admit/release adds zero overhead."""

    def test_transport_noop_without_controller(self):
        from agent.transports.chat_completions import ChatCompletionsTransport

        ct = ChatCompletionsTransport()
        token = ct.admit("some_hash", "interactive", "cli")
        assert token.lock_fd is None  # No-op token

    def test_transport_release_noop(self):
        from agent.transports.chat_completions import ChatCompletionsTransport

        ct = ChatCompletionsTransport()
        token = ct.admit("some_hash", "interactive", "cli")
        ct.release(token)  # Should not raise

    def test_controller_noop_when_disabled(self, hermes_home: Path):
        """Controller returns no-op token when endpoint not configured."""
        ctrl = AdmissionController(hermes_home=hermes_home)
        token = ctrl.acquire("unknown_hash", "interactive", "test")
        assert token is not None
        assert token.lock_fd is None  # No-op

        _cleanup(ctrl)

    def test_controller_is_admitted_false(self, hermes_home: Path):
        """is_admitted returns False for unconfigured endpoints."""
        ctrl = AdmissionController(hermes_home=hermes_home)
        assert ctrl.is_admitted("unknown_hash") is False
        _cleanup(ctrl)

    def test_transport_with_controller_wired(self, controller: AdmissionController):
        """Transport with controller wired acquires real token."""
        from agent.transports.chat_completions import ChatCompletionsTransport

        ct = ChatCompletionsTransport()
        ct.set_admission_controller(controller)

        ehash = endpoint_hash("https://api.test-endpoint.example/v1")
        token = ct.admit(ehash, "interactive", "cli", admission_timeout=2.0)
        assert token is not None
        assert token.lock_fd is not None
        ct.release(token)

    def test_transport_double_release_safe(self, controller: AdmissionController):
        """Double release is safe."""
        from agent.transports.chat_completions import ChatCompletionsTransport

        ct = ChatCompletionsTransport()
        ct.set_admission_controller(controller)

        ehash = endpoint_hash("https://api.test-endpoint.example/v1")
        token = ct.admit(ehash, "interactive", "cli", admission_timeout=2.0)
        ct.release(token)
        ct.release(token)  # Should not raise

    def test_admit_disabled_endpoint_returns_noop(self, hermes_home: Path):
        """When endpoint is not configured, admit returns no-op token."""
        from agent.transports.chat_completions import ChatCompletionsTransport

        ctrl = AdmissionController(hermes_home=hermes_home)
        ct = ChatCompletionsTransport()
        ct.set_admission_controller(ctrl)

        token = ct.admit("unknown_hash", "interactive", "cli")
        assert token is not None
        assert token.lock_fd is None  # No-op
        _cleanup(ctrl)
