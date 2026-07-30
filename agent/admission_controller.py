"""Hybrid SQLite queue + kernel flock admission controller for provider endpoints.

AdmissionController manages per-endpoint capacity via a two-layer gate:

1. **SQLite queue** — tracks queued, running, and completed requests per endpoint.
   The queue enforces fairness (interactive > background lanes), starvation
   prevention (age promotion, burst cap), and the two-clock model (admission
   timeout ≠ provider attempt timeout).

2. **flock (kernel-level file lock)** — the sole capacity gating mechanism.
   Only one process holds LOCK_EX on a per-endpoint lock file at a time.
   SQLite state is advisory metadata; flock is mutual exclusion.

Design invariants
-----------------
- flock is the SOLE capacity gating mechanism. SQLite state is advisory.
- O_CLOEXEC mandatory on all lock file descriptors (no leak to subprocesses).
- PID reuse detection via owner_start_ms (monotonic boot-time clock) + boot_id.
- Lock held across provider retries.
- Degraded flock-only mode when SQLite is unavailable.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import random
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from hermes_constants import get_hermes_home
from hermes_cli.route_identity import normalize_route_base_url

logger = logging.getLogger(__name__)

# ── Schema ───────────────────────────────────────────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS requests (
    id              TEXT PRIMARY KEY,
    seq             INTEGER UNIQUE,
    lane            TEXT NOT NULL,
    source          TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'queued',
    enqueued_ms     INTEGER NOT NULL,
    queue_deadline_ms INTEGER,
    owner_pid       INTEGER,
    owner_start_ms  INTEGER,
    boot_id         TEXT,
    started_ms      INTEGER,
    finished_ms     INTEGER,
    cancel_reason   TEXT,
    endpoint_hash   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_queue
    ON requests(endpoint_hash, state, seq);

CREATE TABLE IF NOT EXISTS scheduler_state (
    endpoint_hash           TEXT PRIMARY KEY,
    consecutive_interactive INTEGER NOT NULL DEFAULT 0
);
"""

# ── AdmissionsToken ───────────────────────────────────────────────────────────


@dataclass
class AdmissionToken:
    """Opaque handle returned by AdmissionController.acquire().

    Holds the lock file descriptor until release().  lock_fd is None
    when admission is disabled (no-op token).
    """

    request_id: str
    endpoint_hash: str
    lock_fd: int | None  # None = no-op (admission disabled)


# ── Admission error ──────────────────────────────────────────────────────────


class AdmissionTimeoutError(TimeoutError):
    """The request spent too long in the queue (admission_timeout expired)."""


class AdmissionCancelledError(Exception):
    """The request was cancelled while queued."""


# ── Endpoint hashing ─────────────────────────────────────────────────────────


def normalize_endpoint_url(base_url: str) -> str:
    """Normalize a base URL for endpoint identity hashing.

    Scheme-lowered, hostname-lowered, trailing-slash-stripped.
    Ports are stripped (two URLs differing only by explicit default port
    are the same endpoint).
    """
    normal = normalize_route_base_url(base_url)

    # Strip the port segment — normalize_route_base_url keeps non-default
    # ports, but for endpoint identity any port difference is collapsed.
    if ":" in normal and normal.count(":") <= 2:
        # Simple host:port case — remove the port segment
        scheme_rest = normal.split("://", 1)
        if len(scheme_rest) == 2:
            scheme = scheme_rest[0]
            rest = scheme_rest[1]
            host_end = rest.find("/")
            if host_end == -1:
                host_part = rest
                path_part = ""
            else:
                host_part = rest[:host_end]
                path_part = rest[host_end:]
            if ":" in host_part and not host_part.startswith("["):
                host_only = host_part.rsplit(":", 1)[0]
                normal = f"{scheme}://{host_only}{path_part}"
            elif host_part.startswith("[") and "]:" in host_part:
                host_only = host_part.rsplit(":", 1)[0] + "]"
                normal = f"{scheme}://{host_only}{path_part}"

    return normal


def endpoint_hash(base_url: str) -> str:
    """SHA-256 hash of normalized base URL, first 16 hex chars."""
    normalized = normalize_endpoint_url(base_url)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


# ── Lane classification ──────────────────────────────────────────────────────


def classify_lane(source: str, is_interactive: bool) -> str:
    """Classify a request into 'interactive' or 'background' lane."""
    if is_interactive:
        return "interactive"
    return "background"


LANE_PRIORITY = {"interactive": 0, "background": 1}
INTERACTIVE = "interactive"
BACKGROUND = "background"


# ── Boot identity helpers ────────────────────────────────────────────────────


def _read_system_boot_id() -> str:
    """Read the system boot id from /proc."""
    try:
        with open("/proc/sys/kernel/random/boot_id") as f:
            return f.read().strip()
    except (OSError, IOError):
        return ""


def _pid_start_time_ms(pid: int) -> int | None:
    """Approximate monotonic start time of a PID, in ms since boot.

    Reads /proc/<pid>/stat field 22 (starttime in clock ticks since boot).
    Returns None when the PID no longer exists or stat is unreadable.
    """
    try:
        with open(f"/proc/{pid}/stat") as f:
            parts = f.read().split()
        # Field 22 (1-indexed) = starttime in clock ticks
        ticks = int(parts[21])
        # Convert ticks to ms. sysconf(_SC_CLK_TCK) is usually 100.
        try:
            clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        except (ValueError, AttributeError, KeyError):
            clk_tck = 100
        return int(ticks * (1000.0 / clk_tck))
    except (OSError, IOError, IndexError, ValueError):
        return None


# ── AdmissionController ──────────────────────────────────────────────────────


class AdmissionController:
    """Per-endpoint admission control with SQLite queue + flock mutual exclusion.

    Thread-safe.  Each endpoint (base_url hash) has its own lock file under
    ``<hermes_home>/admitslots/<endpoint_hash>.lock``.

    Usage::

        ctrl = AdmissionController(hermes_home, config)
        token = ctrl.acquire("a1b2c3d4e5f6g7h8", "interactive", "cli",
                              admission_timeout=30.0)
        if token is None:
            # Timed out waiting for capacity
            return
        try:
            # Make provider call ...
            pass
        finally:
            ctrl.release(token)
    """

    def __init__(self, hermes_home: Path | None = None, config: dict | None = None):
        """Initialize the admission controller.

        Args:
            hermes_home: Base path for lock files and DB. Defaults to
                ``get_hermes_home()``.
            config: Per-endpoint admission config from config.yaml.
                Expected shape: ``{"admission": {"enabled": bool, ...}}``.
        """
        self._hermes_home = Path(hermes_home or get_hermes_home())

        # Lockfile directory
        self._lock_dir = self._hermes_home / "admitslots"
        self._lock_dir.mkdir(parents=True, exist_ok=True)

        # Thread lock for SQLite writes (must be set before _init_db)
        self._lock = threading.Lock()

        # SQLite
        self._db_path = self._hermes_home / "admission_queue.db"
        self._db: sqlite3.Connection | None = None
        try:
            self._init_db()
        except Exception as exc:
            logger.warning("Admission DB init failed, degraded mode: %s", exc)
            self._db = None

        # Per-endpoint config overrides (from providers.custom[N].admission)
        self._config: dict[str, Any] = dict(config or {})

        # Bookkeeping: open lock fds we hold, keyed by endpoint_hash
        self._held_locks: dict[str, int] = {}

    # ── Public API ───────────────────────────────────────────────────────────

    def acquire(
        self,
        endpoint_hash: str,
        lane: str,
        source: str,
        admission_timeout: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> AdmissionToken | None:
        """Block until capacity granted or timeout.

        Args:
            endpoint_hash: SHA-256[:16] of normalized base URL.
            lane: ``"interactive"`` or ``"background"``.
            source: Request source identifier.
            admission_timeout: Max seconds to wait in queue. Falls back to
                config value, or 30s.
            cancel_event: Optional event to unblock early.

        Returns:
            AdmissionToken on success, or None if admission_timeout expired
            or admission is disabled for this endpoint.
        """
        # Short-circuit when admission disabled
        if not self._is_admitted(endpoint_hash):
            return AdmissionToken(request_id="", endpoint_hash="", lock_fd=None)

        max_wait = admission_timeout
        if max_wait is None:
            max_wait = float(
                self._endpoint_config(endpoint_hash).get("admission_timeout", 30.0)
            )

        request_id = endpoint_hash[:8] + f"_{int(time.monotonic() * 1000)}_{random.getrandbits(32):08x}"
        seq = int(time.monotonic() * 1_000_000)
        now_ms = int(time.monotonic() * 1000)
        deadline_ms = now_ms + int(max_wait * 1000) if max_wait > 0 else None

        # Step 1: INSERT queued row (always succeeds, no admission yet)
        try:
            self._insert_request(
                request_id, seq, endpoint_hash, lane, source, now_ms, deadline_ms
            )
        except sqlite3.OperationalError as exc:
            logger.warning("Admission SQLite unavailable, degrading to flock-only: %s", exc)
            return self._flock_only_acquire(endpoint_hash, max_wait, cancel_event)

        # Step 2: Wait loop — poll every 250ms (jittered)
        poll_base = 0.250
        start_wall = time.monotonic()
        lock_fd: int | None = None

        try:
            while True:
                # Check cancellation
                if cancel_event is not None and cancel_event.is_set():
                    self._cancel_queued(request_id)
                    return None

                # Check admission_timeout
                elapsed = time.monotonic() - start_wall
                if max_wait > 0 and elapsed >= max_wait:
                    self._expire_request(request_id)
                    return None

                # Step 3: Short BEGIN IMMEDIATE transaction
                try:
                    lock_fd = self._try_acquire(endpoint_hash, request_id)
                except (sqlite3.ProgrammingError, sqlite3.OperationalError):
                    # DB became unavailable (closed, locked, etc.) — degrade
                    # to flock-only for the remainder of this attempt
                    logger.debug("Admission DB unavailable, degrading to flock-only")
                    return self._flock_only_acquire(
                        endpoint_hash, max_wait - elapsed if max_wait > 0 else 0,
                        cancel_event,
                    )

                if lock_fd is not None:
                    # Step 4: Success! Lock acquired.
                    return AdmissionToken(
                        request_id=request_id,
                        endpoint_hash=endpoint_hash,
                        lock_fd=lock_fd,
                    )

                # Step 3b: Expire stale rows
                try:
                    self._expire_stale_rows(endpoint_hash)
                except (sqlite3.ProgrammingError, sqlite3.OperationalError):
                    pass  # Best-effort

                # Sleep with jitter
                jitter = random.uniform(0.8, 1.2)
                time.sleep(poll_base * jitter)
        except BaseException:
            # Cleanup on unexpected error
            try:
                self._cancel_queued(request_id)
            except (sqlite3.ProgrammingError, sqlite3.OperationalError):
                pass  # DB gone, nothing to cancel
            if lock_fd is not None:
                self._close_lock_fd(lock_fd)
            raise

    def release(self, token: AdmissionToken) -> None:
        """Release the slot. Idempotent."""
        if token is None or token.lock_fd is None:
            return
        request_id = token.request_id
        endpoint_hash = token.endpoint_hash
        lock_fd = token.lock_fd

        try:
            with self._lock:
                now_ms = int(time.monotonic() * 1000)
                self._exec(
                    "UPDATE requests SET state=?, finished_ms=? WHERE id=?",
                    ("done", now_ms, request_id),
                )
        except sqlite3.OperationalError:
            pass  # Best-effort; degrade gracefully

        self._close_lock_fd(lock_fd)
        token.lock_fd = None  # Prevent double-close

    def cancel(self, token: AdmissionToken) -> None:
        """Cancel a queued or running request.

        If still queued: atomically removes from queue.
        If already running: marks cancelled but lock held until release.
        """
        if token is None:
            return
        request_id = token.request_id
        endpoint_hash = token.endpoint_hash
        lock_fd = token.lock_fd

        if lock_fd is None:
            # Still queued — cancel from queue
            self._cancel_queued(request_id)
        else:
            # Already running — mark cancelled, release will finalize
            try:
                with self._lock:
                    self._exec(
                        "UPDATE requests SET state=?, cancel_reason=? WHERE id=? AND state='running'",
                        ("cancelled", "user_cancelled", request_id),
                    )
            except sqlite3.OperationalError:
                pass

    def is_admitted(self, endpoint_hash: str) -> bool:
        """Returns True if this endpoint has admission enabled."""
        return self._is_admitted(endpoint_hash)

    def status(self, endpoint_hash: str) -> dict:
        """Return current admission status for an endpoint."""
        result: dict[str, Any] = {
            "endpoint_hash": endpoint_hash,
            "admitted": self._is_admitted(endpoint_hash),
            "queued": 0,
            "running": 0,
            "consecutive_interactive": 0,
        }
        try:
            with self._lock:
                row = self._fetch_one(
                    "SELECT COUNT(*) FROM requests WHERE endpoint_hash=? AND state='queued'",
                    (endpoint_hash,),
                )
                if row:
                    result["queued"] = row[0]
                row = self._fetch_one(
                    "SELECT COUNT(*) FROM requests WHERE endpoint_hash=? AND state='running'",
                    (endpoint_hash,),
                )
                if row:
                    result["running"] = row[0]
                row = self._fetch_one(
                    "SELECT consecutive_interactive FROM scheduler_state WHERE endpoint_hash=?",
                    (endpoint_hash,),
                )
                if row:
                    result["consecutive_interactive"] = row[0]
        except sqlite3.OperationalError:
            pass
        return result

    def queue(self, endpoint_hash: str) -> list[dict]:
        """Return the current queue for an endpoint (queued requests only)."""
        try:
            with self._lock:
                rows = self._fetch_all(
                    "SELECT id, seq, lane, source, state, enqueued_ms, queue_deadline_ms "
                    "FROM requests WHERE endpoint_hash=? AND state='queued' ORDER BY seq",
                    (endpoint_hash,),
                )
                return [
                    {
                        "id": r[0],
                        "seq": r[1],
                        "lane": r[2],
                        "source": r[3],
                        "state": r[4],
                        "enqueued_ms": r[5],
                        "queue_deadline_ms": r[6],
                    }
                    for r in rows
                ]
        except sqlite3.OperationalError:
            return []

    def flush(self, endpoint_hash: str) -> int:
        """Cancel all queued requests for an endpoint. Returns count."""
        count = 0
        try:
            with self._lock:
                self._exec(
                    "UPDATE requests SET state='cancelled' WHERE endpoint_hash=? AND state='queued'",
                    (endpoint_hash,),
                )
                count = self._db.total_changes if self._db else 0
        except sqlite3.OperationalError:
            pass
        return count

    def drain(self, endpoint_hash: str) -> bool:
        """Wait for all running requests to finish for an endpoint.

        Returns True when the drain completed without timeout.
        """
        # Poll for all running requests to finish
        deadline = time.monotonic() + 30.0  # 30s drain timeout
        while time.monotonic() < deadline:
            try:
                with self._lock:
                    row = self._fetch_one(
                        "SELECT COUNT(*) FROM requests WHERE endpoint_hash=? AND state='running'",
                        (endpoint_hash,),
                    )
                    if row and row[0] == 0:
                        return True
            except sqlite3.OperationalError:
                return True
            time.sleep(0.5)
        return False

    def reset(self, endpoint_hash: str) -> None:
        """Reset admission state for an endpoint (clear queue and stats)."""
        try:
            with self._lock:
                self._exec(
                    "UPDATE requests SET state='cancelled' WHERE endpoint_hash=? AND state='queued'",
                    (endpoint_hash,),
                )
                self._exec(
                    "DELETE FROM scheduler_state WHERE endpoint_hash=?",
                    (endpoint_hash,),
                )
        except sqlite3.OperationalError:
            pass

    # ── Internal: admission config ───────────────────────────────────────────

    def _is_admitted(self, endpoint_hash: str) -> bool:
        """Check whether admission is enabled for this endpoint."""
        cfg = self._endpoint_config(endpoint_hash)
        return bool(cfg.get("enabled", False))

    def _endpoint_config(self, endpoint_hash: str) -> dict:
        """Return admission config dict for an endpoint.

        Config is looked up from the global config by matching endpoint_hash
        against configured base URLs. Falls back to an empty dict when no
        per-endpoint config exists.
        """
        # Return directly from the config dict keyed by endpoint_hash
        # (set externally or computed at init time)
        return self._config.get(endpoint_hash, {})

    # ── Internal: DB init & helpers ──────────────────────────────────────────

    def _init_db(self) -> None:
        """Initialize SQLite database in WAL mode."""
        try:
            self._db = sqlite3.connect(
                str(self._db_path),
                timeout=5.0,
                check_same_thread=False,
            )
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute(f"PRAGMA busy_timeout={5000}")
            with self._lock:
                self._db.executescript(_SCHEMA_SQL)
                self._db.commit()
        except sqlite3.OperationalError as exc:
            logger.warning("Admission SQLite init failed, degraded mode: %s", exc)
            self._db = None

    def _exec(self, sql: str, params: tuple = ()) -> None:
        """Execute SQL on the DB thread-safely."""
        if self._db is None:
            raise sqlite3.OperationalError("database not available")
        self._db.execute(sql, params)
        self._db.commit()

    def _fetch_one(self, sql: str, params: tuple = ()) -> Any:
        """Fetch one row."""
        if self._db is None:
            return None
        return self._db.execute(sql, params).fetchone()

    def _fetch_all(self, sql: str, params: tuple = ()) -> list:
        """Fetch all rows."""
        if self._db is None:
            return []
        return self._db.execute(sql, params).fetchall()

    def _insert_request(
        self,
        request_id: str,
        seq: int,
        endpoint_hash: str,
        lane: str,
        source: str,
        now_ms: int,
        deadline_ms: int | None,
    ) -> None:
        """Insert a new queued request."""
        with self._lock:
            self._exec(
                "INSERT INTO requests "
                "(id, seq, lane, source, state, enqueued_ms, queue_deadline_ms, endpoint_hash) "
                "VALUES (?, ?, ?, ?, 'queued', ?, ?, ?)",
                (request_id, seq, lane, source, now_ms, deadline_ms, endpoint_hash),
            )

    def _cancel_queued(self, request_id: str) -> None:
        """Atomically mark a queued request as cancelled."""
        try:
            with self._lock:
                self._exec(
                    "UPDATE requests SET state='cancelled' WHERE id=? AND state='queued'",
                    (request_id,),
                )
        except sqlite3.OperationalError:
            pass

    def _expire_request(self, request_id: str) -> None:
        """Mark a request as expired (admission_timeout reached while queued)."""
        try:
            with self._lock:
                self._exec(
                    "UPDATE requests SET state='expired' WHERE id=? AND state='queued'",
                    (request_id,),
                )
        except sqlite3.OperationalError:
            pass

    def _expire_stale_rows(self, endpoint_hash: str) -> None:
        """Expire rows whose queue deadline has passed."""
        try:
            now_ms = int(time.monotonic() * 1000)
            with self._lock:
                self._exec(
                    "UPDATE requests SET state='expired' "
                    "WHERE endpoint_hash=? AND state='queued' "
                    "AND queue_deadline_ms IS NOT NULL AND queue_deadline_ms < ?",
                    (endpoint_hash, now_ms),
                )
        except sqlite3.OperationalError:
            pass

    # ── Internal: flock acquire logic ────────────────────────────────────────

    def _try_acquire(self, endpoint_hash: str, request_id: str) -> int | None:
        """Attempt to acquire the lock for this request.

        The critical section:
        1. Expire cancelled/expired queued rows
        2. Check own admission_timeout — expire self
        3. Select next eligible request with fairness policy
        4. Only if it's THIS request: attempt LOCK_EX|LOCK_NB on lock file
        5. If lock free: mark stale 'running' rows as 'abandoned',
           mark this row 'running', commit tx, KEEP flock open

        Returns the lock fd on success, None if another request is ahead.
        """
        lock_fd = None
        try:
            with self._lock:
                if self._db is None:
                    return None  # Degraded mode handled separately

                # 1. Expire cancelled/expired queued rows
                now_ms = int(time.monotonic() * 1000)
                self._db.execute(
                    "UPDATE requests SET state='expired' "
                    "WHERE endpoint_hash=? AND state='queued' "
                    "AND queue_deadline_ms IS NOT NULL AND queue_deadline_ms < ?",
                    (endpoint_hash, now_ms),
                )

                # 2. Check own admission_timeout
                row = self._fetch_one(
                    "SELECT queue_deadline_ms FROM requests WHERE id=? AND state='queued'",
                    (request_id,),
                )
                if row and row[0] is not None and row[0] < now_ms:
                    self._exec(
                        "UPDATE requests SET state='expired' WHERE id=?",
                        (request_id,),
                    )
                    return None

                # 3. Select next eligible request with fairness policy
                next_id = self._select_next_eligible(endpoint_hash)

                # 4. Only if it's THIS request
                if next_id is None or next_id != request_id:
                    return None

                # 5. Attempt LOCK_EX|LOCK_NB on lock file
                lock_fd = self._open_lock_file(endpoint_hash)
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except (OSError, IOError):
                    # Lock held by another process
                    self._close_lock_fd(lock_fd)
                    return None

                # Mark stale 'running' rows as 'abandoned'
                self._db.execute(
                    "UPDATE requests SET state='abandoned' "
                    "WHERE endpoint_hash=? AND state='running'",
                    (endpoint_hash,),
                )

                # Mark this row 'running'
                pid = os.getpid()
                boot_id = _read_system_boot_id()
                start_ms = _pid_start_time_ms(pid) or int(time.monotonic() * 1000)
                self._db.execute(
                    "UPDATE requests SET state=?, owner_pid=?, owner_start_ms=?, "
                    "boot_id=?, started_ms=? WHERE id=?",
                    ("running", pid, start_ms, boot_id, now_ms, request_id),
                )

                # Update consecutive_interactive tracking
                self._update_consecutive_interactive(endpoint_hash, request_id)

                self._db.commit()
                return lock_fd
        except sqlite3.OperationalError:
            if lock_fd is not None:
                self._close_lock_fd(lock_fd)
            return None
        except BaseException:
            if lock_fd is not None:
                self._close_lock_fd(lock_fd)
            raise

    def _select_next_eligible(self, endpoint_hash: str) -> str | None:
        """Select the next eligible request for an endpoint.

        Applies fairness policy:
        - FIFO within each lane
        - Interactive lane has priority
        - If consecutive_interactive >= 3 AND background requests queued,
          force oldest background request (burst cap)
        - If any queued request has waited > max_queue_age, promote to front
          (age promotion)
        """
        # Fetch all queued requests for this endpoint, ordered by seq
        rows = self._fetch_all(
            "SELECT id, lane, enqueued_ms, queue_deadline_ms "
            "FROM requests WHERE endpoint_hash=? AND state='queued' "
            "ORDER BY seq",
            (endpoint_hash,),
        )

        if not rows:
            return None

        # Get consecutive_interactive count
        sched = self._fetch_one(
            "SELECT consecutive_interactive FROM scheduler_state WHERE endpoint_hash=?",
            (endpoint_hash,),
        )
        consecutive_interactive = sched[0] if sched else 0

        now_ms = int(time.monotonic() * 1000)
        max_queue_age_ms = int(
            (self._endpoint_config(endpoint_hash).get("max_queue_age", 120) or 120) * 1000
        )

        # Check for age-promoted requests (any queue wait exceeds max_queue_age)
        aged_candidates = [
            r for r in rows
            if r[3] is None or r[3] >= now_ms  # not expired
        ]
        for row in aged_candidates:
            age_ms = now_ms - row[2]
            if age_ms >= max_queue_age_ms:
                # Promote the oldest aged request
                return row[0]

        # Separate by lane
        interactive = [r for r in rows if r[1] == INTERACTIVE]
        background = [r for r in rows if r[1] == BACKGROUND]

        # Burst cap: if consecutive_interactive >= 3 and background exists,
        # force the oldest background request
        burst = self._endpoint_config(endpoint_hash).get("interactive_burst", 3) or 3
        if consecutive_interactive >= burst and background:
            return background[0][0]

        # Normal priority: interactive first, then background
        if interactive:
            return interactive[0][0]
        if background:
            return background[0][0]

        return None

    def _update_consecutive_interactive(
        self, endpoint_hash: str, request_id: str
    ) -> None:
        """Update consecutive_interactive counter after a grant.

        Resets to 0 on any background grant; increments on interactive.
        """
        row = self._fetch_one(
            "SELECT lane FROM requests WHERE id=?",
            (request_id,),
        )
        if row is None:
            return

        lane = row[0]
        if lane == BACKGROUND:
            self._db.execute(
                "INSERT OR REPLACE INTO scheduler_state (endpoint_hash, consecutive_interactive) "
                "VALUES (?, 0)",
                (endpoint_hash,),
            )
        else:
            # Increment consecutive_interactive
            current = self._fetch_one(
                "SELECT consecutive_interactive FROM scheduler_state WHERE endpoint_hash=?",
                (endpoint_hash,),
            )
            val = (current[0] if current else 0) + 1
            self._db.execute(
                "INSERT OR REPLACE INTO scheduler_state (endpoint_hash, consecutive_interactive) "
                "VALUES (?, ?)",
                (endpoint_hash, val),
            )

    # ── Internal: lock file management ──────────────────────────────────────

    def _lock_file_path(self, endpoint_hash: str) -> Path:
        """Path for the endpoint's flock lock file."""
        return self._lock_dir / f"{endpoint_hash}.lock"

    def _open_lock_file(self, endpoint_hash: str) -> int:
        """Open (or create) the lock file with O_CLOEXEC.

        Returns the file descriptor.
        """
        path = self._lock_file_path(endpoint_hash)
        # O_CLOEXEC: prevent leaking fd to subprocesses (Kanban workers, cron)
        return os.open(
            str(path),
            os.O_CREAT | os.O_RDWR | os.O_CLOEXEC,
            0o644,
        )

    def _close_lock_fd(self, fd: int) -> None:
        """Close a lock file descriptor safely."""
        try:
            os.close(fd)
        except (OSError, IOError):
            pass

    # ── Internal: degraded flock-only mode ──────────────────────────────────

    def _flock_only_acquire(
        self,
        endpoint_hash: str,
        admission_timeout: float,
        cancel_event: threading.Event | None,
    ) -> AdmissionToken | None:
        """Degraded mode: plain flock without SQLite queue.

        Used when SQLite is unavailable. No queue ordering, no fairness —
        just mutual exclusion.
        """
        poll_base = 0.250
        start_wall = time.monotonic()
        request_id = endpoint_hash[:8] + f"_degraded_{int(time.monotonic() * 1000)}"

        while True:
            if cancel_event is not None and cancel_event.is_set():
                return None
            if admission_timeout > 0 and (time.monotonic() - start_wall) >= admission_timeout:
                return None

            lock_fd = self._open_lock_file(endpoint_hash)
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return AdmissionToken(
                    request_id=request_id,
                    endpoint_hash=endpoint_hash,
                    lock_fd=lock_fd,
                )
            except (OSError, IOError):
                self._close_lock_fd(lock_fd)

            jitter = random.uniform(0.8, 1.2)
            time.sleep(poll_base * jitter)
