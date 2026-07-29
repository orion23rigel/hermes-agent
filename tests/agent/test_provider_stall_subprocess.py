from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class _DelayedCompletionHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        content_length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(content_length)
        time.sleep(0.20)
        payload = json.dumps(
            {
                "id": "late-response",
                "object": "chat.completion",
                "created": 0,
                "model": "test-model",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "too late"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
                },
            }
        ).encode("utf-8")
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except BrokenPipeError:
            pass

    def log_message(self, _format: str, *_args) -> None:
        return


def test_real_subprocess_provider_deadline_maps_to_kanban_retry_lifecycle(
    tmp_path, monkeypatch
):
    from hermes_cli import kanban_db as kb

    repo = Path(__file__).resolve().parents[2]
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    db_path = tmp_path / "kanban.db"
    claim_lock = f"{kb._claimer_id().split(':', 1)[0]}:subprocess-stall"
    with kb.connect(db_path) as conn:
        task_id = kb.create_task(
            conn,
            title="real subprocess provider stall",
            assignee="default",
            workspace_kind="scratch",
            workspace_path=str(workspace),
            initial_status="running",
        )
        task = kb.claim_task(conn, task_id, claimer=claim_lock)
        assert task is not None
        run_id = kb.get_task(conn, task_id).current_run_id
        assert run_id is not None

    server = ThreadingHTTPServer(("127.0.0.1", 0), _DelayedCompletionHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}/v1"

    program = r'''
import json
import sys
from cli import _kanban_worker_terminal_exit_code, _single_query_exit_code
from run_agent import AIAgent

agent = AIAgent(
    api_key="test-key",
    base_url=sys.argv[1],
    model="test-model",
    provider="openai",
    quiet_mode=True,
    skip_context_files=True,
    skip_memory=True,
)
agent._resolved_api_call_timeout = lambda: 0.05
agent._api_max_retries = 1
agent._try_recover_primary_transport = lambda *_args, **_kwargs: False
result = agent.run_conversation(
    user_message="reply briefly",
    conversation_history=[],
)
print(json.dumps({
    "failed": result.get("failed"),
    "error_code": result.get("error_code"),
    "retryable": result.get("retryable"),
}), flush=True)
code = _single_query_exit_code(result, kanban_worker=True)
raise SystemExit(_kanban_worker_terminal_exit_code(code))
'''

    env = os.environ.copy()
    for key in list(env):
        if key.endswith("_API_KEY") or key.endswith("_TOKEN"):
            env.pop(key)
    env.update(
        {
            "HERMES_HOME": str(tmp_path / "hermes-home"),
            "HERMES_KANBAN_TASK": task_id,
            "HERMES_KANBAN_WORKSPACE": str(workspace),
            "HERMES_KANBAN_DB": str(db_path),
            "HERMES_KANBAN_RUN_ID": str(run_id),
            "HERMES_KANBAN_CLAIM_LOCK": claim_lock,
            "HERMES_API_TIMEOUT": "0.05",
            "HERMES_STREAM_RETRIES": "0",
            # Kanban workers fail closed unless a summarization route exists.
            # This synthetic key satisfies preflight; the test provider itself
            # remains the loopback server above and no external request is made.
            "OPENROUTER_API_KEY": "test-key",
            "PYTHONPATH": str(repo),
        }
    )

    try:
        process = subprocess.Popen(
            [sys.executable, "-c", program, base_url],
            cwd=repo,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        stdout, stderr = process.communicate(timeout=30)
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=2)

    assert process.returncode == 74, f"stdout={stdout!r}\nstderr={stderr!r}"
    records = [
        json.loads(line)
        for line in stdout.splitlines()
        if line.startswith("{") and line.endswith("}")
    ]
    assert records[-1] == {
        "failed": True,
        "error_code": "provider_request_stalled",
        "retryable": True,
    }

    # Feed the real child's wait status through the production dispatcher path.
    monkeypatch.setenv("HERMES_KANBAN_DB", str(db_path))
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")
    with kb.connect(db_path) as conn:
        conn.execute(
            "UPDATE tasks SET worker_pid=? WHERE id=?",
            (process.pid, task_id),
        )
        conn.commit()
        kb._record_worker_exit(process.pid, process.returncode << 8)

        assert task_id not in kb.detect_crashed_workers(conn)
        assert task_id in getattr(
            kb.detect_crashed_workers, "_last_retryable_failures", []
        )
        task = kb.get_task(conn, task_id)
        assert task.status == "ready"
        assert task.consecutive_failures == 1
        run = conn.execute(
            "SELECT status, outcome FROM task_runs WHERE id=?", (run_id,)
        ).fetchone()
        assert dict(run) == {
            "status": "retryable_failure",
            "outcome": "retryable_failure",
        }
        assert kb.check_respawn_guard(conn, task_id) == "retryable_failure_cooldown"
