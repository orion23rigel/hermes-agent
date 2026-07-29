from types import SimpleNamespace

import pytest

import cli


@pytest.fixture(autouse=True)
def reset_single_query_finalize_state(monkeypatch):
    monkeypatch.setattr(cli, "_single_query_finalize_attempted_session_ids", set())
    monkeypatch.setattr(cli, "_cleanup_done", False)


def test_finalize_single_query_runs_cleanup_without_reemitting_finalize_before_release(monkeypatch):
    calls = []
    fake_cli = SimpleNamespace(_release_active_session=lambda: calls.append(("release", {})))

    def cleanup(**kwargs):
        calls.append(("cleanup", kwargs))

    monkeypatch.setattr(
        cli,
        "_notify_single_query_session_finalize",
        lambda _cli: calls.append(("finalize", {})),
    )
    monkeypatch.setattr(cli, "_run_cleanup", cleanup)

    cli._finalize_single_query(fake_cli)

    assert calls == [
        ("finalize", {}),
        ("cleanup", {"notify_session_finalize": False}),
        ("release", {}),
    ]


def test_finalize_single_query_releases_session_when_cleanup_fails(monkeypatch):
    calls = []
    fake_cli = SimpleNamespace(_release_active_session=lambda: calls.append("release"))

    def cleanup(**kwargs):
        calls.append("cleanup")
        raise RuntimeError("cleanup failed")

    monkeypatch.setattr(
        cli,
        "_notify_single_query_session_finalize",
        lambda _cli: calls.append("finalize"),
    )
    monkeypatch.setattr(cli, "_run_cleanup", cleanup)

    with pytest.raises(RuntimeError, match="cleanup failed"):
        cli._finalize_single_query(fake_cli)

    assert calls == ["finalize", "cleanup", "release"]


def test_finalize_single_query_runs_cleanup_when_finalize_hook_fails(monkeypatch):
    calls = []
    fake_agent = SimpleNamespace(session_id="agent-session", platform="cli")
    fake_cli = SimpleNamespace(
        agent=fake_agent,
        session_id="cli-session",
        _release_active_session=lambda: calls.append("release"),
    )

    def invoke_hook(name, **kwargs):
        calls.append("finalize")
        raise RuntimeError("hook failed")

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", invoke_hook)
    monkeypatch.setattr(cli, "_run_cleanup", lambda **kwargs: calls.append("cleanup"))

    cli._finalize_single_query(fake_cli)

    assert calls == ["finalize", "cleanup", "release"]


def test_finalize_single_query_signal_window_does_not_reemit_during_atexit(monkeypatch):
    calls = []
    fake_agent = SimpleNamespace(session_id="agent-session", platform="cli")
    fake_cli = SimpleNamespace(
        agent=fake_agent,
        session_id="cli-session",
        _release_active_session=lambda: calls.append(("release", {})),
    )

    def invoke_hook(name, **kwargs):
        calls.append((name, kwargs))

    def interrupted_cleanup(**_kwargs):
        raise KeyboardInterrupt()

    expected_finalize = (
        "on_session_finalize",
        {
            "session_id": "agent-session",
            "platform": "cli",
            "reason": "shutdown",
        },
    )

    original_run_cleanup = cli._run_cleanup
    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", invoke_hook)
    monkeypatch.setattr(cli, "_run_cleanup", interrupted_cleanup)

    with pytest.raises(KeyboardInterrupt):
        cli._finalize_single_query(fake_cli)

    assert calls == [expected_finalize, ("release", {})]

    # Simulate later atexit cleanup after the interrupted one-shot path. The
    # active agent may already be unavailable by then.
    monkeypatch.setattr(cli, "_run_cleanup", original_run_cleanup)
    monkeypatch.setattr(cli, "_active_agent_ref", None)
    monkeypatch.setattr(cli, "_reset_terminal_input_modes_on_exit", lambda: None)
    monkeypatch.setattr(cli, "_cleanup_all_terminals", lambda: None)
    monkeypatch.setattr(cli, "_cleanup_all_browsers", lambda: None)
    monkeypatch.setattr("tools.mcp_tool.shutdown_mcp_servers", lambda: None)
    monkeypatch.setattr("agent.auxiliary_client.shutdown_cached_clients", lambda: None)

    cli._run_cleanup()

    assert calls == [expected_finalize, ("release", {})]


def test_notify_single_query_session_finalize_uses_agent_session(monkeypatch):
    calls = []
    fake_agent = SimpleNamespace(session_id="agent-session", platform="cli")
    fake_cli = SimpleNamespace(agent=fake_agent, session_id="cli-session")

    def invoke_hook(name, **kwargs):
        calls.append((name, kwargs))

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", invoke_hook)

    cli._notify_single_query_session_finalize(fake_cli)

    assert calls == [
        (
            "on_session_finalize",
            {
                "session_id": "agent-session",
                "platform": "cli",
                "reason": "shutdown",
            },
        )
    ]


def test_human_single_query_main_finalizes_after_query(monkeypatch):
    calls = []

    import cli as cli_mod

    class _Console:
        def print(self, *_args, **_kwargs):
            calls.append("query-label")

    class FakeCLI:
        def __init__(self, **_kwargs):
            self.console = _Console()
            self.session_id = "single-query-session"
            self.agent = SimpleNamespace(
                session_id="single-query-session",
                platform="cli",
            )

        def _claim_active_session(self, surface, *, stderr=False):
            calls.append(("claim", surface, stderr))
            return True

        def _show_security_advisories(self):
            calls.append("advisories")

        def chat(self, query, images=None):
            calls.append(("chat", query, images))
            # The interactive chat API is side-effecting and may return None on
            # success; the initialized agent, not a response sentinel, proves it ran.
            return None

        def _print_exit_summary(self, clear_screen=True):
            calls.append("summary")

    monkeypatch.setattr(cli_mod, "HermesCLI", FakeCLI)
    monkeypatch.setattr(cli_mod.atexit, "register", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        cli_mod,
        "_finalize_single_query",
        lambda fake_cli: calls.append(("finalize", fake_cli.session_id)),
    )

    cli_mod.main(query="hello", quiet=False, toolsets="terminal")

    assert calls == [
        ("claim", "cli", False),
        "query-label",
        "advisories",
        ("chat", "hello", None),
        "summary",
        ("finalize", "single-query-session"),
    ]


def test_quiet_single_query_main_finalizes_while_preserving_exit_code(monkeypatch):
    calls = []

    import cli as cli_mod

    def run_conversation(*, user_message, conversation_history):
        calls.append(("run", user_message, conversation_history))
        return {
            "final_response": "",
            "error": "provider failed",
            "failed": True,
        }

    class FakeCLI:
        def __init__(self, **_kwargs):
            self.provider = "test-provider"
            self.model = "test-model"
            self.session_id = "quiet-session"
            self.conversation_history = []
            self._active_agent_route_signature = "same-route"
            self.agent = SimpleNamespace(
                session_id="quiet-session",
                platform="cli",
                quiet_mode=False,
                suppress_status_output=False,
                stream_delta_callback=object(),
                tool_gen_callback=object(),
                run_conversation=run_conversation,
            )

        def _claim_active_session(self, surface, *, stderr=False):
            calls.append(("claim", surface, stderr))
            return True

        def _ensure_runtime_credentials(self):
            calls.append("credentials")
            return True

        def _resolve_turn_agent_config(self, effective_query):
            calls.append(("resolve", effective_query))
            return {
                "signature": "same-route",
                "model": None,
                "runtime": None,
                "request_overrides": None,
            }

        def _init_agent(self, **kwargs):
            calls.append(("init", kwargs))
            return True

    monkeypatch.delenv("HERMES_KANBAN_TASK", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_GOAL_MODE", raising=False)
    monkeypatch.setattr(cli_mod, "HermesCLI", FakeCLI)
    monkeypatch.setattr(cli_mod.atexit, "register", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        cli_mod,
        "_finalize_single_query",
        lambda fake_cli: calls.append(("finalize", fake_cli.session_id)),
    )

    with pytest.raises(SystemExit) as exc_info:
        cli_mod.main(query="hello", quiet=True, toolsets="terminal")

    assert exc_info.value.code == 1
    assert ("claim", "cli", True) in calls
    assert ("run", "hello", []) in calls
    assert calls[-1] == ("finalize", "quiet-session")


@pytest.mark.parametrize("init_exit_code", [1, 78])
def test_quiet_worker_init_failure_honors_classified_exit_marker(
    tmp_path, monkeypatch, init_exit_code
):
    import cli as cli_mod

    class FakeCLI:
        def __init__(self, **_kwargs):
            self.provider = "test-provider"
            self.model = "test-model"
            self.session_id = "quiet-init-failure"
            self.conversation_history = []
            self._active_agent_route_signature = "same-route"
            self.agent = None
            self._agent_init_exit_code = init_exit_code

        def _claim_active_session(self, _surface, *, stderr=False):
            return True

        def _ensure_runtime_credentials(self):
            return True

        def _resolve_turn_agent_config(self, _effective_query):
            return {
                "signature": "same-route",
                "model": None,
                "runtime": None,
                "request_overrides": None,
            }

        def _init_agent(self, **_kwargs):
            return False

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_quiet_init")
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "kanban.db"))
    monkeypatch.delenv("HERMES_KANBAN_GOAL_MODE", raising=False)
    monkeypatch.setattr(cli_mod, "HermesCLI", FakeCLI)
    monkeypatch.setattr(cli_mod.atexit, "register", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli_mod, "_finalize_single_query", lambda _cli: None)

    with pytest.raises(SystemExit) as exc_info:
        cli_mod.main(query="hello", quiet=True, toolsets="terminal")

    assert exc_info.value.code == init_exit_code


@pytest.mark.parametrize("init_exit_code", [1, 78])
def test_human_worker_init_failure_honors_classified_exit_marker(
    tmp_path, monkeypatch, init_exit_code
):
    import cli as cli_mod

    class _Console:
        def print(self, *_args, **_kwargs):
            pass

    class FakeCLI:
        def __init__(self, **_kwargs):
            self.console = _Console()
            self.session_id = "human-init-failure"
            self.agent = None
            self._agent_init_exit_code = init_exit_code

        def _claim_active_session(self, _surface, *, stderr=False):
            return True

        def _show_security_advisories(self):
            pass

        def chat(self, _query, images=None):
            return None

        def _print_exit_summary(self, clear_screen=True):
            pass

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_human_init")
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "kanban.db"))
    monkeypatch.setattr(cli_mod, "HermesCLI", FakeCLI)
    monkeypatch.setattr(cli_mod.atexit, "register", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli_mod, "_finalize_single_query", lambda _cli: None)

    with pytest.raises(SystemExit) as exc_info:
        cli_mod.main(query="hello", quiet=False, toolsets="terminal")

    assert exc_info.value.code == init_exit_code


@pytest.mark.parametrize(
    ("result", "kanban_worker", "expected"),
    [
        ({}, False, 0),
        ({}, True, 0),
        ({"failed": True}, False, 1),
        ({"failed": True}, True, 1),
        ({"failed": True, "failure_reason": "rate_limit"}, False, 1),
        ({"failed": True, "failure_reason": "rate_limit"}, True, 75),
        ({"failed": True, "failure_reason": "billing"}, True, 75),
        (
            {
                "failed": True,
                "error_code": "provider_request_stalled",
                "retryable": True,
            },
            False,
            1,
        ),
        (
            {
                "failed": True,
                "error_code": "provider_request_stalled",
                "retryable": True,
            },
            True,
            74,
        ),
        (
            {
                "failed": True,
                "error_code": "provider_request_stalled",
                "retryable": False,
            },
            True,
            1,
        ),
    ],
)
def test_single_query_exit_code_is_scoped_to_kanban_workers(
    result, kanban_worker, expected
):
    import cli as cli_mod

    assert cli_mod._single_query_exit_code(
        result, kanban_worker=kanban_worker
    ) == expected


def test_goal_mode_initial_provider_stall_skips_goal_loop_and_exits_74(
    monkeypatch,
):
    import cli as cli_mod

    stalled = {
        "final_response": "",
        "failed": True,
        "error": "provider request stalled",
        "error_code": "provider_request_stalled",
        "retryable": True,
    }

    class FakeCLI:
        def __init__(self, **_kwargs):
            self.provider = "test-provider"
            self.model = "test-model"
            self.session_id = "goal-stall"
            self.conversation_history = []
            self._active_agent_route_signature = "same-route"
            self.agent = SimpleNamespace(
                session_id="goal-stall",
                platform="cli",
                quiet_mode=False,
                suppress_status_output=False,
                stream_delta_callback=object(),
                tool_gen_callback=object(),
                run_conversation=lambda **_kwargs: stalled,
            )

        def _claim_active_session(self, _surface, *, stderr=False):
            return True

        def _ensure_runtime_credentials(self):
            return True

        def _resolve_turn_agent_config(self, _query):
            return {
                "signature": "same-route",
                "model": None,
                "runtime": None,
                "request_overrides": None,
            }

        def _init_agent(self, **_kwargs):
            return True

    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_goal_stall")
    monkeypatch.setenv("HERMES_KANBAN_GOAL_MODE", "1")
    monkeypatch.setattr(cli_mod, "HermesCLI", FakeCLI)
    monkeypatch.setattr(cli_mod.atexit, "register", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli_mod, "_finalize_single_query", lambda _cli: None)
    monkeypatch.setattr(
        cli_mod,
        "_run_kanban_goal_loop_q",
        lambda *_args, **_kwargs: pytest.fail("goal loop must not run"),
    )

    with pytest.raises(SystemExit) as exc_info:
        cli_mod.main(query="hello", quiet=True, toolsets="terminal")

    assert exc_info.value.code == 74


def test_goal_mode_later_provider_stall_is_returned_promptly(
    monkeypatch, tmp_path,
):
    import cli as cli_mod
    from hermes_cli import goals
    from hermes_cli import kanban_db as kb

    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "kanban.db"))
    with kb.connect() as conn:
        task_id = kb.create_task(
            conn,
            title="goal stall",
            body="finish safely",
            assignee="engineer",
            goal_mode=True,
        )
    monkeypatch.setenv("HERMES_KANBAN_TASK", task_id)

    stalled = {
        "final_response": "",
        "failed": True,
        "error_code": "provider_request_stalled",
        "retryable": True,
    }
    fake_cli = SimpleNamespace(
        session_id="goal-session",
        conversation_history=[],
        agent=SimpleNamespace(
            session_id="goal-session",
            run_conversation=lambda **_kwargs: stalled,
        ),
    )

    def run_one_more_turn(**kwargs):
        kwargs["run_turn"]("continue")

    monkeypatch.setattr(goals, "run_kanban_goal_loop", run_one_more_turn)

    assert cli_mod._run_kanban_goal_loop_q(fake_cli, "first") is stalled
