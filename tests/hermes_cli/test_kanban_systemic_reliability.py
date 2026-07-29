"""RED contracts for systemic Kanban workspace and DAG reliability.

These tests intentionally exercise public DB behavior with real temporary Git
repositories.  They are regression specifications for failure classes observed
on the live board; they must not depend on or mutate the live Hermes home.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from hermes_cli import kanban_db as kb


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.delenv("HERMES_KANBAN_DB", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_BOARD", raising=False)
    monkeypatch.delenv("HERMES_KANBAN_WORKSPACES_ROOT", raising=False)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    return home


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _init_git_repo(repo: Path) -> None:
    repo.mkdir(parents=True)
    subprocess.run(
        ["git", "init", "-b", "main", str(repo)],
        check=True,
        capture_output=True,
        text=True,
    )
    _git(repo, "config", "user.email", "kanban-systemic@example.invalid")
    _git(repo, "config", "user.name", "Kanban Systemic Test")
    (repo / "README.md").write_text("initial\n", encoding="utf-8")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-m", "initial")


def _task_count(conn) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])


def test_create_rejects_invalid_worktree_before_inserting(
    kanban_home, tmp_path, monkeypatch
):
    """A runnable worktree card must have a resolvable Git anchor at creation.

    Missing, relative, and absolute non-Git paths are deterministic input
    errors.  None may leave a row/event/link behind for the dispatcher to
    discover later.
    """
    decoy_repo = tmp_path / "decoy"
    _init_git_repo(decoy_repo)
    monkeypatch.chdir(decoy_repo)
    non_repo = tmp_path / "not-a-repository"
    non_repo.mkdir()

    invalid_paths = (None, "relative/repository", str(non_repo))
    with kb.connect() as conn:
        assert _task_count(conn) == 0
        for workspace_path in invalid_paths:
            with pytest.raises(ValueError, match="(?i)worktree|default_workdir|git"):
                kb.create_task(
                    conn,
                    title="must never be inserted",
                    assignee="engineer",
                    workspace_kind="worktree",
                    workspace_path=workspace_path,
                )
            assert _task_count(conn) == 0
            assert conn.execute("SELECT COUNT(*) FROM task_events").fetchone()[0] == 0
            assert conn.execute("SELECT COUNT(*) FROM task_links").fetchone()[0] == 0


def test_decompose_rejects_invalid_worktree_dag_atomically(kanban_home):
    """One invalid child rejects the complete fan-out before any child exists."""
    with kb.connect() as conn:
        root_id = kb.create_task(
            conn,
            title="foundation",
            assignee="orchestrator",
            workspace_kind="scratch",
            triage=True,
        )
        before_tasks = _task_count(conn)
        before_events = int(
            conn.execute("SELECT COUNT(*) FROM task_events").fetchone()[0]
        )

        with pytest.raises(ValueError, match="(?i)worktree|default_workdir|git"):
            kb.decompose_triage_task(
                conn,
                root_id,
                root_assignee="orchestrator",
                author="decomposer",
                children=[
                    {
                        "title": "valid research wave",
                        "assignee": "reviewer",
                        "workspace_kind": "scratch",
                    },
                    {
                        "title": "invalid implementation wave",
                        "assignee": "engineer",
                        "parents": [0],
                        "workspace_kind": "worktree",
                    },
                ],
            )

        root = kb.get_task(conn, root_id)
        assert root is not None
        assert root.status == "triage"
        assert root.assignee == "orchestrator"
        assert _task_count(conn) == before_tasks
        assert conn.execute("SELECT COUNT(*) FROM task_links").fetchone()[0] == 0
        assert (
            conn.execute("SELECT COUNT(*) FROM task_events").fetchone()[0]
            == before_events
        )
        assert not any(event.kind == "decomposed" for event in kb.list_events(conn, root_id))


def test_legacy_invalid_worktree_blocks_after_one_dispatch_attempt(
    kanban_home, monkeypatch
):
    """Legacy bad rows fail fast instead of consuming the transient retry budget."""
    import hermes_cli.profiles as profiles

    monkeypatch.setattr(profiles, "profile_exists", lambda _name: True)

    with kb.connect() as conn:
        # Simulate a row created by an older Hermes version.  New public task
        # creation must reject this shape, so the migration path is exercised
        # by inserting only the minimal legacy state directly.
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, assignee, status, created_at,
                workspace_kind, workspace_path
            ) VALUES (
                't_legacy_invalid_workspace', 'legacy invalid workspace',
                'engineer', 'ready', 1, 'worktree', NULL
            )
            """
        )
        conn.commit()

        spawn_calls: list[str] = []

        def must_not_spawn(task, workspace, board=None):
            spawn_calls.append(task.id)
            return None

        result = kb.dispatch_once(
            conn,
            spawn_fn=must_not_spawn,
            failure_limit=3,
        )

        task = kb.get_task(conn, "t_legacy_invalid_workspace")
        assert task is not None
        assert task.status == "blocked"
        assert task.consecutive_failures == 1
        assert "t_legacy_invalid_workspace" in result.auto_blocked
        assert spawn_calls == []

        runs = conn.execute(
            """
            SELECT status, outcome, error
              FROM task_runs
             WHERE task_id = ?
             ORDER BY id
            """,
            (task.id,),
        ).fetchall()
        assert len(runs) == 1
        assert runs[0]["outcome"] == "gave_up"
        assert "default_workdir" in (runs[0]["error"] or "")

        failure_events = [
            event
            for event in kb.list_events(conn, task.id)
            if event.kind in {"spawn_failed", "gave_up"}
        ]
        assert [event.kind for event in failure_events] == ["gave_up"]
        assert failure_events[0].payload["deterministic"] is True


def test_supersede_subtree_is_atomic_and_never_promotes_descendants(kanban_home):
    """Supersession archives the failed DAG as one replacement-linked unit."""
    with kb.connect() as conn:
        root_id = kb.create_task(conn, title="failed foundation")
        child_id = kb.create_task(
            conn,
            title="dependent implementation",
            parents=[root_id],
        )
        grandchild_id = kb.create_task(
            conn,
            title="dependent review",
            parents=[child_id],
        )
        replacement_id = kb.create_task(conn, title="replacement foundation")

        before = {
            task_id: kb.get_task(conn, task_id).status
            for task_id in (root_id, child_id, grandchild_id, replacement_id)
        }
        with pytest.raises(ValueError, match="(?i)replacement|unknown"):
            kb.supersede_subtree(
                conn,
                root_id,
                "t_missing_replacement",
                reason="invalid replacement must roll back",
                actor="operator",
            )
        assert {
            task_id: kb.get_task(conn, task_id).status
            for task_id in before
        } == before
        assert not conn.execute(
            "SELECT 1 FROM task_events WHERE kind = 'superseded'"
        ).fetchone()

        affected = kb.supersede_subtree(
            conn,
            root_id,
            replacement_id,
            reason="replace failed foundation and every stale dependent",
            actor="operator",
        )
        assert set(affected) == {root_id, child_id, grandchild_id}

        for task_id in affected:
            task = kb.get_task(conn, task_id)
            assert task is not None
            assert task.status == "archived"
            assert task.superseded_by == replacement_id
            events = [
                event
                for event in kb.list_events(conn, task_id)
                if event.kind == "superseded"
            ]
            assert len(events) == 1
            assert events[0].payload["replacement_task_id"] == replacement_id

        replacement = kb.get_task(conn, replacement_id)
        assert replacement is not None
        assert replacement.status == "ready"
        assert replacement.superseded_by is None

        # Dispatcher promotion passes can never resurrect the cancelled DAG.
        assert kb.recompute_ready(conn) == 0
        for task_id in affected:
            assert kb.get_task(conn, task_id).status == "archived"
            superseded_event_id = max(
                event.id
                for event in kb.list_events(conn, task_id)
                if event.kind == "superseded"
            )
            assert not conn.execute(
                """
                SELECT 1 FROM task_events
                 WHERE task_id = ?
                   AND id > ?
                   AND kind IN ('promoted', 'spawned', 'claimed')
                """,
                (task_id, superseded_event_id),
            ).fetchone()


def test_supersede_fences_a_spawn_already_in_progress(
    kanban_home, monkeypatch
):
    """A cancelled claim cannot restore its PID after supersession commits."""
    import hermes_cli.profiles as profiles

    monkeypatch.setattr(profiles, "profile_exists", lambda _name: True)
    terminated: list[tuple[int, str]] = []
    monkeypatch.setattr(
        kb,
        "_terminate_reclaimed_worker",
        lambda pid, claim, **_kwargs: terminated.append((pid, claim)) or {},
    )
    with kb.connect() as conn:
        task_id = kb.create_task(
            conn, title="racing spawn", assignee="engineer",
        )
        replacement_id = kb.create_task(conn, title="replacement")

        def spawn_after_supersede(task, workspace, board=None):
            kb.supersede_subtree(conn, task.id, replacement_id)
            return 43210

        result = kb.dispatch_once(conn, spawn_fn=spawn_after_supersede)
        task = kb.get_task(conn, task_id)
        assert task.status == "archived"
        assert task.worker_pid is None
        assert all(item[0] != task_id for item in result.spawned)
        assert len(terminated) == 1
        assert terminated[0][0] == 43210
        events = kb.list_events(conn, task_id)
        superseded_id = next(e.id for e in events if e.kind == "superseded")
        assert not any(e.kind == "spawned" and e.id > superseded_id for e in events)


def test_supersede_terminates_the_exact_registered_worker(
    kanban_home, monkeypatch
):
    """Already registered host-local attempts are stopped after DB commit."""
    import hermes_cli.profiles as profiles

    monkeypatch.setattr(profiles, "profile_exists", lambda _name: True)
    stopped: list[tuple[int, str]] = []
    monkeypatch.setattr(
        kb,
        "_terminate_reclaimed_worker",
        lambda pid, claim, **_kwargs: stopped.append((pid, claim)) or {},
    )
    with kb.connect() as conn:
        task_id = kb.create_task(conn, title="active", assignee="engineer")
        replacement_id = kb.create_task(conn, title="replacement")
        kb.dispatch_once(conn, spawn_fn=lambda *_args, **_kwargs: 24680)
        expected_claim = kb.get_task(conn, task_id).claim_lock

        kb.supersede_subtree(conn, task_id, replacement_id)

        assert stopped == [(24680, expected_claim)]
        assert kb.get_task(conn, task_id).worker_pid is None


def test_supersede_cli_dry_run_and_apply_share_the_same_subtree(kanban_home):
    """The operator surface previews without mutation, then applies exactly it."""
    from hermes_cli import kanban as kanban_cli

    with kb.connect() as conn:
        root_id = kb.create_task(conn, title="stale root")
        child_id = kb.create_task(conn, title="stale child", parents=[root_id])
        replacement_id = kb.create_task(conn, title="corrected root")

    preview = json.loads(
        kanban_cli.run_slash(
            f"supersede {root_id} --with {replacement_id} --dry-run --json"
        )
    )
    assert preview["dry_run"] is True
    assert set(preview["affected_task_ids"]) == {root_id, child_id}
    with kb.connect() as conn:
        assert kb.get_task(conn, root_id).status == "ready"
        assert kb.get_task(conn, child_id).status == "todo"

    applied = json.loads(
        kanban_cli.run_slash(
            f"supersede {root_id} --with {replacement_id} "
            "--reason 'replace invalid foundation' --json"
        )
    )
    assert applied["dry_run"] is False
    assert set(applied["affected_task_ids"]) == set(
        preview["affected_task_ids"]
    )
    with kb.connect() as conn:
        assert kb.get_task(conn, root_id).status == "archived"
        assert kb.get_task(conn, child_id).status == "archived"


def test_worktree_completion_validates_and_stamps_git_provenance(
    kanban_home, tmp_path, monkeypatch
):
    """Completion is fail-closed until Git state matches the task handoff."""
    import hermes_cli.profiles as profiles

    monkeypatch.setattr(profiles, "profile_exists", lambda _name: True)
    repo = tmp_path / "repository"
    _init_git_repo(repo)
    kb.create_board("provenance", default_workdir=str(repo))

    with kb.connect(board="provenance") as conn:
        task_id = kb.create_task(
            conn,
            title="implementation",
            assignee="engineer",
            workspace_kind="worktree",
            board="provenance",
        )
        dispatched = kb.dispatch_once(
            conn,
            board="provenance",
            spawn_fn=lambda task, workspace, board=None: None,
        )
        assert [item[0] for item in dispatched.spawned] == [task_id]

        task = kb.get_task(conn, task_id)
        assert task is not None
        assert task.status == "running"
        workspace = Path(task.workspace_path)
        expected_branch = task.branch_name
        assert expected_branch
        expected_base = _git(workspace, "rev-parse", "HEAD")

        # Untracked or modified content means the claimed implementation has
        # not produced a reviewable commit.
        (workspace / "implementation.txt").write_text("uncommitted\n", encoding="utf-8")
        with pytest.raises(kb.WorktreeProvenanceError, match="(?i)dirty|uncommitted"):
            kb.complete_task(conn, task_id, result="must not complete dirty work")
        assert kb.get_task(conn, task_id).status == "running"

        _git(workspace, "add", "implementation.txt")
        _git(workspace, "commit", "-m", "implement task")
        expected_commit = _git(workspace, "rev-parse", "HEAD")

        # A clean checkout is still invalid when it is no longer on the
        # task-owned branch.
        _git(workspace, "switch", "-c", "unexpected-completion-branch")
        with pytest.raises(kb.WorktreeProvenanceError, match="(?i)branch|mismatch"):
            kb.complete_task(conn, task_id, result="must not complete wrong branch")
        assert kb.get_task(conn, task_id).status == "running"
        _git(workspace, "switch", expected_branch)

        # Worker-declared provenance is a readback contract, not trusted prose.
        with pytest.raises(kb.WorktreeProvenanceError, match="(?i)commit|mismatch"):
            kb.complete_task(
                conn,
                task_id,
                result="must not accept invented commit",
                metadata={
                    "worktree_provenance": {
                        "workspace_path": str(workspace),
                        "branch": expected_branch,
                        "commit_sha": "0" * 40,
                        "common_dir": str(repo / ".git"),
                        "clean": True,
                    }
                },
            )
        assert kb.get_task(conn, task_id).status == "running"

        assert kb.complete_task(
            conn,
            task_id,
            result="verified implementation",
        )
        completed = kb.get_task(conn, task_id)
        assert completed is not None
        assert completed.status == "done"

        run = conn.execute(
            """
            SELECT metadata
              FROM task_runs
             WHERE task_id = ? AND outcome = 'completed'
             ORDER BY id DESC
             LIMIT 1
            """,
            (task_id,),
        ).fetchone()
        assert run is not None
        metadata = json.loads(run["metadata"])
        provenance = metadata["worktree_provenance"]
        expected_common_dir = _git(
            workspace,
            "rev-parse",
            "--path-format=absolute",
            "--git-common-dir",
        )
        assert provenance == {
            "workspace_path": str(workspace.resolve()),
            "branch": expected_branch,
            "base_sha": expected_base,
            "commit_sha": expected_commit,
            "common_dir": str(Path(expected_common_dir).resolve()),
            "clean": True,
        }


def test_single_parent_worktree_starts_from_validated_parent_result(
    kanban_home, tmp_path, monkeypatch
):
    """A linear dependent cannot silently branch from unrelated main."""
    import hermes_cli.profiles as profiles

    monkeypatch.setattr(profiles, "profile_exists", lambda _name: True)
    repo = tmp_path / "linear-repository"
    _init_git_repo(repo)
    kb.create_board("linear", default_workdir=str(repo))

    with kb.connect(board="linear") as conn:
        parent_id = kb.create_task(
            conn,
            title="foundation",
            assignee="engineer",
            workspace_kind="worktree",
            board="linear",
        )
        kb.dispatch_once(
            conn,
            board="linear",
            spawn_fn=lambda task, workspace, board=None: None,
        )
        parent = kb.get_task(conn, parent_id)
        parent_workspace = Path(parent.workspace_path)
        (parent_workspace / "foundation.txt").write_text(
            "foundation\n", encoding="utf-8"
        )
        _git(parent_workspace, "add", "foundation.txt")
        _git(parent_workspace, "commit", "-m", "foundation result")
        parent_result = _git(parent_workspace, "rev-parse", "HEAD")
        assert kb.complete_task(conn, parent_id, result="foundation committed")

        child_id = kb.create_task(
            conn,
            title="dependent",
            assignee="engineer",
            workspace_kind="worktree",
            parents=[parent_id],
            board="linear",
        )
        kb.dispatch_once(
            conn,
            board="linear",
            spawn_fn=lambda task, workspace, board=None: None,
        )
        child = kb.get_task(conn, child_id)
        child_workspace = Path(child.workspace_path)
        assert _git(child_workspace, "rev-parse", "HEAD") == parent_result
        run = kb.latest_run(conn, child_id)
        assert run.metadata["worktree_start"]["base_sha"] == parent_result
        assert (child_workspace / "foundation.txt").read_text() == "foundation\n"


def test_missing_worktree_parent_provenance_blocks_child_before_spawn(
    kanban_home, tmp_path, monkeypatch
):
    """A code-producing dependency may never silently degrade to repo HEAD."""
    import hermes_cli.profiles as profiles

    monkeypatch.setattr(profiles, "profile_exists", lambda _name: True)
    repo = tmp_path / "missing-parent-provenance"
    _init_git_repo(repo)
    kb.create_board("missing-parent-provenance", default_workdir=str(repo))
    with kb.connect(board="missing-parent-provenance") as conn:
        parent_id = kb.create_task(
            conn,
            title="legacy completed parent",
            assignee="engineer",
            workspace_kind="worktree",
            board="missing-parent-provenance",
        )
        conn.execute(
            "UPDATE tasks SET status = 'done' WHERE id = ?",
            (parent_id,),
        )
        conn.commit()
        child_id = kb.create_task(
            conn,
            title="must not start at HEAD",
            assignee="engineer",
            parents=[parent_id],
            workspace_kind="worktree",
            workspace_path=str(repo),
            board="missing-parent-provenance",
        )
        spawned: list[str] = []
        result = kb.dispatch_once(
            conn,
            board="missing-parent-provenance",
            spawn_fn=lambda task, *_args, **_kwargs: spawned.append(task.id),
        )
        assert spawned == []
        assert child_id in result.auto_blocked
        assert kb.get_task(conn, child_id).status == "blocked"
        assert "provenance" in kb.get_task(conn, child_id).last_failure_error


def test_review_worktree_gets_the_same_server_stamped_start(
    kanban_home, tmp_path, monkeypatch
):
    """Review-column dispatch cannot bypass worktree base/readback stamping."""
    import hermes_cli.profiles as profiles

    monkeypatch.setattr(profiles, "profile_exists", lambda _name: True)
    repo = tmp_path / "review-provenance"
    _init_git_repo(repo)
    kb.create_board("review-provenance", default_workdir=str(repo))
    with kb.connect(board="review-provenance") as conn:
        task_id = kb.create_task(
            conn,
            title="review branch",
            assignee="reviewer",
            workspace_kind="worktree",
            board="review-provenance",
        )
        conn.execute("UPDATE tasks SET status = 'review' WHERE id = ?", (task_id,))
        conn.commit()
        kb.dispatch_once(
            conn,
            board="review-provenance",
            spawn_fn=lambda *_args, **_kwargs: None,
        )
        task = kb.get_task(conn, task_id)
        assert task.status == "running"
        run = kb.latest_run(conn, task_id)
        assert run.metadata["worktree_start"]["branch"] == task.branch_name
        assert len(run.metadata["worktree_start"]["base_sha"]) == 40


def test_worker_spawn_declares_portable_git_mounts(
    kanban_home, tmp_path, monkeypatch
):
    """A remote worker receives the checkout and owning Git metadata."""
    repo = tmp_path / "repository"
    _init_git_repo(repo)
    workspace = tmp_path / "linked-worktree"
    _git(repo, "worktree", "add", "-b", "wt/portable", str(workspace))
    expected_common = Path(
        _git(workspace, "rev-parse", "--path-format=absolute", "--git-common-dir")
    ).resolve()

    captured: dict[str, object] = {}

    class FakeProcess:
        pid = 43210

    def fake_popen(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        return FakeProcess()

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    monkeypatch.setattr(kb, "_git_common_dir", lambda _path: expected_common)
    monkeypatch.setattr(kb, "_resolve_hermes_argv", lambda: ["hermes"])
    monkeypatch.setattr(kb, "_resolve_worker_cli_toolsets", lambda _home: None)
    monkeypatch.setattr(
        "hermes_cli.profiles.resolve_profile_env",
        lambda _profile: str(kanban_home),
    )
    monkeypatch.setattr(
        kb, "worker_logs_dir", lambda board=None: tmp_path / "worker-logs"
    )

    task = SimpleNamespace(
        id="t_portable",
        assignee="engineer",
        tenant=None,
        branch_name="wt/portable",
        current_run_id=1,
        claim_lock="test:claim",
        goal_mode=False,
        goal_max_turns=None,
        max_runtime_seconds=None,
        skills=[],
        model_override=None,
        provider_override=None,
        workspace_kind="worktree",
    )
    assert kb._default_spawn(task, str(workspace), board="default") == 43210

    env = captured["env"]
    assert isinstance(env, dict)
    mounts = json.loads(env["HERMES_KANBAN_RUNTIME_MOUNTS"])
    assert f"{workspace.resolve()}:{workspace.resolve()}" in mounts
    assert f"{expected_common}:{expected_common}" in mounts
    assert env["HERMES_KANBAN_ISOLATED_SANDBOX"] == "1"


def test_kanban_docker_runtime_is_exact_path_and_nonpersistent(
    tmp_path, monkeypatch
):
    """Profile terminal config cannot re-enable shared persistent state."""
    import tools.terminal_tool as terminal

    workspace = tmp_path / "workspace"
    common = tmp_path / "repo.git"
    workspace.mkdir()
    common.mkdir()
    mounts = [
        f"{workspace}:{workspace}",
        f"{common}:{common}",
    ]
    monkeypatch.setenv("TERMINAL_ENV", "docker")
    monkeypatch.setenv("TERMINAL_CWD", str(workspace))
    monkeypatch.setenv("TERMINAL_CONTAINER_PERSISTENT", "true")
    monkeypatch.setenv("TERMINAL_DOCKER_PERSIST_ACROSS_PROCESSES", "true")
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_isolated")
    monkeypatch.setenv("HERMES_KANBAN_ISOLATED_SANDBOX", "1")
    monkeypatch.setenv("HERMES_KANBAN_RUNTIME_MOUNTS", json.dumps(mounts))

    config = terminal._get_env_config()

    assert config["cwd"] == str(workspace)
    assert config["host_cwd"] is None
    assert config["docker_mount_cwd_to_workspace"] is False
    assert set(mounts).issubset(set(config["docker_volumes"]))
    assert config["container_persistent"] is False
    assert config["docker_persist_across_processes"] is False
    assert config["_kanban_task_scoped"] is True
    assert terminal._resolve_container_task_id(None) == "kanban-t_isolated"
    assert terminal._resolve_container_task_id("subagent-1") == "kanban-t_isolated"


def test_kanban_docker_validation_timeout_removes_new_container(monkeypatch):
    """Every validation exception owns fail-closed exact-container cleanup."""
    from tools.environments.docker import DockerEnvironment

    environment = object.__new__(DockerEnvironment)
    cleaned: list[bool] = []
    monkeypatch.setattr(
        environment,
        "_validate_task_scoped_container_unchecked",
        lambda _workspace: (_ for _ in ()).throw(
            subprocess.TimeoutExpired("docker exec", 15)
        ),
    )
    monkeypatch.setattr(
        environment,
        "cleanup",
        lambda *, force_remove=False: cleaned.append(force_remove),
    )
    with pytest.raises(subprocess.TimeoutExpired):
        environment._validate_task_scoped_container("/workspace")
    assert cleaned == [True]


def test_kanban_docker_low_pid_capacity_fails_closed(monkeypatch):
    from tools.environments.docker import DockerEnvironment

    environment = object.__new__(DockerEnvironment)
    environment._container_id = "container-id"
    environment._docker_exe = "docker"
    cleaned: list[bool] = []
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0, stdout="250 256\n", stderr="",
        ),
    )
    monkeypatch.setattr(
        environment,
        "cleanup",
        lambda *, force_remove=False: cleaned.append(force_remove),
    )
    with pytest.raises(RuntimeError, match="PID capacity"):
        environment._validate_task_scoped_container("/workspace")
    assert cleaned == [True]


def test_kanban_compression_feasibility_is_eager(monkeypatch):
    """A deterministic auxiliary-model defect fails before the first turn."""
    import agent.agent_init as agent_init
    import agent.conversation_compression as compression

    calls: list[object] = []
    fake_agent = SimpleNamespace(_compression_feasibility_checked=False)
    monkeypatch.setenv("HERMES_KANBAN_TASK", "t_compression")

    def reject(agent):
        calls.append(agent)
        raise ValueError("auxiliary compression model is below 64K")

    monkeypatch.setattr(compression, "check_compression_model_feasibility", reject)
    with pytest.raises(ValueError, match="below 64K"):
        agent_init._preflight_kanban_compression(fake_agent)
    assert calls == [fake_agent]

    calls.clear()
    monkeypatch.delenv("HERMES_KANBAN_TASK")
    agent_init._preflight_kanban_compression(fake_agent)
    assert calls == []


def test_permanent_worker_startup_exit_is_classified_without_retry(monkeypatch):
    """EX_CONFIG from worker startup is distinguishable from a crash."""
    pid = 424242
    raw_status = kb.KANBAN_PERMANENT_FAILURE_EXIT_CODE << 8
    monkeypatch.setitem(kb._recent_worker_exits, pid, (raw_status, 1.0))
    assert kb._classify_worker_exit(pid) == (
        "permanent_failure",
        kb.KANBAN_PERMANENT_FAILURE_EXIT_CODE,
    )


def test_protocol_worker_exit_has_a_dedicated_classification(monkeypatch):
    pid = 424243
    raw_status = kb.KANBAN_PROTOCOL_FAILURE_EXIT_CODE << 8
    monkeypatch.setitem(kb._recent_worker_exits, pid, (raw_status, 1.0))
    assert kb._classify_worker_exit(pid) == (
        "protocol_failure",
        kb.KANBAN_PROTOCOL_FAILURE_EXIT_CODE,
    )


def test_quiet_worker_cannot_exit_zero_before_terminal_transition(
    kanban_home, monkeypatch
):
    import cli

    with kb.connect() as conn:
        task_id = kb.create_task(conn, title="protocol exit")
        task = kb.claim_task(conn, task_id)
    monkeypatch.setenv("HERMES_KANBAN_TASK", task_id)
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(task.current_run_id))
    monkeypatch.setenv("HERMES_KANBAN_CLAIM_LOCK", str(task.claim_lock))
    monkeypatch.setenv("HERMES_KANBAN_DB", str(kb.kanban_db_path()))

    assert (
        cli._kanban_worker_terminal_exit_code(0)
        == kb.KANBAN_PROTOCOL_FAILURE_EXIT_CODE
    )
    with kb.connect() as conn:
        assert kb.complete_task(conn, task_id, result="terminal lifecycle call")
    assert cli._kanban_worker_terminal_exit_code(0) == 0


def test_worker_lease_readback_fences_reclaimed_attempt(kanban_home):
    with kb.connect() as conn:
        task_id = kb.create_task(conn, title="lease")
        task = kb.claim_task(conn, task_id)
        assert kb.verify_worker_lease(
            conn,
            task_id,
            expected_run_id=task.current_run_id,
            expected_claim_lock=task.claim_lock,
            expected_workspace=task.workspace_path,
        ) == "active"
        assert kb.verify_worker_lease(
            conn,
            task_id,
            expected_run_id=task.current_run_id + 1,
            expected_claim_lock=task.claim_lock,
        ) == "stale"
        kb.archive_task(conn, task_id)
        assert kb.verify_worker_lease(
            conn,
            task_id,
            expected_run_id=task.current_run_id,
            expected_claim_lock=task.claim_lock,
        ) == "terminal"


def test_worker_startup_preflight_rejects_stale_claim(
    kanban_home, tmp_path, monkeypatch
):
    import agent.agent_init as agent_init

    workspace = tmp_path / "startup-workspace"
    workspace.mkdir()
    with kb.connect() as conn:
        task_id = kb.create_task(
            conn,
            title="startup fence",
            workspace_kind="dir",
            workspace_path=str(workspace),
        )
        task = kb.claim_task(conn, task_id)
    monkeypatch.setenv("HERMES_KANBAN_TASK", task_id)
    monkeypatch.setenv("HERMES_KANBAN_WORKSPACE", str(workspace))
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(task.current_run_id))
    monkeypatch.setenv("HERMES_KANBAN_CLAIM_LOCK", "stale-claim")
    monkeypatch.setenv("HERMES_KANBAN_DB", str(kb.kanban_db_path()))
    monkeypatch.setenv("TERMINAL_ENV", "local")
    agent = SimpleNamespace(
        valid_tool_names={"kanban_show", "kanban_complete", "kanban_block"}
    )
    with pytest.raises(ValueError, match="lease is stale"):
        agent_init._preflight_kanban_runtime(agent)


def test_spawn_time_workspace_error_is_deterministic(
    kanban_home, monkeypatch
):
    import hermes_cli.profiles as profiles

    monkeypatch.setattr(profiles, "profile_exists", lambda _name: True)
    with kb.connect() as conn:
        task_id = kb.create_task(
            conn, title="spawn preflight", assignee="engineer",
        )

        def reject(*_args, **_kwargs):
            raise kb.WorkspaceValidationError("portable mount is invalid")

        result = kb.dispatch_once(
            conn, spawn_fn=reject, failure_limit=3,
        )
        assert task_id in result.auto_blocked
        assert kb.get_task(conn, task_id).status == "blocked"
        assert kb.get_task(conn, task_id).consecutive_failures == 1


def test_permanent_worker_startup_exit_blocks_on_first_reap(
    kanban_home, monkeypatch
):
    """The dispatcher consumes EX_CONFIG as a terminal deterministic fault."""
    import hermes_cli.profiles as profiles

    monkeypatch.setattr(profiles, "profile_exists", lambda _name: True)
    monkeypatch.setattr(kb, "_pid_alive", lambda _pid: False)
    monkeypatch.setenv("HERMES_KANBAN_CRASH_GRACE_SECONDS", "0")

    with kb.connect() as conn:
        task_id = kb.create_task(conn, title="startup preflight", assignee="engineer")
        dispatched = kb.dispatch_once(
            conn,
            spawn_fn=lambda task, workspace, board=None: 54321,
        )
        assert [item[0] for item in dispatched.spawned] == [task_id]
        kb._record_worker_exit(
            54321,
            kb.KANBAN_PERMANENT_FAILURE_EXIT_CODE << 8,
        )

        assert kb.detect_crashed_workers(conn) == [task_id]
        task = kb.get_task(conn, task_id)
        assert task is not None
        assert task.status == "blocked"
        assert task.consecutive_failures == 1
        assert task_id in getattr(kb.detect_crashed_workers, "_last_auto_blocked")
        run = kb.latest_run(conn, task_id)
        assert run is not None
        assert run.outcome == "preflight_failed"


def test_legacy_invalid_workspace_has_actionable_read_only_diagnostic(
    kanban_home,
):
    """Migration guidance is visible without mutating the legacy row."""
    from hermes_cli import kanban_diagnostics as diagnostics

    with kb.connect() as conn:
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, status, created_at, workspace_kind, workspace_path
            ) VALUES (
                't_invalid_diagnostic', 'legacy', 'todo', 1, 'worktree', NULL
            )
            """
        )
        conn.commit()
        before = dict(
            conn.execute(
                "SELECT status, workspace_path FROM tasks WHERE id = ?",
                ("t_invalid_diagnostic",),
            ).fetchone()
        )
        task = kb.get_task(conn, "t_invalid_diagnostic")
        found = diagnostics.compute_task_diagnostics(
            task,
            kb.list_events(conn, task.id),
            kb.list_runs(conn, task.id),
            now=10,
        )
        after = dict(
            conn.execute(
                "SELECT status, workspace_path FROM tasks WHERE id = ?",
                ("t_invalid_diagnostic",),
            ).fetchone()
        )

    invalid = [item for item in found if item.kind == "invalid_workspace"]
    assert len(invalid) == 1
    assert invalid[0].severity == "critical"
    assert invalid[0].data["retryable"] is False
    command = invalid[0].actions[0].payload["command"]
    assert "supersede t_invalid_diagnostic" in command
    assert "--dry-run" in command
    assert before == after


def test_disposable_docker_worker_edits_tests_commits_completes_and_cleans(
    kanban_home, tmp_path, monkeypatch
):
    """Real backend E2E for the complete isolated worker lifecycle."""
    if shutil.which("docker") is None:
        pytest.skip("Docker CLI is unavailable")
    available = subprocess.run(
        ["docker", "version", "--format", "{{.Server.Version}}"],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if available.returncode != 0:
        pytest.skip("Docker daemon is unavailable")

    import hermes_cli.profiles as profiles
    import tools.terminal_tool as terminal
    from tools import kanban_tools
    from tools.environments import docker as docker_environment

    repo = tmp_path / "docker-e2e-repo"
    _init_git_repo(repo)
    kb.create_board("docker-e2e", default_workdir=str(repo))
    monkeypatch.setattr(profiles, "profile_exists", lambda _name: True)

    with kb.connect(board="docker-e2e") as conn:
        task_id = kb.create_task(
            conn,
            title="disposable worker lifecycle",
            assignee="engineer",
            workspace_kind="worktree",
            board="docker-e2e",
        )
        dispatched = kb.dispatch_once(
            conn,
            board="docker-e2e",
            spawn_fn=lambda task, workspace, board=None: None,
        )
        assert [item[0] for item in dispatched.spawned] == [task_id]
        task = kb.get_task(conn, task_id)
        assert task is not None

    workspace = Path(task.workspace_path)
    common = Path(
        _git(workspace, "rev-parse", "--path-format=absolute", "--git-common-dir")
    ).resolve()
    mounts = [
        f"{workspace.resolve()}:{workspace.resolve()}",
        f"{common}:{common}",
    ]
    environment_key = f"kanban-{task_id}"

    monkeypatch.setenv("HERMES_KANBAN_TASK", task_id)
    monkeypatch.setenv("HERMES_KANBAN_WORKSPACE", str(workspace))
    monkeypatch.setenv("HERMES_KANBAN_BRANCH", str(task.branch_name))
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(task.current_run_id))
    monkeypatch.setenv("HERMES_KANBAN_CLAIM_LOCK", str(task.claim_lock))
    monkeypatch.setenv("HERMES_KANBAN_BOARD", "docker-e2e")
    monkeypatch.setenv(
        "HERMES_KANBAN_DB", str(kb.kanban_db_path(board="docker-e2e"))
    )
    monkeypatch.setenv("HERMES_PROFILE", "engineer")
    monkeypatch.setenv("HERMES_KANBAN_ISOLATED_SANDBOX", "1")
    monkeypatch.setenv("HERMES_KANBAN_RUNTIME_MOUNTS", json.dumps(mounts))
    monkeypatch.setenv("TERMINAL_ENV", "docker")
    monkeypatch.setenv(
        "TERMINAL_DOCKER_IMAGE",
        "nikolaik/python-nodejs:python3.11-nodejs20",
    )
    monkeypatch.setenv("TERMINAL_CWD", str(workspace))
    monkeypatch.setenv("TERMINAL_DOCKER_VOLUMES", "[]")
    monkeypatch.setenv("TERMINAL_DOCKER_RUN_AS_HOST_USER", "true")
    monkeypatch.setenv("TERMINAL_CONTAINER_PERSISTENT", "true")
    monkeypatch.setenv("TERMINAL_DOCKER_PERSIST_ACROSS_PROCESSES", "true")
    monkeypatch.setenv("TERMINAL_CONTAINER_DISK", "0")
    profile_label = docker_environment._sanitize_label_value(
        docker_environment._get_active_profile_name()
    )

    stale_container = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--label",
            "hermes-agent=1",
            "--label",
            f"hermes-task-id={environment_key}",
            "--label",
            f"hermes-profile={profile_label}",
            "alpine:latest",
            "sleep",
            "300",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    ).stdout.strip()
    try:
        unrelated_container = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--label",
                "hermes-agent=1",
                "--label",
                "hermes-task-id=kanban-unrelated-e2e",
                "--label",
                f"hermes-profile={profile_label}",
                "alpine:latest",
                "sleep",
                "300",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        ).stdout.strip()
    except Exception:
        subprocess.run(
            ["docker", "rm", "-f", stale_container],
            capture_output=True,
            timeout=30,
            check=False,
        )
        raise

    def run(command: str, **kwargs) -> dict:
        result = json.loads(terminal.terminal_tool(command, **kwargs))
        assert result.get("exit_code") == 0, result
        return result

    try:
        run("printf 'VALUE = 42\\n' > implementation.py")
        stale_probe = subprocess.run(
            ["docker", "inspect", stale_container],
            capture_output=True,
            timeout=15,
            check=False,
        )
        unrelated_probe = subprocess.run(
            ["docker", "inspect", unrelated_container],
            capture_output=True,
            timeout=15,
            check=False,
        )
        assert stale_probe.returncode != 0
        assert unrelated_probe.returncode == 0
        run(
            "PYTHONDONTWRITEBYTECODE=1 python -c "
            "\"import implementation; assert implementation.VALUE == 42\""
        )
        run("git add implementation.py && git commit -m 'implement docker e2e'")
        head = run("git rev-parse HEAD")["output"].strip()
        assert len(head) == 40
        background = run("sleep 300", background=True)
        assert background.get("session_id")
        assert background.get("pid")

        completed = json.loads(
            kanban_tools._handle_complete(
                {
                    "summary": "edited, tested, and committed in disposable Docker",
                    "metadata": {
                        "tests_run": [
                            "PYTHONDONTWRITEBYTECODE=1 python import assertion"
                        ],
                    },
                }
            )
        )
        assert completed.get("ok") is True, completed
        with kb.connect(board="docker-e2e") as conn:
            final_task = kb.get_task(conn, task_id)
            assert final_task is not None
            assert final_task.status == "done"
            completed_run = kb.latest_run(conn, task_id)
            assert completed_run is not None
            assert (
                completed_run.metadata["worktree_provenance"]["commit_sha"]
                == head
            )
    finally:
        terminal.cleanup_vm(environment_key, force_remove=True)
        subprocess.run(
            ["docker", "rm", "-f", stale_container, unrelated_container],
            capture_output=True,
            timeout=30,
            check=False,
        )

    leftover = subprocess.run(
        [
            "docker",
            "ps",
            "-aq",
            "--filter",
            "label=hermes-agent=1",
            "--filter",
            f"label=hermes-task-id={environment_key}",
            "--filter",
            f"label=hermes-profile={profile_label}",
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    assert leftover.stdout.strip() == ""
    assert environment_key not in terminal._active_environments
