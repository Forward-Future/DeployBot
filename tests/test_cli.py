from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, call, patch

from agent_merge_queue.cli import (
    FreezeResult,
    GitHub,
    QueueEntry,
    QueueError,
    active_batch,
    batch_fingerprint,
    batch_overlap_peers,
    check_states,
    command_block,
    command_dequeue,
    command_drain,
    command_enqueue,
    command_integrate,
    command_merge,
    command_promote,
    command_react,
    command_refresh_request,
    command_request,
    command_resume,
    command_unblock,
    completed_batch_ids,
    entries_in_batch,
    effective_queue_marker,
    freeze_queue,
    generated_only_change,
    latest_batch_marker,
    latest_marker,
    marker_queued_at,
    marker_priority_at,
    main,
    near_ready_overlap_holds,
    new_batch,
    overlap_groups,
    promote_integrations,
    queue_state_body,
    queue_from_intent,
    queue_timestamp,
    reconcile_externally_merged_threads,
    reusable_batch,
    settle_integration_checks,
    should_settle_batch,
    structured_dependencies,
)
from agent_merge_queue.config import parse_config
from agent_merge_queue.records import integration_body, intent_body
from agent_merge_queue.reviews import ReviewVerdict


CONFIG = parse_config(
    {
        "queue": {
            "required_checks": ["CI"],
            "dependency_directive": "Queue-after",
            "trusted_actors": ["trusted"],
        }
    }
)


def entry(number: int, *paths: str, state: str = "ready") -> QueueEntry:
    value = QueueEntry(
        number=number,
        title=f"PR {number}",
        url=f"https://example.test/{number}",
        head_sha=str(number) * 40,
        queued_head_sha=str(number) * 40,
        queued_at=f"2026-06-20T00:00:0{number}Z",
        queue_state="queued",
        is_draft=False,
        base_branch="main",
        mergeable="MERGEABLE",
        merge_state="CLEAN",
        labels=["merge-queue"],
        checks={"CI": "passed"},
        review_verdicts=(),
        source_paths=list(paths),
        generated_paths=[],
        dependencies=[],
        state=state,
        reasons=[] if state == "ready" else ["waiting gate"],
    )
    return value


class QueueCoreTest(unittest.TestCase):
    def test_status_is_a_read_only_pipeline_view(self) -> None:
        status = {"repository": "example/repo"}
        with (
            patch("agent_merge_queue.cli.load_config", return_value=CONFIG),
            patch("agent_merge_queue.cli.GitHub"),
            patch("agent_merge_queue.cli.pipeline_status", return_value=status),
            patch("agent_merge_queue.cli.print_pipeline_status") as print_status,
        ):
            result = main(["status", "--json"])

        self.assertEqual(result, 0)
        print_status.assert_called_once_with(status, json_output=True)

    def test_dependency_directive_is_configurable(self) -> None:
        body = "Queue-after: #12, #14\nBlocked by #99"
        self.assertEqual(structured_dependencies(body, "Queue-after"), [12, 14])

    def test_marker_without_queue_time_stays_missing(self) -> None:
        self.assertIsNone(marker_queued_at({"head_sha": "a" * 40}))
        self.assertEqual(
            marker_queued_at({"queued_at": "2026-06-20T00:00:00Z"}),
            "2026-06-20T00:00:00Z",
        )
        self.assertEqual(
            marker_priority_at({"priority_at": "2026-06-19T23:59:00Z"}),
            "2026-06-19T23:59:00Z",
        )

    def test_intent_request_time_is_the_stable_queue_priority(self) -> None:
        value = entry(1)
        value.labels = ["deploy-requested"]
        intent = {
            "intent_id": "intent-1",
            "requested_at": "2026-06-20T00:00:00Z",
            "requested_head": value.head_sha,
            "state": "requested",
        }
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"trusted"}

        changed = queue_from_intent(
            client,
            value,
            intent,
            comments=[],
            labels={"deploy-requested"},
        )

        self.assertTrue(changed)
        self.assertEqual(value.priority_at, "2026-06-20T00:00:00Z")
        self.assertIn(
            '"priority_at": "2026-06-20T00:00:00Z"',
            client.comment.call_args.args[1],
        )

    def test_settle_window_only_collects_a_mixed_ready_burst(self) -> None:
        ready = entry(1)
        waiting = entry(2, state="waiting")
        waiting.labels = ["deploy-requested"]
        client = Mock()
        client.config = CONFIG

        self.assertTrue(should_settle_batch(client, [ready, waiting]))
        self.assertFalse(should_settle_batch(client, [ready]))
        waiting.labels.append("merge-queue-blocked")
        self.assertFalse(should_settle_batch(client, [ready, waiting]))

    def test_overlap_mode_holds_only_ready_members_of_near_ready_components(
        self,
    ) -> None:
        config = parse_config(
            {
                "queue": {"required_checks": ["CI"], "trusted_actors": ["trusted"]},
                "integration": {"mode": "overlap"},
            }
        )
        overlapping = entry(1, "shared.py")
        independent = entry(2, "other.py")
        waiting = entry(3, state="waiting")
        waiting.labels = ["deploy-requested"]
        client = Mock()
        client.config = config
        client.coordinator_logins = {"coordinator"}
        client.changed_paths.return_value = (["shared.py"], [])
        client.comments.return_value = []

        holds = near_ready_overlap_holds(client, [overlapping, independent, waiting])

        self.assertEqual(holds, {1: [3]})
        client.changed_paths.assert_called_once_with(3)

    def test_reconciles_an_externally_merged_requested_thread(self) -> None:
        head_sha = "a" * 40
        merge_sha = "m" * 40
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.thread_records.return_value = [
            {
                "phase": "deploy-requested",
                "provider": "codex",
                "pull_request": 42,
                "thread_id": "thread-42",
            }
        ]
        client.comments.return_value = [
            {
                "created_at": "2026-06-20T00:00:00Z",
                "user": {"login": "trusted"},
                "body": intent_body(
                    intent_id="intent-42",
                    state="requested",
                    requested_at="2026-06-20T00:00:00Z",
                    requested_head=head_sha,
                    provider="codex",
                    thread_id="thread-42",
                ),
            }
        ]
        client.externally_integrated_merge.return_value = merge_sha

        result = reconcile_externally_merged_threads(client)

        self.assertEqual(
            result,
            [{"head_sha": head_sha, "merge_sha": merge_sha, "pull_request": 42}],
        )
        record = client.record_thread.call_args.args[0]
        self.assertEqual(record.phase, "merged")
        self.assertEqual(record.pull_request, 42)

    def test_overlap_hold_includes_transitive_ready_members(self) -> None:
        config = parse_config(
            {
                "queue": {"required_checks": ["CI"], "trusted_actors": ["trusted"]},
                "integration": {"mode": "overlap"},
            }
        )
        first = entry(1, "shared.py", "bridge.py")
        second = entry(2, "bridge.py")
        waiting = entry(3, state="waiting")
        waiting.labels = ["deploy-requested"]
        client = Mock()
        client.config = config
        client.coordinator_logins = {"coordinator"}
        client.changed_paths.return_value = (["shared.py"], [])
        client.comments.return_value = []

        self.assertEqual(
            near_ready_overlap_holds(client, [first, second, waiting]),
            {1: [3], 2: [3]},
        )

    def test_overlap_hold_does_not_rewrite_an_existing_frozen_batch(self) -> None:
        config = parse_config(
            {
                "queue": {"required_checks": ["CI"], "trusted_actors": ["trusted"]},
                "integration": {"mode": "overlap"},
            }
        )
        ready = entry(1, "shared.py")
        waiting = entry(2, state="waiting")
        waiting.labels = ["deploy-requested"]
        batch = new_batch([ready], frozen_at="2026-06-20T00:01:00Z")
        client = Mock()
        client.config = config
        client.coordinator_logins = {"coordinator"}
        client.changed_paths.return_value = (["shared.py"], [])
        client.comments.return_value = []
        with patch("agent_merge_queue.cli.latest_batch_marker", return_value=batch):
            self.assertEqual(
                near_ready_overlap_holds(client, [ready, waiting]),
                {},
            )

    def test_simultaneous_intents_use_pull_request_number_as_fifo_tiebreaker(
        self,
    ) -> None:
        first = entry(1)
        second = entry(2)
        first.priority_at = second.priority_at = "2026-06-20T00:00:00Z"
        first.queued_at = "2026-06-20T00:00:02Z"
        second.queued_at = "2026-06-20T00:00:01Z"
        client = object.__new__(GitHub)
        client.queued_numbers = Mock(return_value=[2, 1])
        client.snapshot = Mock(side_effect=lambda number: {1: first, 2: second}[number])

        self.assertEqual([value.number for value in client.queue()], [1, 2])

    def test_registry_race_converges_on_the_lowest_issue_number(self) -> None:
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client._paged_api = Mock(
            side_effect=[
                [],
                [
                    {"number": 10, "title": "DeployBot delivery registry"},
                    {"number": 9, "title": "DeployBot delivery registry"},
                ],
            ]
        )
        client._json = Mock(side_effect=[[], {"number": 10}])
        client._run = Mock(return_value="")

        self.assertEqual(client.registry_issue_number(create=True), 9)

    def test_frozen_merge_fast_path_never_rescans_the_whole_queue(self) -> None:
        value = entry(1, "a.py")
        batch = new_batch([value], frozen_at="2026-06-20T00:01:00Z")
        client = Mock()
        client.config = CONFIG
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"trusted"}
        client.pipeline_control.return_value = {"state": "running"}
        client.snapshot.return_value = value
        client.merge.return_value = "m" * 40
        client.comments.return_value = []

        merged = command_merge(
            client,
            "1",
            str(batch["batch_id"]),
            frozen_entry=value,
            frozen_batch=batch,
            active_numbers={1},
        )

        self.assertEqual(merged, "m" * 40)
        client.queue.assert_not_called()
        client.snapshot.assert_called_once_with(
            1,
            known_source_paths=["a.py"],
            known_generated_paths=[],
        )
        client.merge.assert_called_once_with(
            1,
            value.head_sha,
            authorization_entry=value,
        )

    def test_frozen_merge_accepts_the_same_head_already_integrated_externally(
        self,
    ) -> None:
        value = entry(1, "a.py")
        batch = new_batch([value], frozen_at="2026-06-20T00:01:00Z")
        client = Mock()
        client.config = CONFIG
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.snapshot.side_effect = QueueError("PR #1 is not open")
        client.externally_integrated_merge.return_value = "e" * 40
        client.comments.return_value = []

        merged = command_merge(
            client,
            "1",
            str(batch["batch_id"]),
            frozen_entry=value,
            frozen_batch=batch,
            active_numbers={1},
        )

        self.assertEqual(merged, "e" * 40)
        client.externally_integrated_merge.assert_called_once_with(1, value.head_sha)
        client.merge.assert_not_called()

    def test_external_merge_must_be_exact_and_ancestral_to_main(self) -> None:
        head_sha = "a" * 40
        merge_sha = "m" * 40
        base_sha = "b" * 40
        client = object.__new__(GitHub)
        client.repository = "example/repo"
        client._json = Mock(
            return_value={
                "headRefOid": head_sha,
                "mergeCommit": {"oid": merge_sha},
                "state": "MERGED",
            }
        )
        client.base_sha = Mock(return_value=base_sha)
        client.is_ancestor = Mock(return_value=True)

        self.assertEqual(
            client.externally_integrated_merge(1, head_sha),
            merge_sha,
        )
        self.assertEqual(
            [call.args for call in client.is_ancestor.call_args_list],
            [(head_sha, base_sha), (merge_sha, base_sha)],
        )

    def test_generated_paths_are_configurable(self) -> None:
        self.assertTrue(
            generated_only_change(
                "dist/app.js",
                None,
                generated_paths=frozenset({"dist/app.js"}),
                generated_version_paths=frozenset(),
                asset_version_pattern=r"\?v=[0-9a-f]{12}",
            )
        )
        self.assertFalse(
            generated_only_change(
                "src/app.js",
                "+code",
                generated_paths=frozenset(),
                generated_version_paths=frozenset(),
                asset_version_pattern=r"\?v=[0-9a-f]{12}",
            )
        )
        version_patch = """@@ -1 +1 @@
-<script src="/app.js?v=111111111111"></script>
+<script src="/app.js?v=222222222222"></script>
"""
        self.assertTrue(
            generated_only_change(
                "public/index.html",
                version_patch,
                generated_paths=frozenset(),
                generated_version_paths=frozenset({"public/**"}),
                asset_version_pattern=r"\?v=[0-9a-f]{12}",
            )
        )

    def test_waiting_promotion_defers_changed_file_fetch(self) -> None:
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"trusted"}
        client.comments = Mock(return_value=[])
        client.changed_paths = Mock(return_value=(["a.py"], []))
        client._json = Mock(
            return_value={
                "baseRefName": "main",
                "body": "",
                "headRefOid": "a" * 40,
                "isDraft": False,
                "labels": [{"name": "deploy-requested"}],
                "mergeStateStatus": "CLEAN",
                "mergeable": "MERGEABLE",
                "number": 1,
                "state": "OPEN",
                "statusCheckRollup": [],
                "title": "Waiting",
                "url": "https://example.test/1",
            }
        )

        value = client.snapshot(
            1,
            require_marker=False,
            allow_blocked_label=True,
            defer_paths_until_ready=True,
        )

        self.assertEqual(value.state, "waiting")
        client.changed_paths.assert_not_called()

    def test_integration_snapshot_uses_exact_commit_check_fallback(self) -> None:
        head_sha = "a" * 40
        marker = {
            "batch_id": "batch",
            "conflict": None,
            "heads": {"1": "1" * 40, "2": "2" * 40},
            "pull_requests": [1, 2],
        }
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.comments = Mock(
            return_value=[
                {
                    "created_at": "2026-06-20T00:00:00Z",
                    "user": {"login": "coordinator"},
                    "body": integration_body(marker),
                }
            ]
        )
        client.changed_paths = Mock(return_value=(["combined.py"], []))
        client.commit_check_runs = Mock(
            return_value=[
                {
                    "name": "CI",
                    "conclusion": "success",
                    "started_at": "2026-06-20T00:01:00Z",
                }
            ]
        )
        client._json = Mock(
            return_value={
                "baseRefName": "main",
                "body": "",
                "headRefOid": head_sha,
                "isDraft": False,
                "labels": [],
                "mergeStateStatus": "UNSTABLE",
                "mergeable": "MERGEABLE",
                "number": 38,
                "state": "OPEN",
                "statusCheckRollup": [],
                "title": "Integration",
                "url": "https://example.test/38",
            }
        )

        value = client.snapshot(
            38,
            require_marker=False,
            allow_blocked_label=True,
        )

        self.assertEqual(value.state, "ready")
        self.assertEqual(value.checks["CI"], "passed")
        client.commit_check_runs.assert_called_once_with(head_sha)

    def test_required_checks_do_not_accept_skipped(self) -> None:
        states = check_states(
            [
                {"name": "CI", "conclusion": "SUCCESS"},
                {"name": "Review", "conclusion": "SKIPPED"},
            ]
        )
        self.assertEqual(states, {"CI": "passed", "Review": "pending"})

    def test_latest_check_rerun_wins(self) -> None:
        states = check_states(
            [
                {
                    "name": "CI",
                    "conclusion": "FAILURE",
                    "startedAt": "2026-06-20T00:00:00Z",
                },
                {
                    "name": "CI",
                    "conclusion": "SUCCESS",
                    "startedAt": "2026-06-20T00:01:00Z",
                },
            ]
        )
        self.assertEqual(states["CI"], "passed")

        states = check_states(
            [
                {
                    "name": "CI",
                    "conclusion": "SUCCESS",
                    "startedAt": "2026-06-20T00:00:00Z",
                },
                {
                    "name": "CI",
                    "conclusion": "FAILURE",
                    "startedAt": "2026-06-20T00:01:00Z",
                },
            ]
        )
        self.assertEqual(states["CI"], "failed")

        states = check_states(
            [
                {
                    "name": "CI",
                    "conclusion": "ACTION_REQUIRED",
                    "started_at": "2026-06-20T00:00:00Z",
                },
                {
                    "name": "CI",
                    "conclusion": "SUCCESS",
                    "started_at": "2026-06-20T00:01:00Z",
                },
            ]
        )
        self.assertEqual(states["CI"], "passed")

    def test_undated_queued_rerun_hides_older_success(self) -> None:
        states = check_states(
            [
                {
                    "name": "CI",
                    "conclusion": "SUCCESS",
                    "startedAt": "2026-06-20T00:00:00Z",
                },
                {"name": "CI", "status": "QUEUED"},
            ]
        )

        self.assertEqual(states["CI"], "pending")

    def test_review_verdicts_are_classified_generically(self) -> None:
        value = entry(1)
        value.review_verdicts = (ReviewVerdict("Any bot", "blocked", ("one finding",)),)

        value.classify(CONFIG)

        self.assertEqual(value.state, "blocked")
        self.assertIn("one finding", value.reasons)

    def test_github_blocked_state_fails_closed_but_mergeable_states_do_not(
        self,
    ) -> None:
        blocked = entry(1)
        blocked.merge_state = "BLOCKED"
        blocked.classify(CONFIG)
        self.assertEqual(blocked.state, "blocked")
        self.assertIn("merge state as BLOCKED", blocked.reasons[-1])

        for index, state in enumerate(("BEHIND", "HAS_HOOKS", "UNSTABLE"), start=2):
            value = entry(index)
            value.merge_state = state
            value.classify(CONFIG)
            self.assertEqual(value.state, "ready", state)

    def test_overlap_groups_are_connected_components(self) -> None:
        groups = overlap_groups(
            [entry(1, "a.py"), entry(2, "a.py", "b.py"), entry(3, "b.py")]
        )

        self.assertEqual(groups[0]["pull_requests"], [1, 2, 3])

    def test_queue_discovery_uses_every_paginated_pull_request(self) -> None:
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client._paged_api = Mock(
            return_value=[
                {"number": number, "labels": [{"name": "merge-queue"}]}
                for number in range(1, 151)
            ]
        )

        self.assertEqual(client.queued_numbers(), list(range(1, 151)))

    def test_workflow_history_uses_every_paginated_page(self) -> None:
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client._json = Mock(
            return_value=[
                {"workflow_runs": [{"id": number} for number in range(1, 101)]},
                {"workflow_runs": [{"id": number} for number in range(101, 151)]},
            ]
        )

        self.assertEqual(
            [run["id"] for run in client.workflow_runs()], list(range(1, 151))
        )
        client._json.assert_called_once_with(
            "api",
            "--paginate",
            "--slurp",
            "repos/example/repo/actions/runs?branch=main&per_page=100",
        )

    def test_integration_status_requires_every_source_authorization(self) -> None:
        client = object.__new__(GitHub)
        client.source_deploy_authorized = Mock(return_value=True)
        integration = {
            "heads": {"1": "a" * 40, "2": "b" * 40},
            "pull_requests": [1, 2],
        }

        self.assertTrue(client.integration_sources_authorized(integration))
        client.source_deploy_authorized.assert_has_calls(
            [
                call(1, "a" * 40),
                call(2, "b" * 40),
            ]
        )
        client.source_deploy_authorized.reset_mock(side_effect=True)
        client.source_deploy_authorized.side_effect = [True, False]
        self.assertFalse(client.integration_sources_authorized(integration))

    def test_dependency_must_be_on_configured_base_branch(self) -> None:
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client._json = Mock(
            side_effect=[
                {
                    "mergedAt": "2026-06-20T00:00:00Z",
                    "mergeCommit": {"oid": "abc"},
                },
                {"status": "ahead"},
            ]
        )

        self.assertTrue(client.dependency_is_merged(12))
        self.assertIn("...main", client._json.call_args_list[1].args[1])

    def test_queue_marker_rejects_forgery_and_free_text_injection(self) -> None:
        sha = "a" * 40
        marker = (
            "<!-- agent-merge-queue:v1 "
            + json.dumps({"schema": 1, "head_sha": sha})
            + " -->\n"
            + f"Queued for the agent-managed merge queue on `{sha}`."
        )
        forged = {
            "created_at": "2026-06-20T00:01:00Z",
            "user": {"login": "attacker"},
            "body": marker,
        }
        injected = {
            "created_at": "2026-06-20T00:02:00Z",
            "user": {"login": "trusted"},
            "body": "prefix " + marker,
        }
        valid = {
            "created_at": "2026-06-20T00:03:00Z",
            "user": {"login": "trusted"},
            "body": marker,
        }

        self.assertIsNone(latest_marker([forged, injected], "trusted"))
        self.assertEqual(
            latest_marker([forged, injected, valid], "trusted")["head_sha"], sha
        )

        collaborator = {
            "created_at": "2026-06-20T00:04:00Z",
            "author_association": "COLLABORATOR",
            "user": {"login": "another-writer"},
            "body": marker,
        }
        self.assertIsNone(latest_marker([collaborator], {"github-actions[bot]"}))

    def test_latest_queue_state_uses_comment_id_when_timestamps_tie(self) -> None:
        sha = "a" * 40
        comments = [
            {
                "id": 100,
                "created_at": "2026-06-20T00:00:00Z",
                "user": {"login": "trusted"},
                "body": queue_state_body("queued", sha, queued_at=None),
            },
            {
                "id": 101,
                "created_at": "2026-06-20T00:00:00Z",
                "user": {"login": "trusted"},
                "body": queue_state_body("dequeued", sha, queued_at=None),
            },
        ]

        self.assertEqual(latest_marker(comments, "trusted")["state"], "dequeued")

    def test_legacy_astro_markers_remain_readable(self) -> None:
        sha = "a" * 40
        queue = {
            "schema": 1,
            "head_sha": sha,
            "queued_at": "2026-06-20T00:00:00Z",
        }
        queue_comment = {
            "created_at": "2026-06-20T00:00:00Z",
            "user": {"login": "trusted"},
            "body": (
                "<!-- astrohub-merge-queue:v1 "
                + json.dumps(queue)
                + " -->\n"
                + f"Queued for the agent-managed merge queue on `{sha}`."
            ),
        }
        batch = new_batch([entry(1)], frozen_at="2026-06-20T00:01:00Z")
        batch_comment = {
            "created_at": "2026-06-20T00:01:00Z",
            "user": {"login": "trusted"},
            "body": (
                "<!-- astrohub-merge-batch:v1 "
                + json.dumps(batch)
                + " -->\n"
                + f"Frozen merge batch `{batch['batch_id']}`."
            ),
        }

        self.assertEqual(
            latest_marker([queue_comment], "trusted"),
            {**queue, "state": "queued"},
        )
        self.assertEqual(
            latest_batch_marker([batch_comment], "trusted", batch_id=batch["batch_id"]),
            batch,
        )

    def test_batch_marker_freezes_membership_and_fifo(self) -> None:
        first = entry(1, "a.py")
        second = entry(2, "a.py", "b.py")
        batch = new_batch([first, second], frozen_at="2026-06-20T00:01:00Z")
        body = (
            "<!-- agent-merge-batch:v1 "
            + json.dumps(batch)
            + " -->\n"
            + f"Frozen merge batch `{batch['batch_id']}`."
        )
        comments = [
            {
                "created_at": "2026-06-20T00:01:00Z",
                "user": {"login": "trusted"},
                "body": body,
            }
        ]

        restored = latest_batch_marker(comments, "trusted", batch_id=batch["batch_id"])
        self.assertEqual(restored["pull_requests"], [1, 2])
        self.assertEqual(restored["fingerprint"], batch_fingerprint([first, second]))
        self.assertEqual(batch_overlap_peers(restored, 1, {1, 2}), [2])
        self.assertTrue(
            reusable_batch(
                [restored, restored],
                [first, second],
                batch_fingerprint([first, second]),
            )
        )

    def test_active_batch_excludes_late_arrivals(self) -> None:
        first = entry(1, "a.py")
        second = entry(2, "b.py")
        late = entry(3, "c.py")
        batch = new_batch([first, second], frozen_at="2026-06-20T00:01:00Z")

        selected = active_batch([first, second, late], {1: batch, 2: batch, 3: None})

        self.assertEqual(selected["pull_requests"], [1, 2])
        self.assertEqual(
            [
                value.number
                for value in entries_in_batch([first, second, late], selected)
            ],
            [1, 2],
        )

    def test_completed_batch_is_not_reused(self) -> None:
        first = entry(1, "a.py")
        batch = new_batch([first], frozen_at="2026-06-20T00:01:00Z")
        completion = {
            "schema": 1,
            "batch_id": batch["batch_id"],
            "completed_at": "2026-06-20T00:02:00Z",
        }
        comment = {
            "created_at": "2026-06-20T00:02:00Z",
            "user": {"login": "trusted"},
            "body": (
                "<!-- agent-merge-batch-complete:v1 "
                + json.dumps(completion)
                + " -->\n"
                + f"Completed merge batch `{batch['batch_id']}`."
            ),
        }

        completed = completed_batch_ids([comment], "trusted")

        self.assertEqual(completed, {batch["batch_id"]})
        self.assertIsNone(active_batch([first], {1: batch}, completed))

    def test_freeze_hydrates_paths_for_transiently_waiting_queued_entries(
        self,
    ) -> None:
        first = entry(1, state="waiting")
        second = entry(2, state="waiting")
        client = Mock()
        client.config = CONFIG
        client.coordinator_logins = {"coordinator"}
        client.queued_numbers.return_value = [1, 2]
        client.changed_paths.side_effect = lambda _number: (["shared.py"], [])
        client.comments.return_value = []

        with patch(
            "agent_merge_queue.cli.utc_now", return_value="2026-06-20T00:01:00Z"
        ):
            frozen = freeze_queue(client, known_entries=[first, second])

        self.assertEqual(client.changed_paths.call_count, 2)
        self.assertEqual(
            frozen.overlap_groups,
            [{"pull_requests": [1, 2], "source_paths": ["shared.py"]}],
        )
        self.assertEqual(
            frozen.batch["source_paths"],
            {"1": ["shared.py"], "2": ["shared.py"]},
        )

    def test_queue_timestamp_preserves_refresh_but_resets_reenqueue(self) -> None:
        previous = {"queued_at": "2026-06-20T00:00:00Z"}
        self.assertEqual(
            queue_timestamp(
                previous,
                already_queued=True,
                now="2026-06-20T01:00:00Z",
            ),
            "2026-06-20T00:00:00Z",
        )
        self.assertEqual(
            queue_timestamp(
                previous,
                already_queued=False,
                now="2026-06-20T01:00:00Z",
            ),
            "2026-06-20T01:00:00Z",
        )

    def test_drain_merges_independent_ready_entries_and_skips_overlap(self) -> None:
        first = entry(1, "a.py")
        overlap_left = entry(2, "shared.py")
        overlap_right = entry(3, "shared.py")
        waiting = entry(4, "d.py", state="waiting")
        frozen = FreezeResult(
            batch={"batch_id": "batch"},
            queue=[first, overlap_left, overlap_right, waiting],
            blocked_queue=[],
            next_batch=[],
            overlap_groups=[{"pull_requests": [2, 3], "source_paths": ["shared.py"]}],
        )
        client = Mock()
        with (
            patch("agent_merge_queue.cli.freeze_queue", return_value=frozen),
            patch(
                "agent_merge_queue.cli.command_merge", return_value="m" * 40
            ) as merge,
            redirect_stdout(io.StringIO()),
        ):
            result = command_drain(client, json_output=True)

        merge.assert_called_once_with(
            client,
            "1",
            "batch",
            emit=False,
            frozen_entry=first,
            frozen_batch=frozen.batch,
            active_numbers={1, 2, 3, 4},
        )
        self.assertEqual(result["merged"][0]["number"], 1)
        self.assertEqual(result["integration_required"][0]["pull_requests"], [2, 3])
        self.assertEqual(result["waiting"][0]["number"], 4)

    def test_drain_retries_transient_mergeability_in_the_same_run(self) -> None:
        first = entry(1, "a.py")
        frozen = FreezeResult(
            batch={"batch_id": "batch"},
            queue=[first],
            blocked_queue=[],
            next_batch=[],
            overlap_groups=[],
        )
        client = Mock()
        client.snapshot.return_value = first
        with (
            patch("agent_merge_queue.cli.freeze_queue", return_value=frozen),
            patch(
                "agent_merge_queue.cli.command_merge",
                side_effect=[
                    QueueError("GitHub is still computing mergeability"),
                    "m" * 40,
                ],
            ) as merge,
            patch("agent_merge_queue.cli.time.sleep") as sleep,
            redirect_stdout(io.StringIO()),
        ):
            result = command_drain(client, json_output=True)

        self.assertEqual(merge.call_count, 2)
        sleep.assert_called_once_with(1)
        self.assertEqual(result["merged"][0]["number"], 1)

    def test_drain_completes_waiting_pass_then_promotes_next_batch(self) -> None:
        waiting = entry(1, "a.py", state="waiting")
        ready = entry(2, "b.py")
        first = FreezeResult(
            batch={"batch_id": "first"},
            queue=[waiting],
            blocked_queue=[],
            next_batch=[ready],
            overlap_groups=[],
        )
        second = FreezeResult(
            batch={"batch_id": "second"},
            queue=[waiting, ready],
            blocked_queue=[],
            next_batch=[],
            overlap_groups=[],
        )
        client = Mock()
        with (
            patch("agent_merge_queue.cli.freeze_queue", side_effect=[first, second]),
            patch(
                "agent_merge_queue.cli.command_merge", return_value="m" * 40
            ) as merge,
            redirect_stdout(io.StringIO()),
        ):
            result = command_drain(client, json_output=True)

        merge.assert_called_once_with(
            client,
            "2",
            "second",
            emit=False,
            frozen_entry=ready,
            frozen_batch=second.batch,
            active_numbers={1, 2},
        )
        self.assertEqual(result["batch_ids"], ["first", "second"])
        self.assertEqual(result["merged"][0]["number"], 2)
        self.assertEqual(result["waiting"][0]["number"], 1)

    def test_reenqueue_toggles_label_to_wake_event_coordinator(self) -> None:
        value = entry(1)
        old_head = "a" * 40
        value.queued_head_sha = old_head
        marker = {
            "created_at": "2026-06-20T00:00:00Z",
            "author_association": "OWNER",
            "user": {"login": "owner"},
            "body": (
                "<!-- agent-merge-queue:v1 "
                + json.dumps(
                    {
                        "schema": 1,
                        "head_sha": old_head,
                        "queued_at": "2026-06-20T00:00:00Z",
                    }
                )
                + " -->\n"
                + f"Queued for the agent-managed merge queue on `{old_head}`."
            ),
        }
        client = Mock()
        client.config = CONFIG
        client.resolve_pr.return_value = 1
        client.snapshot.return_value = value
        client.comments.return_value = [marker]
        client.trusted_logins = {"owner"}
        with redirect_stdout(io.StringIO()):
            command_enqueue(client, "1")

        client.comments.assert_not_called()
        client.remove_label.assert_called_once_with(1, "merge-queue")
        client.add_label.assert_called_once_with(1, "merge-queue")

    def test_reenabling_blocked_pr_toggles_queue_label_to_wake_coordinator(
        self,
    ) -> None:
        value = entry(1)
        value.labels.append("merge-queue-blocked")
        marker = {
            "created_at": "2026-06-20T00:00:00Z",
            "user": {"login": "trusted"},
            "body": (
                "<!-- agent-merge-queue:v1 "
                + json.dumps(
                    {
                        "schema": 1,
                        "head_sha": value.head_sha,
                        "queued_at": "2026-06-20T00:00:00Z",
                    }
                )
                + " -->\n"
                + "Queued for the agent-managed merge queue on "
                + f"`{value.head_sha}`."
            ),
        }
        client = Mock()
        client.config = CONFIG
        client.resolve_pr.return_value = 1
        client.snapshot.return_value = value
        client.comments.return_value = [marker]
        client.trusted_logins = {"trusted"}

        with redirect_stdout(io.StringIO()):
            command_enqueue(client, "1")

        self.assertEqual(
            [value.args for value in client.remove_label.call_args_list],
            [
                (1, "merge-queue-blocked"),
                (1, "merge-queue"),
            ],
        )
        client.add_label.assert_called_once_with(1, "merge-queue")

    def test_unblock_toggles_queue_label_to_wake_coordinator(self) -> None:
        value = entry(1)
        value.labels.append("merge-queue-blocked")
        client = Mock()
        client.config = CONFIG
        client.resolve_pr.return_value = 1
        client.snapshot.return_value = value
        client.comments.return_value = [
            {
                "created_at": "2026-06-20T00:00:00Z",
                "user": {"login": "trusted"},
                "body": queue_state_body(
                    "blocked",
                    value.head_sha,
                    queued_at=value.queued_at,
                    reason="hold",
                ),
            }
        ]
        client.trusted_logins = {"trusted"}
        client.labels.return_value = {"merge-queue", "merge-queue-blocked"}

        with redirect_stdout(io.StringIO()):
            command_unblock(client, "1")

        self.assertEqual(
            [value.args for value in client.remove_label.call_args_list],
            [
                (1, "merge-queue-blocked"),
                (1, "merge-queue"),
            ],
        )
        client.add_label.assert_called_once_with(1, "merge-queue")

    def test_durable_queue_state_survives_label_changes(self) -> None:
        value = entry(1)
        value.labels = ["merge-queue"]
        value.queue_state = "dequeued"
        value.classify(CONFIG)
        self.assertEqual(value.state, "blocked")
        self.assertIn("authorization was revoked", " ".join(value.reasons))

        value.queue_state = "blocked"
        value.labels = ["merge-queue"]
        value.classify(CONFIG)
        self.assertEqual(value.state, "blocked")
        self.assertIn("trusted actor blocked", " ".join(value.reasons))

    def test_block_and_dequeue_write_durable_state_before_labels_change(self) -> None:
        value = entry(1)
        queued = {
            "created_at": "2026-06-20T00:00:00Z",
            "user": {"login": "trusted"},
            "body": queue_state_body(
                "queued", value.head_sha, queued_at=value.queued_at
            ),
        }
        client = Mock()
        client.config = CONFIG
        client.resolve_pr.return_value = 1
        client.snapshot.return_value = value
        client.comments.return_value = [queued]
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"trusted"}
        client.repository = "example/repo"
        client.base_sha.return_value = "b" * 40
        client.labels.return_value = {"merge-queue"}

        with redirect_stdout(io.StringIO()):
            command_block(client, "1", "investigate")

        block_body = client.comment.call_args_list[0].args[1]
        self.assertIn('"state": "blocked"', block_body)
        client.add_label.assert_called_once_with(1, "merge-queue-blocked")
        names = [call[0] for call in client.method_calls]
        self.assertLess(names.index("comment"), names.index("add_label"))

        client.reset_mock()
        client.config = CONFIG
        client.resolve_pr.return_value = 1
        client.snapshot.return_value = value
        client.comments.return_value = [queued]
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"trusted"}
        client.labels.return_value = {"merge-queue"}
        with redirect_stdout(io.StringIO()):
            command_dequeue(client, "1", "superseded")

        dequeue_body = client.comment.call_args.args[1]
        self.assertIn('"state": "dequeued"', dequeue_body)
        self.assertEqual(
            [call.args for call in client.remove_label.call_args_list],
            [(1, "merge-queue"), (1, "merge-queue-blocked")],
        )
        names = [call[0] for call in client.method_calls]
        self.assertLess(names.index("add_label"), names.index("comment"))
        self.assertLess(names.index("comment"), names.index("remove_label"))

    def test_final_merge_rechecks_durable_authorization(self) -> None:
        value = entry(1)
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client._json = Mock(
            return_value={
                "headRefOid": value.head_sha,
                "isDraft": False,
                "labels": [{"name": "merge-queue"}],
                "state": "OPEN",
            }
        )
        client.comments = Mock(
            return_value=[
                {
                    "id": 2,
                    "created_at": "2026-06-20T00:00:01Z",
                    "user": {"login": "trusted"},
                    "body": queue_state_body(
                        "dequeued", value.head_sha, queued_at=value.queued_at
                    ),
                }
            ]
        )

        with self.assertRaisesRegex(QueueError, "durable queue authorization"):
            client.merge(1, value.head_sha)
        self.assertEqual(client._json.call_count, 1)

    def test_prevalidated_merge_avoids_duplicate_authorization_reads(self) -> None:
        value = entry(1)
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client._json = Mock(return_value={"merged": True, "sha": "m" * 40})
        client.comments = Mock()

        result = client.merge(
            1,
            value.head_sha,
            authorization_entry=value,
        )

        self.assertEqual(result, "m" * 40)
        client.comments.assert_not_called()
        self.assertEqual(client._json.call_count, 1)

    def test_final_integration_merge_requires_current_exact_head_intent(self) -> None:
        integration_head = "f" * 40
        source_head = "a" * 40
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client._json = Mock(
            side_effect=[
                {
                    "headRefOid": integration_head,
                    "isDraft": False,
                    "labels": [{"name": "merge-queue"}],
                    "state": "OPEN",
                },
                {"headRefOid": source_head},
            ]
        )
        integration_comments = [
            {
                "id": 1,
                "created_at": "2026-06-20T00:00:00Z",
                "user": {"login": "coordinator"},
                "body": integration_body(
                    {
                        "batch_id": "batch-1",
                        "heads": {"1": source_head},
                        "pull_requests": [1],
                    }
                ),
            },
            {
                "id": 2,
                "created_at": "2026-06-20T00:00:01Z",
                "user": {"login": "coordinator"},
                "body": queue_state_body(
                    "queued",
                    integration_head,
                    queued_at="2026-06-20T00:00:01Z",
                    integration_batch_id="batch-1",
                ),
            },
        ]
        stale_source_intent = {
            "id": 3,
            "created_at": "2026-06-20T00:00:02Z",
            "user": {"login": "trusted"},
            "body": intent_body(
                intent_id="intent-2",
                state="requested",
                requested_at="2026-06-20T00:00:02Z",
                requested_head="b" * 40,
            ),
        }
        client.comments = Mock(
            side_effect=lambda number: (
                integration_comments if number == 99 else [stale_source_intent]
            )
        )

        with self.assertRaisesRegex(QueueError, "authorization was revoked"):
            client.merge(99, integration_head)

    def test_coordinator_queue_marker_requires_trusted_deploy_intent(self) -> None:
        sha = "a" * 40
        intent = {
            "id": 1,
            "created_at": "2026-06-20T00:00:00Z",
            "user": {"login": "trusted"},
            "body": intent_body(
                intent_id="intent-1",
                state="requested",
                requested_at="2026-06-20T00:00:00Z",
                requested_head=sha,
            ),
        }
        delegated = {
            "id": 2,
            "created_at": "2026-06-20T00:01:00Z",
            "user": {"login": "coordinator"},
            "body": queue_state_body(
                "queued",
                sha,
                queued_at="2026-06-20T00:01:00Z",
                intent_id="intent-1",
            ),
        }
        marker = effective_queue_marker(
            [intent, delegated], {"trusted"}, {"coordinator"}
        )
        self.assertEqual(marker["head_sha"], sha)
        self.assertIsNone(
            effective_queue_marker([delegated], {"trusted"}, {"coordinator"})
        )
        stale_intent = dict(intent)
        stale_intent["body"] = intent_body(
            intent_id="intent-1",
            state="requested",
            requested_at="2026-06-20T00:00:00Z",
            requested_head="b" * 40,
        )
        self.assertIsNone(
            effective_queue_marker(
                [stale_intent, delegated], {"trusted"}, {"coordinator"}
            )
        )

        integration = {
            "id": 3,
            "created_at": "2026-06-20T00:02:00Z",
            "user": {"login": "coordinator"},
            "body": integration_body(
                {
                    "batch_id": "batch-1",
                    "heads": {"1": sha},
                    "pull_requests": [1],
                }
            ),
        }
        integration_marker = {
            "id": 4,
            "created_at": "2026-06-20T00:03:00Z",
            "user": {"login": "coordinator"},
            "body": queue_state_body(
                "queued",
                sha,
                queued_at="2026-06-20T00:03:00Z",
                integration_batch_id="batch-1",
            ),
        }
        self.assertEqual(
            effective_queue_marker(
                [integration, integration_marker],
                {"trusted"},
                {"coordinator"},
                integration_authorized=lambda _: True,
            ),
            latest_marker([integration_marker], {"coordinator"}),
        )
        self.assertIsNone(
            effective_queue_marker(
                [integration, integration_marker],
                {"trusted"},
                {"coordinator"},
                integration_authorized=lambda _: False,
            )
        )

    def test_request_records_intent_before_gates_and_adds_intent_label(self) -> None:
        value = entry(1, state="waiting")
        value.labels = []
        value.reasons = ["CI is not complete"]
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.resolve_pr.return_value = 1
        client.require_actor.return_value = "trusted"
        client.snapshot.return_value = value
        with (
            patch("agent_merge_queue.cli.utc_now", return_value="2026-06-20T00:00:00Z"),
            redirect_stdout(io.StringIO()),
        ):
            result = command_request(
                client,
                "1",
                provider="codex",
                thread_id="thread-1",
                thread_url=None,
            )
        self.assertEqual(result["state"], "deploy-requested")
        self.assertIn("deploybot-intent:v1", client.comment.call_args.args[1])
        client.add_label.assert_called_with(1, "deploy-requested")
        client.record_thread.assert_called_once()

    def test_promote_queues_ready_intent_but_leaves_waiting_visible(self) -> None:
        ready = entry(1)
        ready.labels = ["deploy-requested"]
        waiting = entry(2, state="waiting")
        waiting.labels = ["deploy-requested"]
        waiting.reasons = ["CI is not complete"]
        intent = intent_body(
            intent_id="intent-1",
            state="requested",
            requested_at="2026-06-20T00:00:00Z",
            requested_head=ready.head_sha,
        )
        intent_comment = {
            "id": 1,
            "created_at": "2026-06-20T00:00:00Z",
            "user": {"login": "trusted"},
            "body": intent,
        }
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.intent_numbers.return_value = [1, 2]
        client.active_integration_sources.return_value = set()
        client.comments.return_value = [intent_comment]
        client.snapshot.side_effect = [ready, waiting]
        client.labels.return_value = {"deploy-requested"}
        with redirect_stdout(io.StringIO()):
            result = command_promote(client)
        self.assertEqual(result["promoted"], [1])
        self.assertEqual(result["waiting"][0]["number"], 2)
        self.assertIn("intent-1", client.comment.call_args.args[1])
        client.add_label.assert_called_with(1, "merge-queue")

    def test_promote_never_auto_resumes_a_repair_block(self) -> None:
        blocked = entry(1)
        blocked.labels = ["deploy-requested", "merge-queue-blocked"]
        intent_comment = {
            "id": 1,
            "created_at": "2026-06-20T00:00:00Z",
            "user": {"login": "trusted"},
            "body": intent_body(
                intent_id="intent-1",
                state="requested",
                requested_at="2026-06-20T00:00:00Z",
                requested_head=blocked.head_sha,
            ),
        }
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.intent_numbers.return_value = [1]
        client.active_integration_sources.return_value = set()
        client.comments.return_value = [intent_comment]
        client.snapshot.return_value = blocked

        with redirect_stdout(io.StringIO()):
            result = command_promote(client)

        self.assertEqual(result["promoted"], [])
        self.assertIn("deploybot resume", result["waiting"][0]["reasons"][0])
        client.add_label.assert_not_called()

    def test_resume_atomically_requeues_repaired_exact_head(self) -> None:
        value = entry(1)
        value.labels = ["deploy-requested", "merge-queue-blocked"]
        intent_comment = {
            "id": 1,
            "created_at": "2026-06-20T00:00:00Z",
            "user": {"login": "trusted"},
            "body": intent_body(
                intent_id="intent-1",
                state="requested",
                requested_at="2026-06-20T00:00:00Z",
                requested_head=value.head_sha,
            ),
        }
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.resolve_pr.return_value = 1
        client.comments.return_value = [intent_comment]
        client.snapshot.return_value = value
        client.labels.return_value = set(value.labels)
        with redirect_stdout(io.StringIO()):
            command_resume(client, "1")
        client.add_label.assert_called_with(1, "merge-queue")
        client.remove_label.assert_called_with(1, "merge-queue-blocked")

    def test_promote_does_not_duplicate_sources_under_active_integration(self) -> None:
        client = Mock()
        client.config = CONFIG
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.intent_numbers.return_value = [1]
        client.active_integration_sources.return_value = {1}
        with redirect_stdout(io.StringIO()):
            result = command_promote(client)
        self.assertEqual(result["promoted"], [])
        self.assertIn("integration PR", result["waiting"][0]["reasons"][0])
        client.snapshot.assert_not_called()

    def test_refresh_request_requires_ready_head_and_chains_intent(self) -> None:
        value = entry(1)
        previous_body = intent_body(
            intent_id="old-intent",
            state="requested",
            requested_at="2026-06-20T00:00:00Z",
            requested_head="a" * 40,
            provider="codex",
            thread_id="thread-1",
        )
        previous_comment = {
            "id": 1,
            "created_at": "2026-06-20T00:00:00Z",
            "user": {"login": "trusted"},
            "body": previous_body,
        }
        refreshed_comments: list[dict] = [previous_comment]
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.resolve_pr.return_value = 1
        client.require_actor.return_value = "trusted"
        client.snapshot.return_value = value
        client.labels.return_value = {"deploy-requested"}

        def comments(_: int):
            return list(refreshed_comments)

        def add_comment(_: int, body: str):
            refreshed_comments.append(
                {
                    "id": len(refreshed_comments) + 1,
                    "created_at": "2026-06-20T01:00:00Z",
                    "user": {"login": "trusted"},
                    "body": body,
                }
            )

        client.comments.side_effect = comments
        client.comment.side_effect = add_comment
        with (
            patch("agent_merge_queue.cli.utc_now", return_value="2026-06-20T01:00:00Z"),
            redirect_stdout(io.StringIO()),
        ):
            result = command_refresh_request(client, "1")
        self.assertEqual(result["parent_intent_id"], "old-intent")
        self.assertEqual(result["head_sha"], value.head_sha)
        self.assertTrue(
            any(
                '"parent_intent_id": "old-intent"' in item["body"]
                for item in refreshed_comments
            )
        )

    def test_integration_pr_contains_every_frozen_head(self) -> None:
        first = entry(1, "shared.py")
        second = entry(2, "shared.py")
        batch = new_batch([first, second], frozen_at="2026-06-20T00:00:00Z")
        client = object.__new__(GitHub)
        client.config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                },
                "integration": {"mode": "all"},
            }
        )
        client.repository = "example/repo"
        client.owner = "example"
        client.name = "repo"
        client.base_sha = Mock(return_value="b" * 40)
        client._json = Mock(
            side_effect=[
                {},
                {"sha": "1" * 40},
                {"sha": "2" * 40},
                [],
                {"number": 99, "html_url": "https://example.test/99"},
            ]
        )
        client.comment = Mock()
        client.labels = Mock(return_value={"merge-queue", "deploy-requested"})
        client.remove_label = Mock()
        result = client.create_integration_pull_request(
            batch=batch, entries=[first, second]
        )
        self.assertEqual(result["number"], 99)
        marker = client.comment.call_args.args[1]
        self.assertIn(first.head_sha, marker)
        self.assertIn(second.head_sha, marker)
        merge_calls = [
            call
            for call in client._json.call_args_list
            if "repos/example/repo/merges" in call.args
        ]
        self.assertEqual(len(merge_calls), 2)
        self.assertEqual(
            [call.args for call in client.remove_label.call_args_list],
            [
                (1, "merge-queue"),
                (1, "deploy-requested"),
                (2, "merge-queue"),
                (2, "deploy-requested"),
            ],
        )

    def test_failed_integration_pr_creation_removes_its_orphan_branch(self) -> None:
        first = entry(1, "shared.py")
        second = entry(2, "shared.py")
        batch = new_batch([first, second], frozen_at="2026-06-20T00:00:00Z")
        client = object.__new__(GitHub)
        client.config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                },
                "integration": {"mode": "overlap"},
            }
        )
        client.repository = "example/repo"
        client.owner = "example"
        client.name = "repo"
        client.base_sha = Mock(return_value="b" * 40)
        client._json = Mock(
            side_effect=[
                {},
                {"sha": "1" * 40},
                {"sha": "2" * 40},
                [],
                QueueError("GitHub Actions may not create pull requests"),
            ]
        )
        client._run = Mock(return_value="")

        with self.assertRaisesRegex(QueueError, "may not create"):
            client.create_integration_pull_request(batch=batch, entries=[first, second])

        branch = f"deploybot/integration/{batch['batch_id']}"
        client._run.assert_called_once_with(
            "api",
            "--method",
            "DELETE",
            f"repos/example/repo/git/refs/heads/{branch}",
        )

    def test_reactor_uses_cumulative_pr_in_all_mode(self) -> None:
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                    "coordinator_actors": ["coordinator"],
                },
                "integration": {"mode": "all"},
            }
        )
        first = entry(1, "a.py")
        second = entry(2, "b.py")
        batch = new_batch([first, second], frozen_at="2026-06-20T00:00:00Z")
        frozen = FreezeResult(batch, [first, second], [], [], [])
        client = Mock()
        client.config = config
        client.coordinator_logins = {"coordinator"}
        client.pipeline_control.return_value = {"state": "running"}
        client.queued_numbers.return_value = []
        client.integration_pull_request_numbers.return_value = []
        client.create_integration_pull_request.return_value = {
            "number": 99,
            "conflict": None,
        }
        with (
            patch(
                "agent_merge_queue.cli.settle_integration_checks",
                side_effect=[[], [{"pull_request": 99, "state": "ready"}]],
            ),
            patch(
                "agent_merge_queue.cli.promote_integrations",
                side_effect=[[], [99]],
            ),
            patch("agent_merge_queue.cli.command_promote", return_value={}),
            patch("agent_merge_queue.cli.freeze_queue", return_value=frozen),
            patch(
                "agent_merge_queue.cli.command_drain",
                return_value={
                    "batch_id": "integration",
                    "batch_ids": ["integration"],
                    "integration_required": [],
                    "merged": [{"number": 99, "merge_sha": "a" * 40}],
                    "next_batch": [],
                    "waiting": [],
                },
            ) as drain,
            redirect_stdout(io.StringIO()),
        ):
            result = command_react(client, follow=False, timeout_seconds=10)
        drain.assert_called_once_with(client, json_output=False, emit=False)
        self.assertEqual(result["integrations"][0]["number"], 99)
        self.assertEqual(result["drain"]["merged"][0]["number"], 99)

    def test_reactor_drains_existing_integration_before_new_all_mode_batch(
        self,
    ) -> None:
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                    "coordinator_actors": ["coordinator"],
                },
                "integration": {"mode": "all"},
            }
        )
        existing = entry(99, "old.py")
        source = entry(1, "new.py")
        existing_batch = new_batch([existing], frozen_at="2026-06-20T00:00:00Z")
        source_batch = new_batch([source], frozen_at="2026-06-20T00:01:00Z")
        existing_frozen = FreezeResult(existing_batch, [existing], [], [], [])
        source_frozen = FreezeResult(source_batch, [source], [], [], [])
        old_drain = {
            "batch_id": existing_batch["batch_id"],
            "batch_ids": [existing_batch["batch_id"]],
            "integration_required": [],
            "merged": [{"number": 99, "merge_sha": "a" * 40}],
            "next_batch": [],
            "waiting": [],
        }
        new_drain = {
            "batch_id": "new-integration",
            "batch_ids": ["new-integration"],
            "integration_required": [],
            "merged": [{"number": 100, "merge_sha": "b" * 40}],
            "next_batch": [],
            "waiting": [],
        }
        client = Mock()
        client.config = config
        client.coordinator_logins = {"coordinator"}
        client.pipeline_control.return_value = {"state": "running"}
        client.queued_numbers.return_value = [1, 99]
        client.integration_pull_request_numbers.return_value = [99]
        client.create_integration_pull_request.return_value = {
            "number": 100,
            "conflict": None,
        }
        with (
            patch(
                "agent_merge_queue.cli.settle_integration_checks",
                side_effect=[[], [{"pull_request": 100, "state": "ready"}]],
            ),
            patch(
                "agent_merge_queue.cli.promote_integrations",
                side_effect=[[], [100]],
            ),
            patch(
                "agent_merge_queue.cli.command_promote",
                return_value={"promoted": [], "waiting": [], "blocked": []},
            ),
            patch(
                "agent_merge_queue.cli.freeze_queue",
                side_effect=[existing_frozen, source_frozen],
            ) as freeze,
            patch(
                "agent_merge_queue.cli.command_drain",
                side_effect=[old_drain, new_drain],
            ) as drain,
            redirect_stdout(io.StringIO()),
        ):
            result = command_react(client, follow=False, timeout_seconds=10)

        self.assertEqual(
            freeze.call_args_list,
            [
                call(client, held_numbers={1}),
                call(client, known_entries=[], held_numbers=set()),
            ],
        )
        self.assertEqual(
            drain.call_args_list,
            [
                call(
                    client,
                    json_output=False,
                    emit=False,
                    initial_frozen=existing_frozen,
                ),
                call(client, json_output=False, emit=False),
            ],
        )
        client.create_integration_pull_request.assert_called_once_with(
            batch=source_batch,
            entries=[source],
        )
        self.assertEqual(
            [value["number"] for value in result["drain"]["merged"]],
            [99, 100],
        )

    def test_reactor_establishes_overlap_integration_before_draining(self) -> None:
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                    "coordinator_actors": ["coordinator"],
                },
                "integration": {"mode": "overlap"},
            }
        )
        first = entry(1, "shared.py")
        second = entry(2, "shared.py")
        independent = entry(3, "other.py")
        batch = new_batch(
            [first, second, independent], frozen_at="2026-06-20T00:00:00Z"
        )
        frozen = FreezeResult(
            batch,
            [first, second, independent],
            [],
            [],
            [{"pull_requests": [1, 2], "source_paths": ["shared.py"]}],
        )
        client = Mock()
        client.config = config
        client.coordinator_logins = {"coordinator"}
        client.pipeline_control.return_value = {"state": "running"}
        events: list[str] = []
        client.create_integration_pull_request.side_effect = lambda **_kwargs: (
            events.append("integration") or {"number": 99, "conflict": None}
        )

        def drain(*_args, **_kwargs):
            events.append("drain")
            return {"merged": [{"number": 3, "merge_sha": "a" * 40}]}

        with (
            patch(
                "agent_merge_queue.cli.settle_integration_checks",
                side_effect=lambda *_args, **kwargs: (
                    []
                    if kwargs.get("numbers") is None
                    else events.append("settle")
                    or [{"pull_request": 99, "state": "ready"}]
                ),
            ),
            patch(
                "agent_merge_queue.cli.promote_integrations",
                side_effect=[[], [99]],
            ),
            patch("agent_merge_queue.cli.command_promote", return_value={}),
            patch("agent_merge_queue.cli.freeze_queue", return_value=frozen),
            patch("agent_merge_queue.cli.command_drain", side_effect=drain),
            redirect_stdout(io.StringIO()),
        ):
            command_react(client, follow=False, timeout_seconds=10)

        self.assertEqual(events, ["integration", "settle", "drain", "drain"])

    def test_reactor_does_not_drain_when_overlap_integration_creation_fails(
        self,
    ) -> None:
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                    "coordinator_actors": ["coordinator"],
                },
                "integration": {"mode": "overlap"},
            }
        )
        first = entry(1, "shared.py")
        second = entry(2, "shared.py")
        batch = new_batch([first, second], frozen_at="2026-06-20T00:00:00Z")
        frozen = FreezeResult(
            batch,
            [first, second],
            [],
            [],
            [{"pull_requests": [1, 2], "source_paths": ["shared.py"]}],
        )
        client = Mock()
        client.config = config
        client.coordinator_logins = {"coordinator"}
        client.pipeline_control.return_value = {"state": "running"}
        client.create_integration_pull_request.side_effect = QueueError(
            "GitHub Actions may not create pull requests"
        )
        with (
            patch("agent_merge_queue.cli.promote_integrations", return_value=[]),
            patch("agent_merge_queue.cli.command_promote", return_value={}),
            patch("agent_merge_queue.cli.freeze_queue", return_value=frozen),
            patch("agent_merge_queue.cli.command_drain") as drain,
            redirect_stdout(io.StringIO()),
            self.assertRaisesRegex(QueueError, "may not create"),
        ):
            command_react(client, follow=False, timeout_seconds=10)

        drain.assert_not_called()

    def test_reactor_holds_overlap_peer_but_drains_independent_work(self) -> None:
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                    "coordinator_actors": ["coordinator"],
                },
                "integration": {"mode": "overlap"},
            }
        )
        overlapping = entry(1, "shared.py")
        waiting = entry(2, state="waiting")
        waiting.labels = ["deploy-requested"]
        independent = entry(3, "other.py")
        entries = [overlapping, waiting, independent]
        batch = new_batch([independent], frozen_at="2026-06-20T00:01:00Z")
        frozen = FreezeResult(batch, [independent], [], [], [])
        client = Mock()
        client.config = config
        client.coordinator_logins = {"coordinator"}
        client.pipeline_control.return_value = {"state": "running"}
        client.changed_paths.return_value = (["shared.py"], [])
        client.comments.return_value = []

        def promote(*_args, **kwargs):
            kwargs["captured_entries"].extend(entries)
            return {
                "promoted": [1, 3],
                "waiting": [{"number": 2, "reasons": ["CI is not complete"]}],
                "blocked": [],
            }

        with (
            patch("agent_merge_queue.cli.promote_integrations", return_value=[]),
            patch("agent_merge_queue.cli.command_promote", side_effect=promote),
            patch("agent_merge_queue.cli.freeze_queue", return_value=frozen) as freeze,
            patch(
                "agent_merge_queue.cli.command_drain",
                return_value={"merged": [{"number": 3, "merge_sha": "a" * 40}]},
            ),
            redirect_stdout(io.StringIO()),
        ):
            result = command_react(client, follow=False, timeout_seconds=10)

        freeze.assert_called_once_with(
            client,
            known_entries=entries,
            held_numbers={1},
        )
        self.assertEqual(result["promoted"]["held"][0]["number"], 1)
        self.assertEqual(result["drain"]["merged"][0]["number"], 3)

    def test_reactor_dispatches_ci_once_after_a_merged_batch(self) -> None:
        client = Mock()
        client.config = CONFIG
        client.pipeline_control.return_value = {"state": "running"}
        client.dispatch_ci_workflows.return_value = [{"id": 7, "name": "CI"}]
        empty = FreezeResult(None, [], [], [], [])
        with (
            patch("agent_merge_queue.cli.promote_integrations", return_value=[]),
            patch("agent_merge_queue.cli.command_promote", return_value={}),
            patch("agent_merge_queue.cli.freeze_queue", return_value=empty),
            patch(
                "agent_merge_queue.cli.command_drain",
                return_value={"merged": [{"number": 1, "merge_sha": "a" * 40}]},
            ),
            redirect_stdout(io.StringIO()),
        ):
            result = command_react(
                client,
                follow=False,
                timeout_seconds=10,
                dispatch_ci=True,
            )
        client.dispatch_ci_workflows.assert_called_once_with()
        self.assertEqual(result["dispatched_ci"], [{"id": 7, "name": "CI"}])

    def test_reactor_follows_release_without_a_new_merge(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.config = CONFIG
        client.pipeline_control.return_value = {"state": "running"}
        client.base_sha.return_value = sha
        client.workflow_runs.return_value = [
            {
                "id": 42,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
                "event": "workflow_dispatch",
                "created_at": "2026-06-20T00:00:00Z",
                "updated_at": "2026-06-20T00:01:00Z",
            }
        ]
        empty = FreezeResult(None, [], [], [], [])
        release = {"state": "verified", "main_sha": sha}
        with (
            patch("agent_merge_queue.cli.promote_integrations", return_value=[]),
            patch("agent_merge_queue.cli.command_promote", return_value={}),
            patch("agent_merge_queue.cli.freeze_queue", return_value=empty),
            patch(
                "agent_merge_queue.cli.command_drain",
                return_value={"merged": []},
            ),
            patch(
                "agent_merge_queue.cli.command_follow",
                return_value=release,
            ) as follow_release,
            redirect_stdout(io.StringIO()),
        ):
            result = command_react(client, follow=True, timeout_seconds=10)

        follow_release.assert_called_once_with(
            client,
            timeout_seconds=10,
            poll_seconds=10,
            json_output=False,
            emit=False,
        )
        self.assertEqual(result["release"], release)

    def test_reactor_does_not_follow_conflicted_all_mode_integration_batch(
        self,
    ) -> None:
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                    "coordinator_actors": ["coordinator"],
                },
                "integration": {"mode": "all"},
            }
        )
        sha = "a" * 40
        first = entry(1, "a.py")
        second = entry(2, "b.py")
        batch = new_batch([first, second], frozen_at="2026-06-20T00:00:00Z")
        frozen = FreezeResult(batch, [first, second], [], [], [])
        client = Mock()
        client.config = config
        client.coordinator_logins = {"coordinator"}
        client.pipeline_control.return_value = {"state": "running"}
        client.queued_numbers.return_value = []
        client.integration_pull_request_numbers.return_value = []
        client.create_integration_pull_request.return_value = {
            "number": 99,
            "conflict": {"number": 2, "reason": "merge conflict"},
        }
        client.base_sha.return_value = sha
        client.workflow_runs.return_value = [
            {
                "id": 42,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
                "event": "push",
                "created_at": "2026-06-20T00:00:00Z",
                "updated_at": "2026-06-20T00:01:00Z",
            },
            {
                "id": 43,
                "name": "Deploy",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
                "event": "workflow_run",
                "created_at": "2026-06-20T00:01:00Z",
                "updated_at": "2026-06-20T00:02:00Z",
            },
        ]
        client.thread_records.return_value = []
        with (
            patch("agent_merge_queue.cli.promote_integrations", return_value=[]),
            patch("agent_merge_queue.cli.command_promote", return_value={}),
            patch("agent_merge_queue.cli.freeze_queue", return_value=frozen),
            patch("agent_merge_queue.cli.command_drain") as drain,
            patch("agent_merge_queue.cli.command_follow") as follow_release,
            redirect_stdout(io.StringIO()),
        ):
            result = command_react(client, follow=True, timeout_seconds=10)

        drain.assert_not_called()
        follow_release.assert_not_called()
        self.assertIsNone(result["release"])
        self.assertEqual(result["integrations"][0]["number"], 99)

    def test_integration_ci_dispatch_is_owned_until_the_pr_is_ready(self) -> None:
        number = 38
        head_sha = "a" * 40
        branch = "deploybot/integration/batch"
        marker = {
            "batch_id": "batch",
            "conflict": None,
            "heads": {"1": "1" * 40, "2": "2" * 40},
            "pull_requests": [1, 2],
        }
        client = Mock()
        client.config = CONFIG
        client.coordinator_logins = {"coordinator"}
        client.comments.return_value = [
            {
                "created_at": "2026-06-20T00:00:00Z",
                "user": {"login": "coordinator"},
                "body": integration_body(marker),
            }
        ]
        client.pull_head.return_value = {
            "branch": branch,
            "head_sha": head_sha,
            "state": "OPEN",
        }
        active = {
            "id": 7,
            "name": "CI",
            "head_sha": head_sha,
            "event": "workflow_dispatch",
            "status": "in_progress",
            "conclusion": None,
            "created_at": "2026-06-20T00:01:00Z",
        }
        successful = {
            **active,
            "status": "completed",
            "conclusion": "success",
        }
        client.workflow_runs_for_branch.side_effect = [[], [active], [successful]]
        client.dispatch_ci_workflows.return_value = [{"id": 7, "name": "CI"}]
        client.commit_check_runs.return_value = [
            {
                "name": "CI",
                "conclusion": "success",
                "started_at": "2026-06-20T00:01:00Z",
            }
        ]
        ready = entry(8, "combined.py")
        ready.number = number
        ready.head_sha = head_sha
        client.snapshot.return_value = ready

        with (
            patch("agent_merge_queue.cli.time.monotonic", return_value=0),
            patch("agent_merge_queue.cli.time.sleep"),
        ):
            result = settle_integration_checks(
                client,
                timeout_seconds=10,
                poll_seconds=0,
                numbers=[number],
            )

        client.dispatch_ci_workflows.assert_called_once_with(
            ref=branch,
            names=["CI"],
        )
        client.snapshot.assert_called_once_with(
            number,
            require_marker=False,
            allow_blocked_label=True,
            known_checks={"CI": "passed"},
        )
        self.assertEqual(result[0]["state"], "ready")

    def test_integration_promotion_reuses_owned_exact_checks(self) -> None:
        number = 38
        head_sha = "a" * 40
        marker = {
            "batch_id": "batch",
            "conflict": None,
            "heads": {"1": "1" * 40, "2": "2" * 40},
            "pull_requests": [1, 2],
        }
        client = Mock()
        client.config = CONFIG
        client.coordinator_logins = {"coordinator"}
        client.integration_pull_request_numbers.return_value = [number]
        client.comments.return_value = [
            {
                "created_at": "2026-06-20T00:00:00Z",
                "user": {"login": "coordinator"},
                "body": integration_body(marker),
            }
        ]
        client.labels.return_value = set()
        ready = entry(8, "combined.py")
        ready.number = number
        ready.head_sha = head_sha
        client.snapshot.return_value = ready

        promoted = promote_integrations(
            client,
            known_checks_by_number={number: {"CI": "passed"}},
        )

        self.assertEqual(promoted, [number])
        client.snapshot.assert_called_once_with(
            number,
            require_marker=False,
            allow_blocked_label=True,
            known_checks={"CI": "passed"},
        )

    def test_reactor_pauses_when_post_merge_ci_dispatch_fails(self) -> None:
        client = Mock()
        client.config = CONFIG
        client.pipeline_control.return_value = {"state": "running"}
        client.dispatch_ci_workflows.side_effect = QueueError("CI has no dispatch")
        empty = FreezeResult(None, [], [], [], [])
        with (
            patch("agent_merge_queue.cli.promote_integrations", return_value=[]),
            patch("agent_merge_queue.cli.command_promote", return_value={}),
            patch("agent_merge_queue.cli.freeze_queue", return_value=empty),
            patch(
                "agent_merge_queue.cli.command_drain",
                return_value={"merged": [{"number": 1, "merge_sha": "a" * 40}]},
            ),
            redirect_stdout(io.StringIO()),
            self.assertRaisesRegex(QueueError, "CI has no dispatch"),
        ):
            command_react(
                client,
                follow=False,
                timeout_seconds=10,
                dispatch_ci=True,
            )
        client.set_pipeline_control.assert_called_once_with(
            "paused", "post-merge CI dispatch failed: CI has no dispatch"
        )

    def test_github_dispatches_each_configured_active_ci_workflow(self) -> None:
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client._json = Mock(
            return_value=[
                {"id": 7, "name": "CI", "state": "active"},
                {"id": 8, "name": "Old CI", "state": "disabled_manually"},
            ]
        )
        client._run = Mock(return_value="")

        result = client.dispatch_ci_workflows()

        self.assertEqual(result, [{"id": 7, "name": "CI"}])
        client._run.assert_called_once_with(
            "workflow", "run", "7", "--repo", "example/repo", "--ref", "main"
        )

    def test_github_dispatches_deploy_with_exact_successful_ci(self) -> None:
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client._json = Mock(
            return_value=[{"id": 8, "name": "Deploy", "state": "active"}]
        )
        client._run = Mock(return_value="")
        sha = "a" * 40

        result = client.dispatch_deploy_workflows(ci_run={"id": 42, "head_sha": sha})

        self.assertEqual(
            result,
            [{"id": 8, "name": "Deploy", "ci_sha": sha, "ci_run_id": 42}],
        )
        client._run.assert_called_once_with(
            "workflow",
            "run",
            "8",
            "--repo",
            "example/repo",
            "--ref",
            "main",
            "-f",
            f"ci_sha={sha}",
            "-f",
            "ci_run_id=42",
        )

    def test_pipeline_pause_blocks_every_merge_path(self) -> None:
        client = Mock()
        client.pipeline_control.return_value = {
            "state": "paused",
            "reason": "main CI failed",
        }
        commands = (
            lambda: command_merge(client, "1", "batch-1"),
            lambda: command_drain(client, json_output=True),
            lambda: command_integrate(client, all_entries=True),
        )
        for command in commands:
            with (
                self.subTest(command=command),
                self.assertRaisesRegex(QueueError, "main CI failed"),
            ):
                command()
        client.resolve_pr.assert_not_called()


if __name__ == "__main__":
    unittest.main()
