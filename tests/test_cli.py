from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from unittest.mock import Mock, call, patch

from agent_merge_queue.cli import (
    FreezeResult,
    GitHub,
    QueueEntry,
    QueueError,
    RELEASE_REPAIR_LEASE_PREFIX,
    active_batch,
    batch_fingerprint,
    batch_overlap_peers,
    bounded_batch_entries,
    check_states,
    command_block,
    command_dequeue,
    command_drain,
    command_enqueue,
    command_integrate,
    command_follow,
    command_merge,
    command_promote,
    command_react,
    command_refresh_request,
    command_request,
    command_resume,
    command_thread_acknowledge,
    command_unblock,
    command_unpause,
    completed_batch_ids,
    delivery_metrics,
    deployment_repair_required,
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
    pipeline_status,
    promote_integrations,
    pull_request_feature_summary,
    queue_state_body,
    queue_from_intent,
    queue_timestamp,
    reconcile_externally_merged_threads,
    record_repair,
    repair_overlap_hold_active,
    reusable_batch,
    settle_integration_checks,
    should_settle_batch,
    structured_dependencies,
    thread_deployment_notification,
    thread_notification_id,
)
from agent_merge_queue.config import parse_config
from agent_merge_queue.records import (
    control_body,
    integration_body,
    intent_body,
    release_repair_body,
    repair_body,
)
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


def deployment_notification(
    *, main_sha: str, state: str = "pending", merge_sha: str = "m" * 40
) -> dict[str, object]:
    repository = "example/repo"
    provider = "codex"
    thread_id = "thread-42"
    return {
        "notification_id": thread_notification_id(
            repository=repository,
            provider=provider,
            thread_id=thread_id,
            merge_sha=merge_sha,
            pull_request=42,
        ),
        "provider": provider,
        "thread_id": thread_id,
        "state": state,
        "updated_at": "2026-06-20T00:05:00Z",
        "repository": repository,
        "main_sha": main_sha,
        "message": f"Deployed on {main_sha}",
        "merge_sha": merge_sha,
        "pull_request": 42,
        "ci_url": "https://example.test/ci/1",
        "deployment_url": "https://example.test/deploy/2",
    }


class QueueCoreTest(unittest.TestCase):
    def test_follow_binds_pause_to_observed_failed_main(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.config = CONFIG
        release = {
            "state": "ci-failed",
            "main_sha": sha,
            "latest_ci": {},
            "latest_deploy": None,
            "verifications": [],
        }

        with patch("agent_merge_queue.cli.follow_release", return_value=release):
            result = command_follow(
                client,
                timeout_seconds=10,
                poll_seconds=1,
                json_output=True,
                emit=False,
            )

        self.assertEqual(result, release)
        client.set_pipeline_control.assert_called_once_with(
            "paused", f"ci-failed on {sha}", main_sha=sha
        )

    def test_pull_release_details_reads_human_facing_metadata(self) -> None:
        client = object.__new__(GitHub)
        client.repository = "example/repo"
        client._json = Mock(
            return_value={
                "body": "## What changed\n- Added receipts",
                "title": "Readable deployment receipts",
                "url": "https://example.test/pull/42",
            }
        )

        self.assertEqual(
            client.pull_release_details(42),
            {
                "body": "## What changed\n- Added receipts",
                "title": "Readable deployment receipts",
                "url": "https://example.test/pull/42",
            },
        )
        client._json.assert_called_once_with(
            "pr",
            "view",
            "42",
            "--repo",
            "example/repo",
            "--json",
            "body,title,url",
        )

    def test_ensure_labels_exist_is_race_safe(self) -> None:
        client = object.__new__(GitHub)
        client.repository = "example/repo"
        client._json = Mock(return_value=[])
        client._run = Mock()
        client.label_specs = Mock(
            return_value=(("merge-queue", "0E8A16", "Queue label"),)
        )

        client.ensure_labels_exist()

        client._run.assert_called_once_with(
            "label",
            "create",
            "merge-queue",
            "--repo",
            "example/repo",
            "--color",
            "0E8A16",
            "--description",
            "Queue label",
            "--force",
        )

    def test_comment_batch_normalizes_graphql_and_falls_back_when_truncated(
        self,
    ) -> None:
        client = object.__new__(GitHub)
        client.owner = "example"
        client.name = "repo"
        client._json = Mock(
            return_value={
                "data": {
                    "repository": {
                        "pr_1": {
                            "comments": {
                                "pageInfo": {"hasPreviousPage": False},
                                "nodes": [
                                    {
                                        "databaseId": 11,
                                        "body": "one",
                                        "createdAt": "2026-06-20T00:00:00Z",
                                        "author": {"login": "trusted"},
                                    }
                                ],
                            }
                        },
                        "pr_2": {
                            "comments": {
                                "pageInfo": {"hasPreviousPage": True},
                                "nodes": [],
                            }
                        },
                    }
                }
            }
        )
        client.comments = Mock(return_value=[{"id": 22, "body": "fallback"}])

        result = client.comments_for_pull_requests([2, 1])

        self.assertEqual(result[1][0]["user"]["login"], "trusted")
        self.assertEqual(result[1][0]["created_at"], "2026-06-20T00:00:00Z")
        self.assertEqual(result[2][0]["body"], "fallback")
        client.comments.assert_called_once_with(2)

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

    def test_draft_with_pending_gates_waits_without_repair(self) -> None:
        value = entry(1, state="blocked")
        value.is_draft = True
        value.checks = {"CI": "pending", "Optional": "failed"}
        value.reasons = [
            "pull request is draft",
            "GitHub reports the pull request merge state as DRAFT",
            "CI is not complete",
            "Greptile score is missing for the current head",
            "Greptile has not reviewed the current head",
            "0/1 exact-head approvals complete",
        ]
        value.review_verdicts = (
            ReviewVerdict(
                "Greptile",
                "waiting",
                (
                    "Greptile score is missing for the current head",
                    "Greptile has not reviewed the current head",
                ),
            ),
            ReviewVerdict(
                "GitHub approvals",
                "waiting",
                ("0/1 exact-head approvals complete",),
            ),
        )

        self.assertFalse(deployment_repair_required(value))

        value.reasons.append("CI failed")
        self.assertTrue(deployment_repair_required(value))

    def test_status_exposes_intent_head_overlap_and_request_delay(self) -> None:
        draft = entry(1, "shared.py", state="blocked")
        draft.labels = ["deploy-requested"]
        draft.is_draft = True
        draft.checks = {"CI": "pending"}
        draft.reasons = ["pull request is draft", "CI is not complete"]
        conflict = entry(2, "shared.py", state="blocked")
        conflict.labels = ["deploy-requested"]
        conflict.reasons = ["pull request conflicts with main"]
        intents = {
            number: {
                "id": number,
                "created_at": "2026-06-20T00:00:00Z",
                "user": {"login": "trusted"},
                "body": intent_body(
                    intent_id=f"intent-{number}",
                    state="requested",
                    requested_at="2026-06-20T00:00:00Z",
                    requested_head=value.head_sha,
                ),
            }
            for number, value in ((1, draft), (2, conflict))
        }
        client = Mock()
        client.config = CONFIG
        client.trusted_logins = {"trusted"}
        client.queue.return_value = []
        inactive = entry(3, "shared.py")
        inactive.labels = []
        intents[3] = {
            "id": 3,
            "created_at": "2026-06-20T00:00:00Z",
            "user": {"login": "trusted"},
            "body": intent_body(
                intent_id="intent-3",
                state="requested",
                requested_at="2026-06-20T00:00:00Z",
                requested_head=inactive.head_sha,
            ),
        }
        client.open_pull_request_numbers.return_value = [1, 2, 3]
        client.comments.side_effect = lambda number: [intents[number]]
        client.snapshot.side_effect = lambda number, **_kwargs: {
            1: draft,
            2: conflict,
            3: inactive,
        }[number]
        client.base_sha.return_value = "f" * 40
        client.workflow_runs.return_value = []
        client.workflow_runs_for_workflows.return_value = []
        client.pipeline_control.return_value = {"state": "running"}
        client.thread_records.return_value = []
        client.deployment_notifications.return_value = []
        client.registry_comments.return_value = []

        result = pipeline_status(client)

        self.assertEqual(
            [value["number"] for value in result["pull_requests"]["deploy_requested"]],
            [1],
        )
        self.assertEqual(
            [value["number"] for value in result["pull_requests"]["blocked"]],
            [2],
        )
        self.assertTrue(
            result["pull_requests"]["deploy_requested"][0]["deploy_intent"][
                "head_matches"
            ]
        )
        self.assertEqual(
            result["active_intent_overlap_groups"][0]["pull_requests"], [1, 2]
        )
        self.assertEqual({alert["pull_request"] for alert in result["alerts"]}, {1, 2})
        self.assertTrue(
            all(alert["stage"] == "request-to-ready" for alert in result["alerts"])
        )

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

    def test_overlap_mode_holds_ready_peer_for_bounded_genuine_repair(
        self,
    ) -> None:
        config = parse_config(
            {
                "queue": {"required_checks": ["CI"], "trusted_actors": ["trusted"]},
                "integration": {"mode": "overlap"},
            }
        )
        ready = entry(1, "shared.py")
        repairing = entry(2, state="blocked")
        repairing.labels = ["deploy-requested", "merge-queue-blocked"]
        repairing.repair_overlap_hold = True
        client = Mock()
        client.config = config
        client.coordinator_logins = {"coordinator"}
        client.changed_paths.return_value = (["shared.py"], [])
        client.comments.return_value = []

        self.assertTrue(should_settle_batch(client, [ready, repairing]))
        self.assertEqual(
            near_ready_overlap_holds(client, [ready, repairing]),
            {1: [2]},
        )

    def test_repair_overlap_hold_is_bounded_and_intent_scoped(self) -> None:
        value = entry(7, state="blocked")
        value.labels = ["deploy-requested", "merge-queue-blocked"]
        intent = {"intent_id": "intent-7"}
        repair = {
            "created_at": "2026-06-21T12:00:00Z",
            "head_sha": value.head_sha,
            "intent_id": "intent-7",
            "pull_request": 7,
            "reason": "pull request conflicts with main",
        }
        client = Mock()
        client.config = parse_config(
            {
                "queue": {"required_checks": ["CI"], "trusted_actors": ["trusted"]},
                "pipeline": {"repair_hold_minutes": 60},
            }
        )

        self.assertTrue(
            repair_overlap_hold_active(
                client,
                value,
                intent,
                repair,
                now="2026-06-21T12:59:59Z",
            )
        )
        self.assertFalse(
            repair_overlap_hold_active(
                client,
                value,
                intent,
                repair,
                now="2026-06-21T13:00:01Z",
            )
        )
        self.assertFalse(
            repair_overlap_hold_active(
                client,
                value,
                {"intent_id": "intent-new"},
                repair,
                now="2026-06-21T12:30:00Z",
            )
        )
        renewed = {
            **repair,
            "created_at": "2026-06-21T12:50:00Z",
            "hold_started_at": "2026-06-21T11:45:00Z",
        }
        self.assertFalse(
            repair_overlap_hold_active(
                client,
                value,
                intent,
                renewed,
                now="2026-06-21T12:50:01Z",
            )
        )

    @patch("agent_merge_queue.cli.utc_now", return_value="2026-06-21T13:00:00Z")
    def test_record_repair_does_not_reuse_marker_from_previous_intent(
        self,
        _utc_now: Mock,
    ) -> None:
        value = entry(7, state="blocked")
        previous = {
            "created_at": "2026-06-21T12:00:00Z",
            "head_sha": value.head_sha,
            "hold_started_at": "2026-06-21T12:00:00Z",
            "intent_id": "intent-old",
            "pull_request": 7,
            "reason": "pull request conflicts with main",
        }
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.coordinator_logins = {"coordinator"}
        client.comments.return_value = [
            {
                "body": repair_body(previous),
                "created_at": previous["created_at"],
                "user": {"login": "coordinator"},
            }
        ]
        client.base_sha.return_value = "b" * 40
        client.labels.return_value = []

        repair = record_repair(
            client,
            value,
            {"intent_id": "intent-new"},
            str(previous["reason"]),
        )

        self.assertEqual(repair["intent_id"], "intent-new")
        self.assertEqual(repair["hold_started_at"], "2026-06-21T13:00:00Z")
        self.assertIn("merge-queue-blocked", value.labels)
        client.comment.assert_called_once_with(7, repair_body(repair))

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
        self.assertEqual(record.merge_sha, merge_sha)
        obligation = client.record_deployment_notification.call_args.args[0]
        self.assertEqual(obligation.state, "awaiting-verification")
        self.assertEqual(obligation.merge_sha, merge_sha)

    def test_reconcile_marks_closed_unmerged_requested_thread_abandoned(self) -> None:
        head_sha = "a" * 40
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
                ),
            }
        ]
        client.externally_integrated_merge.return_value = None
        client.pull_head.return_value = {"state": "CLOSED", "head_sha": head_sha}

        with patch("agent_merge_queue.cli.notify") as notify:
            result = reconcile_externally_merged_threads(client)

        self.assertEqual(result, [{"pull_request": 42, "state": "abandoned"}])
        self.assertEqual(client.record_thread.call_args.args[0].phase, "abandoned")
        notify.assert_called_once_with(
            CONFIG.pipeline,
            "deploy-abandoned",
            {"repository": "example/repo", "pull_request": 42, "state": "abandoned"},
        )

    def test_verified_release_creates_native_thread_notification(self) -> None:
        sha = "a" * 40
        release = {
            "state": "verified",
            "main_sha": sha,
            "latest_ci": {"url": "https://example.test/ci/1"},
            "latest_deploy": {"url": "https://example.test/deploy/2"},
            "verifications": [],
        }
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.is_ancestor.return_value = True
        client.deployment_notifications.return_value = []
        client.pull_release_details.return_value = {
            "title": "Add human-readable deployment receipts",
            "url": "https://example.test/pull/42",
            "body": """## What changed
- Names the deployed change.
- Summarizes the user-facing features.
- Keeps exact release evidence.
- This fourth item is intentionally omitted.

## Validation
- 144 tests passed.
""",
        }
        client.thread_records.return_value = [
            {
                "phase": "merged",
                "provider": "codex",
                "thread_id": "thread-42",
                "pull_request": 42,
                "url": "codex://thread/thread-42",
                "merge_sha": "m" * 40,
            }
        ]

        with (
            patch("agent_merge_queue.cli.follow_release", return_value=release),
            patch("agent_merge_queue.cli.notify") as notify,
            patch(
                "agent_merge_queue.cli.utc_now",
                return_value="2026-06-20T00:05:00Z",
            ),
        ):
            result = command_follow(
                client,
                timeout_seconds=10,
                poll_seconds=1,
                json_output=True,
                emit=False,
            )

        notification = result["thread_notifications"][0]
        self.assertEqual(notification["provider"], "codex")
        self.assertEqual(notification["thread_id"], "thread-42")
        self.assertEqual(notification["pull_request"], 42)
        self.assertEqual(notification["main_sha"], sha)
        self.assertEqual(
            notification["deployment_url"], "https://example.test/deploy/2"
        )
        self.assertIn("Deployment complete", notification["message"])
        self.assertIn(
            "` Add human-readable deployment receipts ` "
            "([PR #42](https://example.test/pull/42)) is now live.",
            notification["message"],
        )
        self.assertIn("- ` Names the deployed change. `", notification["message"])
        self.assertIn(
            "- ` Summarizes the user-facing features. `", notification["message"]
        )
        self.assertIn("- ` Keeps exact release evidence. `", notification["message"])
        self.assertNotIn("fourth item", notification["message"])
        self.assertNotIn("144 tests", notification["message"])
        self.assertIn(f"- Exact main: `{sha}`", notification["message"])
        self.assertIn("[CI run](https://example.test/ci/1)", notification["message"])
        self.assertTrue(notification["notification_id"].startswith("thread-deployed:"))
        deployed = client.record_thread.call_args.args[0]
        outbox = client.record_deployment_notification.call_args.args[0]
        self.assertEqual(outbox.state, "pending")
        self.assertEqual(outbox.notification_id, notification["notification_id"])
        self.assertEqual(deployed.phase, "deployed")
        self.assertEqual(deployed.merge_sha, "m" * 40)
        self.assertEqual(deployed.deployed_sha, sha)
        self.assertEqual(deployed.ci_url, "https://example.test/ci/1")
        self.assertEqual(deployed.deployment_url, "https://example.test/deploy/2")
        client.is_ancestor.assert_called_once_with("m" * 40, sha)
        client.pull_release_details.assert_called_once_with(42)
        notify.assert_any_call(CONFIG.pipeline, "thread-deployed", notification)

    def test_pull_request_feature_summary_prefers_release_notes(self) -> None:
        body = """Introductory context.

## Features
1. First feature
2. Second feature
2. Second feature

## Test plan
- This should not appear.
"""

        self.assertEqual(
            pull_request_feature_summary(body),
            ["First feature", "Second feature"],
        )

    def test_deployment_receipt_neutralizes_untrusted_markdown(self) -> None:
        notification = thread_deployment_notification(
            repository="example/repo",
            record={
                "provider": "codex",
                "thread_id": "thread-42",
                "pull_request": 42,
                "merge_sha": "m" * 40,
            },
            release={"main_sha": "a" * 40},
            pull_request_details={
                "title": (
                    "[Reauthenticate](https://attacker.example) "
                    "![](https://attacker.example/pixel)"
                ),
                "body": (
                    "## What changed\n"
                    "- Review [details](https://attacker.example/details) and "
                    "![pixel](https://attacker.example/pixel)"
                ),
                "url": "https://example.test/pull/42",
            },
        )

        self.assertNotIn("attacker.example", notification["message"])
        self.assertIn("` Reauthenticate `", notification["message"])
        self.assertIn("- ` Review details and pixel `", notification["message"])
        self.assertIn("[PR #42](https://example.test/pull/42)", notification["message"])

    def test_verified_release_promotes_obligation_after_thread_moves_on(self) -> None:
        sha = "a" * 40
        merge_sha = "m" * 40
        obligation = deployment_notification(main_sha=sha, merge_sha=merge_sha)
        obligation.update(
            {
                "state": "awaiting-verification",
                "main_sha": None,
                "message": None,
                "ci_url": None,
                "deployment_url": None,
            }
        )
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.is_ancestor.return_value = True
        client.deployment_notifications.return_value = [obligation]
        client.pull_release_details.side_effect = QueueError("metadata unavailable")
        client.thread_records.return_value = [
            {
                "phase": "working",
                "provider": "codex",
                "thread_id": "thread-42",
            }
        ]
        release = {
            "state": "verified",
            "main_sha": sha,
            "latest_ci": {"url": "https://example.test/ci/1"},
            "latest_deploy": {"url": "https://example.test/deploy/2"},
            "verifications": [],
        }

        with (
            patch("agent_merge_queue.cli.follow_release", return_value=release),
            patch("agent_merge_queue.cli.notify"),
        ):
            result = command_follow(
                client,
                timeout_seconds=10,
                poll_seconds=1,
                json_output=True,
                emit=False,
            )

        notification = result["thread_notifications"][0]
        self.assertEqual(notification["notification_id"], obligation["notification_id"])
        self.assertEqual(notification["main_sha"], sha)
        self.assertIn(
            "**[PR #42](https://github.com/example/repo/pull/42)** is now live.",
            notification["message"],
        )
        pending = client.record_deployment_notification.call_args.args[0]
        self.assertEqual(pending.state, "pending")
        self.assertEqual(pending.merge_sha, merge_sha)
        client.record_thread.assert_not_called()

    def test_verified_release_does_not_notify_uncontained_merge(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.is_ancestor.return_value = False
        client.deployment_notifications.return_value = []
        client.thread_records.return_value = [
            {
                "phase": "merged",
                "provider": "codex",
                "thread_id": "thread-42",
                "pull_request": 42,
                "merge_sha": "m" * 40,
            }
        ]
        release = {
            "state": "verified",
            "main_sha": sha,
            "latest_ci": None,
            "latest_deploy": None,
            "verifications": [],
        }

        with (
            patch("agent_merge_queue.cli.follow_release", return_value=release),
            patch("agent_merge_queue.cli.notify") as notify,
        ):
            result = command_follow(
                client,
                timeout_seconds=10,
                poll_seconds=1,
                json_output=True,
                emit=False,
            )

        self.assertEqual(result["thread_notifications"], [])
        client.record_thread.assert_not_called()
        client.record_deployment_notification.assert_not_called()
        client.is_ancestor.assert_called_once_with("m" * 40, sha)
        self.assertEqual(notify.call_count, 1)
        self.assertEqual(notify.call_args.args[1], "verified")

    def test_verified_release_resolves_legacy_merged_record(self) -> None:
        sha = "a" * 40
        merge_sha = "m" * 40
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.pull_merge_sha.return_value = merge_sha
        client.is_ancestor.return_value = True
        client.deployment_notifications.return_value = []
        client.thread_records.return_value = [
            {
                "phase": "merged",
                "provider": "codex",
                "thread_id": "legacy-thread",
                "pull_request": 42,
            }
        ]
        release = {
            "state": "verified",
            "main_sha": sha,
            "latest_ci": None,
            "latest_deploy": None,
            "verifications": [],
        }

        with (
            patch("agent_merge_queue.cli.follow_release", return_value=release),
            patch("agent_merge_queue.cli.notify"),
        ):
            result = command_follow(
                client,
                timeout_seconds=10,
                poll_seconds=1,
                json_output=True,
                emit=False,
            )

        self.assertEqual(result["thread_notifications"][0]["merge_sha"], merge_sha)
        self.assertEqual(client.record_thread.call_args.args[0].merge_sha, merge_sha)
        client.pull_merge_sha.assert_called_once_with(42)
        client.is_ancestor.assert_called_once_with(merge_sha, sha)

    def test_pending_thread_notification_retries_with_stable_identity(self) -> None:
        deployed_sha = "a" * 40
        current_sha = "b" * 40
        release = {
            "state": "verified",
            "main_sha": current_sha,
            "latest_ci": {"url": "https://example.test/ci/new"},
            "latest_deploy": {"url": "https://example.test/deploy/new"},
            "verifications": [],
        }
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        pending = deployment_notification(main_sha=deployed_sha)
        pending["ci_url"] = "https://example.test/ci/original"
        pending["deployment_url"] = "https://example.test/deploy/original"
        client.deployment_notifications.return_value = [pending]
        client.thread_records.return_value = [
            {
                "phase": "working",
                "provider": "codex",
                "thread_id": "thread-42",
            }
        ]
        with (
            patch("agent_merge_queue.cli.follow_release", return_value=release),
            patch("agent_merge_queue.cli.notify"),
        ):
            first = command_follow(
                client,
                timeout_seconds=10,
                poll_seconds=1,
                json_output=True,
                emit=False,
            )
            second = command_follow(
                client,
                timeout_seconds=10,
                poll_seconds=1,
                json_output=True,
                emit=False,
            )

        self.assertEqual(
            first["thread_notifications"][0]["notification_id"],
            second["thread_notifications"][0]["notification_id"],
        )
        self.assertEqual(first["thread_notifications"][0]["main_sha"], deployed_sha)
        self.assertEqual(
            first["thread_notifications"][0]["ci_url"],
            "https://example.test/ci/original",
        )
        self.assertEqual(
            first["thread_notifications"][0]["deployment_url"],
            "https://example.test/deploy/original",
        )
        self.assertNotIn("/new", first["thread_notifications"][0]["message"])
        client.record_thread.assert_not_called()
        client.record_deployment_notification.assert_not_called()

    def test_delivered_notification_is_not_reopened_by_release_follow(self) -> None:
        sha = "a" * 40
        merge_sha = "m" * 40
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.is_ancestor.return_value = True
        client.deployment_notifications.return_value = [
            deployment_notification(
                main_sha=sha,
                merge_sha=merge_sha,
                state="delivered",
            )
        ]
        client.thread_records.return_value = [
            {
                "phase": "merged",
                "provider": "codex",
                "thread_id": "thread-42",
                "pull_request": 42,
                "merge_sha": merge_sha,
            }
        ]
        release = {
            "state": "verified",
            "main_sha": sha,
            "latest_ci": None,
            "latest_deploy": None,
            "verifications": [],
        }

        with (
            patch("agent_merge_queue.cli.follow_release", return_value=release),
            patch("agent_merge_queue.cli.notify") as notify,
        ):
            result = command_follow(
                client,
                timeout_seconds=10,
                poll_seconds=1,
                json_output=True,
                emit=False,
            )

        self.assertEqual(result["thread_notifications"], [])
        client.record_deployment_notification.assert_not_called()
        client.record_thread.assert_not_called()
        client.pull_release_details.assert_not_called()
        self.assertEqual(notify.call_count, 1)
        self.assertEqual(notify.call_args.args[1], "verified")

    def test_clients_cannot_publish_controller_owned_deployed_phase(self) -> None:
        with (
            redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit),
        ):
            main(
                [
                    "thread",
                    "update",
                    "--provider",
                    "codex",
                    "--thread-id",
                    "thread-42",
                    "--phase",
                    "deployed",
                ]
            )

    def test_unverified_notification_obligation_cannot_be_acknowledged(self) -> None:
        notification = deployment_notification(main_sha="a" * 40)
        notification.update(
            {
                "state": "awaiting-verification",
                "main_sha": None,
                "message": None,
            }
        )
        client = Mock()
        client.deployment_notifications.return_value = [notification]

        with self.assertRaisesRegex(QueueError, "is not deployed"):
            command_thread_acknowledge(
                client,
                provider="codex",
                thread_id="thread-42",
                notification_id=str(notification["notification_id"]),
            )

        client.record_deployment_notification.assert_not_called()

    def test_acknowledging_native_thread_message_completes_record(self) -> None:
        sha = "a" * 40
        merge_sha = "m" * 40
        client = Mock()
        client.repository = "example/repo"
        notification = deployment_notification(main_sha=sha, merge_sha=merge_sha)
        client.deployment_notifications.return_value = [notification]
        client.thread_records.return_value = [
            {
                "phase": "deployed",
                "provider": "codex",
                "thread_id": "thread-42",
                "pull_request": 42,
                "merge_sha": merge_sha,
                "deployed_sha": sha,
                "ci_url": "https://example.test/ci/1",
                "deployment_url": "https://example.test/deploy/2",
            }
        ]
        with patch(
            "agent_merge_queue.cli.utc_now",
            return_value="2026-06-20T00:06:00Z",
        ):
            result = command_thread_acknowledge(
                client,
                provider="codex",
                thread_id="thread-42",
                notification_id=str(notification["notification_id"]),
            )

        client.deployment_notifications.assert_called_once_with(include_delivered=True)
        client.thread_records.assert_called_once_with(include_terminal=True)
        delivered = client.record_deployment_notification.call_args.args[0]
        self.assertEqual(delivered.state, "delivered")
        self.assertEqual(delivered.notification_id, notification["notification_id"])
        completed = client.record_thread.call_args.args[0]
        self.assertEqual(completed.phase, "completed")
        self.assertEqual(completed.deployed_sha, sha)
        self.assertEqual(completed.ci_url, "https://example.test/ci/1")
        self.assertEqual(completed.deployment_url, "https://example.test/deploy/2")
        self.assertEqual(result["state"], "delivered")

    def test_acknowledgement_completes_merge_when_phase_write_was_interrupted(
        self,
    ) -> None:
        sha = "a" * 40
        merge_sha = "m" * 40
        client = Mock()
        notification = deployment_notification(main_sha=sha, merge_sha=merge_sha)
        client.deployment_notifications.return_value = [notification]
        client.thread_records.return_value = [
            {
                "phase": "merged",
                "provider": "codex",
                "thread_id": "thread-42",
                "pull_request": 42,
                "merge_sha": merge_sha,
            }
        ]

        command_thread_acknowledge(
            client,
            provider="codex",
            thread_id="thread-42",
            notification_id=str(notification["notification_id"]),
        )

        self.assertEqual(client.record_thread.call_args.args[0].phase, "completed")
        self.assertEqual(
            client.record_thread.call_args.args[0].deployed_sha,
            sha,
        )

    def test_thread_acknowledgement_is_idempotent(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.repository = "example/repo"
        notification = deployment_notification(main_sha=sha, state="delivered")
        client.deployment_notifications.return_value = [notification]
        client.thread_records.return_value = [
            {
                "phase": "completed",
                "provider": "codex",
                "thread_id": "thread-42",
                "deployed_sha": sha,
            }
        ]

        result = command_thread_acknowledge(
            client,
            provider="codex",
            thread_id="thread-42",
            notification_id=str(notification["notification_id"]),
        )

        self.assertEqual(result["notification_id"], notification["notification_id"])
        self.assertEqual(result["state"], "delivered")
        client.record_deployment_notification.assert_not_called()
        client.record_thread.assert_not_called()

    def test_old_acknowledgement_does_not_complete_newer_thread_state(self) -> None:
        old_sha = "a" * 40
        new_sha = "b" * 40
        client = Mock()
        client.repository = "example/repo"
        notification = deployment_notification(main_sha=old_sha)
        client.deployment_notifications.return_value = [notification]
        client.thread_records.return_value = [
            {
                "phase": "deployed",
                "provider": "codex",
                "thread_id": "thread-42",
                "merge_sha": "n" * 40,
                "deployed_sha": new_sha,
            }
        ]

        result = command_thread_acknowledge(
            client,
            provider="codex",
            thread_id="thread-42",
            notification_id=str(notification["notification_id"]),
        )

        self.assertEqual(result["state"], "delivered")
        client.record_deployment_notification.assert_called_once()
        client.record_thread.assert_not_called()

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

    def test_generated_paths_do_not_create_hand_edited_overlap(self) -> None:
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                },
                "files": {"generated_paths": ["dist/app.js"]},
            }
        )
        client = object.__new__(GitHub)
        client.config = config
        client.files = Mock(
            return_value=[
                {"filename": "src/app.js", "patch": "+code"},
                {"filename": "dist/app.js", "patch": None},
            ]
        )

        source_paths, generated_paths = client.changed_paths(1)

        self.assertEqual(source_paths, ["src/app.js"])
        self.assertEqual(generated_paths, ["dist/app.js"])

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

    def test_generated_overlap_still_requires_cumulative_validation(self) -> None:
        first = entry(1, "src/a.py")
        second = entry(2, "src/b.py")
        first.generated_paths = ["dist/app.js"]
        second.generated_paths = ["dist/app.js"]

        groups = overlap_groups([first, second])

        self.assertEqual(groups[0]["pull_requests"], [1, 2])
        self.assertEqual(groups[0]["source_paths"], [])
        self.assertEqual(groups[0]["generated_paths"], ["dist/app.js"])

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

    def test_limited_workflow_history_uses_one_page(self) -> None:
        client = object.__new__(GitHub)
        client.repository = "example/repo"
        client.config = CONFIG
        client._json = Mock(
            return_value={"workflow_runs": [{"id": number} for number in range(100)]}
        )

        result = client.workflow_runs(limit=25)

        self.assertEqual([run["id"] for run in result], list(range(25)))
        self.assertNotIn("--paginate", client._json.call_args.args)

    def test_successful_workflow_history_filters_by_workflow(self) -> None:
        client = object.__new__(GitHub)
        client.repository = "example/repo"
        client.config = CONFIG
        client._json = Mock(
            side_effect=[
                [{"id": 7, "name": "Deploy", "state": "active"}],
                {"workflow_runs": [{"id": 9, "name": "Deploy"}]},
            ]
        )

        result = client.successful_workflow_runs(["Deploy"], limit=25)

        self.assertEqual(result, [{"id": 9, "name": "Deploy"}])
        self.assertIn(
            "actions/workflows/7/runs?branch=main&status=success&per_page=25&page=1",
            client._json.call_args.args[-1],
        )

    def test_workflow_histories_are_combined_by_recency_before_limit(self) -> None:
        client = object.__new__(GitHub)
        client.repository = "example/repo"
        client.config = CONFIG
        client._json = Mock(
            side_effect=[
                [
                    {"id": 7, "name": "Deploy A", "state": "active"},
                    {"id": 8, "name": "Deploy B", "state": "active"},
                ],
                {"workflow_runs": [{"id": 70, "created_at": "2026-06-20T00:00:00Z"}]},
                {"workflow_runs": [{"id": 80, "created_at": "2026-06-20T01:00:00Z"}]},
            ]
        )

        result = client.successful_workflow_runs(["Deploy A", "Deploy B"], limit=1)

        self.assertEqual([run["id"] for run in result], [80])

    def test_workflow_history_pages_until_the_requested_time_window(self) -> None:
        client = object.__new__(GitHub)
        client.repository = "example/repo"
        client.config = CONFIG
        recent = [
            {"id": number, "created_at": "2026-06-20T02:00:00Z"}
            for number in range(100)
        ]
        client._json = Mock(
            side_effect=[
                [{"id": 7, "name": "Deploy", "state": "active"}],
                {"workflow_runs": recent},
                {"workflow_runs": [{"id": 101, "created_at": "2026-06-20T00:00:00Z"}]},
            ]
        )

        result = client.successful_workflow_runs(
            ["Deploy"],
            since=datetime(2026, 6, 20, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(len(result), 100)
        self.assertIn("page=2", client._json.call_args.args[-1])

    def test_recent_merged_pulls_stop_after_enough_results(self) -> None:
        client = object.__new__(GitHub)
        client.repository = "example/repo"
        client._json = Mock(
            return_value=[
                {"number": number, "merged_at": "2026-06-20T00:00:00Z"}
                for number in range(1, 31)
            ]
        )

        result = client.recent_merged_pull_requests(25)

        self.assertEqual([pull["number"] for pull in result], list(range(1, 26)))
        client._json.assert_called_once()

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

    def test_freeze_caps_new_batch_and_preserves_fifo_remainder(self) -> None:
        values = [entry(number, f"{number}.py") for number in range(1, 6)]
        client = Mock()
        client.config = CONFIG
        client.coordinator_logins = {"coordinator"}
        client.queue.return_value = values
        client.comments.return_value = []

        frozen = freeze_queue(client)

        self.assertEqual([value.number for value in frozen.queue], [1, 2, 3])
        self.assertEqual([value.number for value in frozen.next_batch], [4, 5])
        self.assertEqual(frozen.batch["pull_requests"], [1, 2, 3])

    def test_bounded_batch_does_not_split_overlap_component(self) -> None:
        values = [
            entry(1, "a.py"),
            entry(2, "b.py"),
            entry(3, "shared.py"),
            entry(4, "shared.py"),
            entry(5, "c.py"),
        ]

        selected = bounded_batch_entries(values, 3)

        self.assertEqual([value.number for value in selected], [1, 2])

    def test_bounded_batch_includes_queued_dependency_closure(self) -> None:
        dependent = entry(1, "a.py")
        dependent.dependencies = [4]
        values = [dependent, entry(2, "b.py"), entry(3, "c.py"), entry(4, "d.py")]

        selected = bounded_batch_entries(values, 3)

        self.assertEqual([value.number for value in selected], [1, 2, 4])

    def test_oversized_indivisible_overlap_closure_ships_alone(self) -> None:
        values = [entry(number, "shared.py") for number in range(1, 5)]
        values.append(entry(5, "independent.py"))

        selected = bounded_batch_entries(values, 3)

        self.assertEqual([value.number for value in selected], [1, 2, 3, 4])

    def test_generated_overlap_does_not_exceed_batch_limit(self) -> None:
        values = [entry(number, f"src/{number}.py") for number in range(1, 5)]
        for value in values:
            value.generated_paths = ["dist/app.js"]

        selected = bounded_batch_entries(values, 3)

        self.assertEqual([value.number for value in selected], [1, 2, 3])

    def test_reactor_holds_admission_until_existing_release_finishes(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.config = CONFIG
        client.pipeline_control.return_value = {"state": "running"}
        client.base_sha.return_value = sha
        client.workflow_runs.return_value = [
            {
                "id": 1,
                "name": "CI",
                "head_sha": sha,
                "status": "in_progress",
                "conclusion": None,
                "created_at": "2026-06-20T00:00:00Z",
                "updated_at": "2026-06-20T00:00:00Z",
            }
        ]
        with (
            patch(
                "agent_merge_queue.cli.command_follow",
                return_value={"state": "testing", "main_sha": sha},
            ),
            patch("agent_merge_queue.cli.command_promote") as promote,
            redirect_stdout(io.StringIO()),
        ):
            result = command_react(client, follow=True, timeout_seconds=10)

        self.assertEqual(result["state"], "release-held")
        promote.assert_not_called()

    def test_reactor_holds_admission_even_without_follow(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.config = CONFIG
        client.pipeline_control.return_value = {"state": "running"}
        client.base_sha.return_value = sha
        client.workflow_runs.return_value = [
            {
                "id": 1,
                "name": "CI",
                "head_sha": sha,
                "status": "in_progress",
                "conclusion": None,
            }
        ]
        with (
            patch("agent_merge_queue.cli.command_follow") as follow_release,
            patch("agent_merge_queue.cli.command_promote") as promote,
            redirect_stdout(io.StringIO()),
        ):
            result = command_react(client, follow=False, timeout_seconds=10)

        self.assertEqual(result["state"], "release-held")
        follow_release.assert_not_called()
        promote.assert_not_called()

    def test_reactor_ignores_rerun_after_same_main_was_verified(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.config = CONFIG
        client.pipeline_control.return_value = {"state": "running"}
        client.base_sha.return_value = sha
        client.verified_main_sha.return_value = sha
        client.workflow_runs.return_value = [
            {
                "id": 2,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "failure",
            }
        ]
        frozen = FreezeResult(None, [], [], [], [])
        with (
            patch("agent_merge_queue.cli.settle_integration_checks", return_value=[]),
            patch("agent_merge_queue.cli.promote_integrations", return_value=[]),
            patch(
                "agent_merge_queue.cli.command_promote",
                return_value={"promoted": [], "waiting": [], "blocked": []},
            ) as promote,
            patch("agent_merge_queue.cli.freeze_queue", return_value=frozen),
            redirect_stdout(io.StringIO()),
        ):
            result = command_react(client, follow=False, timeout_seconds=10)

        self.assertNotEqual(result.get("state"), "release-held")
        promote.assert_called_once()

    def test_reactor_holds_newly_merged_revision_before_ci_is_visible(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.config = CONFIG
        client.pipeline_control.return_value = {"state": "running"}
        client.base_sha.return_value = sha
        client.workflow_runs.return_value = []
        client.thread_records.return_value = [
            {"phase": "merged", "merge_sha": "b" * 40}
        ]
        client.is_ancestor.return_value = True
        with (
            patch("agent_merge_queue.cli.command_promote") as promote,
            redirect_stdout(io.StringIO()),
        ):
            result = command_react(client, follow=False, timeout_seconds=10)

        self.assertEqual(result["state"], "release-held")
        promote.assert_not_called()

    def test_reactor_seeds_first_install_despite_historical_runs(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.config = CONFIG
        client.pipeline_control.return_value = {"state": "running"}
        client.base_sha.return_value = sha
        client.workflow_runs.return_value = [
            {
                "id": 99,
                "name": "CI",
                "head_sha": "b" * 40,
                "status": "completed",
                "conclusion": "success",
            }
        ]
        client.verified_main_sha.return_value = None
        client.thread_records.return_value = []
        client.deployment_notifications.return_value = []
        frozen = FreezeResult(None, [], [], [], [])
        with (
            patch("agent_merge_queue.cli.settle_integration_checks", return_value=[]),
            patch("agent_merge_queue.cli.promote_integrations", return_value=[]),
            patch(
                "agent_merge_queue.cli.command_promote",
                return_value={"promoted": [], "waiting": [], "blocked": []},
            ) as promote,
            patch("agent_merge_queue.cli.freeze_queue", return_value=frozen),
            redirect_stdout(io.StringIO()),
        ):
            result = command_react(client, follow=False, timeout_seconds=10)

        self.assertNotEqual(result.get("state"), "release-held")
        client.record_verified_main.assert_called_once_with(sha)
        promote.assert_called_once()

    def test_reactor_reconciles_external_merge_before_first_watermark(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.config = CONFIG
        client.pipeline_control.return_value = {"state": "running"}
        client.base_sha.return_value = sha
        client.workflow_runs.return_value = []
        client.verified_main_sha.return_value = None
        client.thread_records.return_value = []
        client.deployment_notifications.return_value = []
        client.is_ancestor.return_value = True
        with (
            patch(
                "agent_merge_queue.cli.reconcile_externally_merged_threads",
                return_value=[{"merge_sha": "b" * 40, "pull_request": 1}],
            ) as reconcile,
            patch("agent_merge_queue.cli.command_promote") as promote,
            redirect_stdout(io.StringIO()),
        ):
            result = command_react(client, follow=False, timeout_seconds=10)

        self.assertEqual(result["state"], "release-held")
        self.assertEqual(result["reconciled_merges"][0]["pull_request"], 1)
        client.record_verified_main.assert_not_called()
        reconcile.assert_called_once_with(client)
        promote.assert_not_called()

    def test_reactor_requires_configured_health_before_reopening_admission(self) -> None:
        sha = "a" * 40
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                },
                "pipeline": {
                    "verifications": [
                        {"name": "Login", "url": "https://example.test/login"}
                    ]
                },
            }
        )
        client = Mock()
        client.config = config
        client.pipeline_control.return_value = {"state": "running"}
        client.base_sha.return_value = sha
        client.workflow_runs.return_value = [
            {
                "id": 1,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
            },
            {
                "id": 2,
                "name": "Deploy",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
            },
        ]
        with (
            patch(
                "agent_merge_queue.cli.http_verifications",
                return_value=[{"name": "Login", "passed": False}],
            ),
            patch("agent_merge_queue.cli.command_promote") as promote,
            redirect_stdout(io.StringIO()),
        ):
            result = command_react(client, follow=False, timeout_seconds=10)

        self.assertEqual(result["state"], "release-held")
        self.assertEqual(result["release"]["state"], "verify-failed")
        promote.assert_not_called()

    def test_release_repair_claim_creates_one_deterministic_lease(self) -> None:
        sha = "a" * 40
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.require_actor = Mock(return_value="trusted")
        client.base_sha = Mock(return_value=sha)
        client.workflow_runs = Mock(
            return_value=[
                {
                    "id": 7,
                    "name": "CI",
                    "head_sha": sha,
                    "status": "completed",
                    "conclusion": "success",
                    "updated_at": "2026-06-20T00:01:00Z",
                },
                {
                    "id": 8,
                    "name": "Deploy",
                    "head_sha": sha,
                    "status": "completed",
                    "conclusion": "failure",
                    "created_at": "2026-06-20T00:02:00Z",
                },
            ]
        )
        client.registry_comments = Mock(return_value=[])
        client.registry_issue_number = Mock(return_value=42)
        client.issue_comment = Mock()
        client._json = Mock(
            side_effect=[
                {"tree": {"sha": "t" * 40}},
                {"sha": "l" * 40},
                {},
            ]
        )

        result = client.claim_release_repair(
            provider="codex",
            thread_id="thread-1",
        )

        self.assertEqual(result["state"], "owned")
        self.assertEqual(result["branch"], f"deploybot/repair/{sha[:12]}")
        self.assertEqual(result["run_id"], 8)
        client.issue_comment.assert_called_once()

    def test_repair_claim_ignores_unbacked_registry_owner(self) -> None:
        sha = "a" * 40
        owner = {
            "branch": f"deploybot/repair/{sha[:12]}",
            "main_sha": sha,
            "provider": "codex",
            "thread_id": "thread-1",
        }
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.require_actor = Mock(return_value="trusted")
        client.base_sha = Mock(return_value=sha)
        client.workflow_runs = Mock(
            return_value=[
                {
                    "id": 7,
                    "name": "CI",
                    "head_sha": sha,
                    "status": "completed",
                    "conclusion": "failure",
                }
            ]
        )
        client.registry_comments = Mock(
            return_value=[
                {
                    "body": release_repair_body(owner),
                    "created_at": "2026-06-20T00:00:00Z",
                    "id": 1,
                    "user": {"login": "coordinator"},
                }
            ]
        )
        client.registry_issue_number = Mock(return_value=42)
        client.issue_comment = Mock()
        client._json = Mock(
            side_effect=[
                {"tree": {"sha": "t" * 40}},
                {"sha": "l" * 40},
                {},
            ]
        )

        result = client.claim_release_repair(
            provider="claude", thread_id="thread-1"
        )

        self.assertEqual(result["state"], "owned")
        self.assertEqual(result["provider"], "claude")
        client.registry_comments.assert_not_called()

    def test_release_repair_claim_accepts_failed_health_verification(self) -> None:
        sha = "a" * 40
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                },
                "pipeline": {
                    "verifications": [
                        {"name": "Login", "url": "https://example.test/login"}
                    ]
                },
            }
        )
        client = object.__new__(GitHub)
        client.config = config
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.require_actor = Mock(return_value="trusted")
        client.base_sha = Mock(return_value=sha)
        client.workflow_runs = Mock(
            return_value=[
                {
                    "id": 7,
                    "name": "CI",
                    "head_sha": sha,
                    "status": "completed",
                    "conclusion": "success",
                },
                {
                    "id": 8,
                    "name": "Deploy",
                    "head_sha": sha,
                    "status": "completed",
                    "conclusion": "success",
                },
            ]
        )
        client.registry_comments = Mock(return_value=[])
        client.registry_issue_number = Mock(return_value=42)
        client.issue_comment = Mock()
        client._json = Mock(
            side_effect=[
                {"tree": {"sha": "t" * 40}},
                {"sha": "l" * 40},
                {},
            ]
        )
        with patch(
            "agent_merge_queue.cli.http_verifications",
            return_value=[{"name": "Login", "passed": False}],
        ):
            result = client.claim_release_repair(
                provider="codex", thread_id="thread-1"
            )

        self.assertEqual(result["failure_state"], "verify-failed")

    def test_existing_repair_ref_recovers_encoded_owner(self) -> None:
        sha = "a" * 40
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.require_actor = Mock(return_value="trusted")
        client.base_sha = Mock(return_value=sha)
        client.workflow_runs = Mock(
            return_value=[
                {
                    "id": 7,
                    "name": "CI",
                    "head_sha": sha,
                    "status": "completed",
                    "conclusion": "failure",
                }
            ]
        )
        client.registry_comments = Mock(return_value=[])
        client.registry_issue_number = Mock(return_value=42)
        client.issue_comment = Mock()
        owner = {
            "branch": f"deploybot/repair/{sha[:12]}",
            "claimed_at": "2026-06-20T00:00:00Z",
            "failure_state": "ci-failed",
            "main_sha": sha,
            "provider": "codex",
            "run_id": 7,
            "thread_id": "thread-1",
            "thread_url": None,
        }
        client._json = Mock(
            side_effect=[
                {"tree": {"sha": "t" * 40}},
                {"sha": "n" * 40},
                QueueError("Reference already exists"),
                {"object": {"sha": "l" * 40}},
                {"message": RELEASE_REPAIR_LEASE_PREFIX + json.dumps(owner)},
            ]
        )

        result = client.claim_release_repair(
            provider="claude",
            thread_id="thread-1",
        )

        self.assertEqual(result["state"], "claimed")
        self.assertEqual(result["thread_id"], "thread-1")
        client.issue_comment.assert_called_once()

    def test_failed_repair_registry_write_is_recoverable_from_lease_ref(self) -> None:
        sha = "a" * 40
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.require_actor = Mock(return_value="trusted")
        client.base_sha = Mock(return_value=sha)
        client.workflow_runs = Mock(
            return_value=[
                {
                    "id": 7,
                    "name": "CI",
                    "head_sha": sha,
                    "status": "completed",
                    "conclusion": "failure",
                }
            ]
        )
        client.registry_comments = Mock(return_value=[])
        client.registry_issue_number = Mock(return_value=42)
        owner = {
            "branch": f"deploybot/repair/{sha[:12]}",
            "claimed_at": "2026-06-20T00:00:00Z",
            "failure_state": "ci-failed",
            "main_sha": sha,
            "provider": "codex",
            "run_id": 7,
            "thread_id": "thread-1",
            "thread_url": None,
        }
        client.issue_comment = Mock(
            side_effect=[QueueError("temporary failure"), None]
        )
        client._json = Mock(
            side_effect=[
                {"tree": {"sha": "t" * 40}},
                {"sha": "l" * 40},
                {},
                {"tree": {"sha": "t" * 40}},
                {"sha": "n" * 40},
                QueueError("Reference already exists"),
                {"object": {"sha": "l" * 40}},
                {"message": RELEASE_REPAIR_LEASE_PREFIX + json.dumps(owner)},
            ]
        )

        with self.assertRaisesRegex(QueueError, "temporary failure"):
            client.claim_release_repair(provider="codex", thread_id="thread-1")

        result = client.claim_release_repair(
            provider="codex", thread_id="thread-1"
        )
        self.assertEqual(result["state"], "owned")
        self.assertEqual(result["lease_sha"], "l" * 40)

    def test_release_repair_claim_rejects_main_that_advances_during_claim(self) -> None:
        sha = "a" * 40
        newer = "b" * 40
        client = object.__new__(GitHub)
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.require_actor = Mock(return_value="trusted")
        client.base_sha = Mock(side_effect=[sha, sha, newer])
        client.workflow_runs = Mock(
            return_value=[
                {
                    "id": 7,
                    "name": "CI",
                    "head_sha": sha,
                    "status": "completed",
                    "conclusion": "failure",
                }
            ]
        )
        client.registry_comments = Mock(return_value=[])
        client._json = Mock(
            side_effect=[
                {"tree": {"sha": "t" * 40}},
                {"sha": "l" * 40},
                {},
            ]
        )
        client._run = Mock(return_value="")

        with self.assertRaisesRegex(QueueError, "advanced during claim"):
            client.claim_release_repair(
                provider="codex", thread_id="thread-1", main_sha=sha
            )

        client._run.assert_called_once()

    def test_integration_can_require_non_actions_author(self) -> None:
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                },
                "integration": {"require_non_actions_author": True},
            }
        )
        client = object.__new__(GitHub)
        client.config = config
        client.repository = "example/repo"
        client.owner = "example"
        client.coordinator_logins = {"trusted"}
        client.base_sha = Mock(return_value="b" * 40)
        client._json = Mock(
            side_effect=[
                {},
                {},
                {},
                [],
                {"number": 99, "user": {"login": "github-actions[bot]"}},
            ]
        )
        client._run = Mock(return_value="")

        with self.assertRaisesRegex(QueueError, "GitHub App installation token"):
            client.create_integration_pull_request(
                batch={"batch_id": "batch"},
                entries=[entry(1), entry(2)],
            )
        self.assertEqual(client._run.call_count, 2)

    def test_integration_requires_app_author_to_be_a_coordinator(self) -> None:
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                },
                "integration": {"require_non_actions_author": True},
            }
        )
        client = object.__new__(GitHub)
        client.config = config
        client.repository = "example/repo"
        client.owner = "example"
        client.coordinator_logins = {"trusted"}
        client.base_sha = Mock(return_value="b" * 40)
        client._json = Mock(
            side_effect=[
                {},
                {},
                {},
                [],
                {"number": 99, "user": {"login": "deploybot-app[bot]"}},
            ]
        )
        client._run = Mock(return_value="")

        with self.assertRaisesRegex(QueueError, "queue.coordinator_actors"):
            client.create_integration_pull_request(
                batch={"batch_id": "batch"},
                entries=[entry(1), entry(2)],
            )
        self.assertEqual(client._run.call_count, 2)

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
            [
                {
                    "pull_requests": [1, 2],
                    "source_paths": ["shared.py"],
                    "generated_paths": [],
                }
            ],
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

    def test_drain_stops_after_one_batch_merges(self) -> None:
        first_entry = entry(1, "a.py")
        later_entry = entry(2, "b.py")
        first = FreezeResult(
            batch={"batch_id": "first"},
            queue=[first_entry],
            blocked_queue=[],
            next_batch=[later_entry],
            overlap_groups=[],
        )
        client = Mock()
        with (
            patch("agent_merge_queue.cli.freeze_queue", return_value=first) as freeze,
            patch(
                "agent_merge_queue.cli.command_merge", return_value="m" * 40
            ) as merge,
            redirect_stdout(io.StringIO()),
        ):
            result = command_drain(client, json_output=True)

        freeze.assert_called_once()
        merge.assert_called_once()
        self.assertEqual(result["merged"][0]["number"], 1)
        self.assertEqual(result["next_batch"], [2])

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
        waiting.checks = {"CI": "pending"}
        waiting.reasons = ["CI is not complete"]
        intent_comments = {
            value.number: {
                "id": value.number,
                "created_at": "2026-06-20T00:00:00Z",
                "user": {"login": "trusted"},
                "body": intent_body(
                    intent_id=f"intent-{value.number}",
                    state="requested",
                    requested_at="2026-06-20T00:00:00Z",
                    requested_head=value.head_sha,
                ),
            }
            for value in (ready, waiting)
        }
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.intent_numbers.return_value = [1, 2]
        client.active_integration_sources.return_value = set()
        client.comments.side_effect = lambda number: [intent_comments[number]]
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

    def test_promote_does_not_hold_resumed_repair_against_itself(self) -> None:
        ready = entry(1)
        ready.labels = ["deploy-requested", "merge-queue"]
        intent_comment = {
            "id": 1,
            "created_at": "2026-06-20T00:00:00Z",
            "user": {"login": "trusted"},
            "body": intent_body(
                intent_id="intent-1",
                state="requested",
                requested_at="2026-06-20T00:00:00Z",
                requested_head=ready.head_sha,
            ),
        }
        repair_comment = {
            "id": 2,
            "created_at": "2026-06-20T00:01:00Z",
            "user": {"login": "coordinator"},
            "body": repair_body(
                {
                    "created_at": "2026-06-20T00:01:00Z",
                    "head_sha": ready.head_sha,
                    "hold_started_at": "2026-06-20T00:01:00Z",
                    "intent_id": "intent-1",
                    "pull_request": 1,
                    "reason": "pull request conflicts with main",
                }
            ),
        }
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.intent_numbers.return_value = [1]
        client.active_integration_sources.return_value = set()
        client.comments.return_value = [intent_comment, repair_comment]
        client.snapshot.return_value = ready

        captured: list[QueueEntry] = []
        with redirect_stdout(io.StringIO()):
            command_promote(client, captured_entries=captured)

        self.assertFalse(captured[0].repair_overlap_hold)
        self.assertEqual(near_ready_overlap_holds(client, captured), {})

    def test_promote_clears_a_transitional_draft_block(self) -> None:
        ready = entry(1)
        ready.labels = ["deploy-requested", "merge-queue-blocked"]
        intent_comment = {
            "id": 1,
            "created_at": "2026-06-20T00:00:00Z",
            "user": {"login": "trusted"},
            "body": intent_body(
                intent_id="intent-1",
                state="requested",
                requested_at="2026-06-20T00:00:00Z",
                requested_head=ready.head_sha,
            ),
        }
        repair_comment = {
            "id": 2,
            "created_at": "2026-06-20T00:01:00Z",
            "user": {"login": "coordinator"},
            "body": repair_body(
                {
                    "head_sha": ready.head_sha,
                    "reason": (
                        "pull request is draft; CI is not complete; "
                        "Greptile score is missing for the current head"
                    ),
                }
            ),
        }
        client = Mock()
        client.config = CONFIG
        client.repository = "example/repo"
        client.trusted_logins = {"trusted"}
        client.coordinator_logins = {"coordinator"}
        client.intent_numbers.return_value = [1]
        client.active_integration_sources.return_value = set()
        client.comments.return_value = [intent_comment, repair_comment]
        client.snapshot.return_value = ready

        with redirect_stdout(io.StringIO()):
            result = command_promote(client)

        self.assertEqual(result["promoted"], [1])
        client.remove_label.assert_called_with(1, "merge-queue-blocked")
        client.add_label.assert_called_with(1, "merge-queue")

    def test_promote_records_stale_intent_as_thread_repair(self) -> None:
        value = entry(1)
        value.labels = ["deploy-requested"]
        intent_comment = {
            "id": 1,
            "created_at": "2026-06-20T00:00:00Z",
            "user": {"login": "trusted"},
            "body": intent_body(
                intent_id="intent-1",
                state="requested",
                requested_at="2026-06-20T00:00:00Z",
                requested_head="a" * 40,
                provider="codex",
                thread_id="thread-1",
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
        client.snapshot.return_value = value
        client.labels.return_value = {"deploy-requested"}
        client.base_sha.return_value = "b" * 40

        captured: list[QueueEntry] = []
        with redirect_stdout(io.StringIO()):
            result = command_promote(client, captured_entries=captured)

        self.assertEqual(result["promoted"], [])
        self.assertEqual(result["blocked"][0]["number"], 1)
        self.assertIn("older head", result["blocked"][0]["reason"])
        self.assertTrue(captured[0].repair_overlap_hold)
        client.add_label.assert_called_with(1, "merge-queue-blocked")
        self.assertEqual(client.record_thread.call_args.args[0].phase, "blocked")

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

    def test_metrics_skip_deployments_that_finished_before_the_merge(self) -> None:
        merge_sha = "m" * 40
        deployed_sha = "d" * 40
        client = Mock()
        client.config = CONFIG
        client.trusted_logins = {"trusted"}
        client.successful_workflow_runs.return_value = [
            {
                "name": "Deploy",
                "conclusion": "success",
                "head_sha": "o" * 40,
                "created_at": "2026-06-20T00:20:00Z",
                "updated_at": "2026-06-20T00:30:00Z",
            },
            {
                "name": "Deploy",
                "conclusion": "success",
                "head_sha": deployed_sha,
                "created_at": "2026-06-20T01:01:00Z",
                "updated_at": "2026-06-20T01:05:00Z",
            },
        ]
        client.recent_merged_pull_requests.return_value = [
            {
                "number": 42,
                "merged_at": "2026-06-20T01:00:00Z",
                "merge_commit_sha": merge_sha,
            }
        ]
        client.comments_for_pull_requests.return_value = {42: []}
        client.is_ancestor.return_value = True

        result = delivery_metrics(client, limit=25)

        self.assertEqual(result["sample_count"], 1)
        self.assertEqual(result["samples"][0]["live_at"], "2026-06-20T01:05:00Z")
        client.is_ancestor.assert_called_once_with(merge_sha, deployed_sha)
        call_args = client.successful_workflow_runs.call_args
        self.assertEqual(call_args.args, (CONFIG.pipeline.deploy_workflows,))
        self.assertEqual(call_args.kwargs["limit"], 100)
        self.assertEqual(
            call_args.kwargs["since"],
            datetime(2026, 6, 20, 1, tzinfo=timezone.utc),
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
        client.queued_numbers.return_value = [1, 2, 99]
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
            patch(
                "agent_merge_queue.cli.freeze_queue",
                side_effect=[
                    frozen,
                    FreezeResult(
                        new_batch([entry(99)], frozen_at="2026-06-20T00:01:00Z"),
                        [entry(99)],
                        [],
                        [],
                        [],
                    ),
                ],
            ) as freeze,
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
        self.assertEqual(
            freeze.call_args_list,
            [call(client, known_entries=[], held_numbers=set()), call(client, held_numbers={1, 2})],
        )
        drain.assert_called_once()
        self.assertEqual(drain.call_args.kwargs["initial_frozen"].queue[0].number, 99)
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
            ],
        )
        client.create_integration_pull_request.assert_called_once_with(
            batch=source_batch,
            entries=[source],
        )
        self.assertEqual(
            [value["number"] for value in result["drain"]["merged"]],
            [99],
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

        self.assertEqual(events, ["integration", "settle", "drain"])

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

    def test_unpause_compare_and_sets_matching_failed_release(self) -> None:
        sha = "a" * 40
        control_id = "pause-1"
        client = Mock()
        client.pipeline_control.side_effect = [
            {
                "state": "paused",
                "reason": f"ci-failed on {sha}",
                "control_id": control_id,
                "main_sha": sha,
            },
            {
                "state": "running",
                "resumes_control_id": control_id,
            },
        ]
        client.base_sha.return_value = sha

        with redirect_stdout(io.StringIO()):
            command_unpause(
                client,
                main_sha=sha,
                control_id=control_id,
            )

        client.set_pipeline_control.assert_called_once_with(
            "running", None, resumes_control_id=control_id
        )

    def test_unpause_rejects_changed_pause_record(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.pipeline_control.return_value = {
            "state": "paused",
            "reason": f"ci-failed on {sha}",
            "control_id": "newer",
            "main_sha": sha,
        }

        with self.assertRaisesRegex(QueueError, "pause record changed"):
            command_unpause(
                client,
                main_sha=sha,
                control_id="older",
            )

        client.set_pipeline_control.assert_not_called()

    def test_unpause_rejects_advanced_main(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.pipeline_control.return_value = {
            "state": "paused",
            "reason": f"ci-failed on {sha}",
            "control_id": "same",
            "main_sha": sha,
        }
        client.base_sha.return_value = "b" * 40

        with self.assertRaisesRegex(QueueError, "main advanced"):
            command_unpause(
                client,
                main_sha=sha,
                control_id="same",
            )

        client.set_pipeline_control.assert_not_called()

    def test_unpause_rejects_new_pause_won_during_transition(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.pipeline_control.side_effect = [
            {
                "state": "paused",
                "control_id": "pause-1",
                "main_sha": sha,
            },
            {
                "state": "paused",
                "control_id": "pause-2",
                "main_sha": sha,
            },
        ]
        client.base_sha.return_value = sha

        with self.assertRaisesRegex(QueueError, "changed during unpause"):
            command_unpause(client, main_sha=sha, control_id="pause-1")

        client.set_pipeline_control.assert_called_once_with(
            "running", None, resumes_control_id="pause-1"
        )

    def test_unpause_repauses_when_main_advances_during_transition(self) -> None:
        sha = "a" * 40
        newer = "b" * 40
        client = Mock()
        client.pipeline_control.side_effect = [
            {
                "state": "paused",
                "control_id": "pause-1",
                "main_sha": sha,
            },
            {
                "state": "running",
                "resumes_control_id": "pause-1",
            },
            {
                "state": "running",
                "resumes_control_id": "pause-1",
            },
        ]
        client.base_sha.side_effect = [sha, newer]

        with self.assertRaisesRegex(QueueError, "pipeline remains paused"):
            command_unpause(client, main_sha=sha, control_id="pause-1")

        self.assertEqual(
            client.set_pipeline_control.call_args_list,
            [
                call("running", None, resumes_control_id="pause-1"),
                call(
                    "paused",
                    f"main advanced during unpause from {sha} to {newer}",
                    main_sha=newer,
                ),
            ],
        )

    def test_unpause_preserves_newer_pause_when_main_advances(self) -> None:
        sha = "a" * 40
        newer = "b" * 40
        client = Mock()
        client.pipeline_control.side_effect = [
            {
                "state": "paused",
                "control_id": "pause-1",
                "main_sha": sha,
            },
            {
                "state": "running",
                "resumes_control_id": "pause-1",
            },
            {
                "state": "paused",
                "control_id": "pause-2",
                "main_sha": newer,
            },
        ]
        client.base_sha.side_effect = [sha, newer]

        with self.assertRaisesRegex(QueueError, "pipeline remains paused"):
            command_unpause(client, main_sha=sha, control_id="pause-1")

        client.set_pipeline_control.assert_called_once_with(
            "running", None, resumes_control_id="pause-1"
        )

    def test_pipeline_control_ignores_stale_resume_after_new_pause(self) -> None:
        sha = "a" * 40
        client = object.__new__(GitHub)
        client.coordinator_logins = {"coordinator"}
        client.registry_comments = Mock(
            return_value=[
                {
                    "id": 1,
                    "created_at": "2026-06-21T17:17:13Z",
                    "user": {"login": "coordinator"},
                    "body": control_body(
                        state="paused",
                        control_id="pause-1",
                        reason=f"ci-failed on {sha}",
                        main_sha=sha,
                    ),
                },
                {
                    "id": 2,
                    "created_at": "2026-06-21T17:17:14Z",
                    "user": {"login": "coordinator"},
                    "body": control_body(
                        state="paused",
                        control_id="pause-2",
                        reason=f"deploy-failed on {sha}",
                        main_sha=sha,
                    ),
                },
                {
                    "id": 3,
                    "created_at": "2026-06-21T17:17:15Z",
                    "user": {"login": "coordinator"},
                    "body": control_body(
                        state="running",
                        control_id="resume-1",
                        resumes_control_id="pause-1",
                    ),
                },
                {
                    "id": 4,
                    "created_at": "2026-06-21T17:17:16Z",
                    "user": {"login": "coordinator"},
                    "body": (
                        '<!-- deploybot-control:v1 {"recorded_at": '
                        '"2026-06-21T17:17:16Z", "schema": 1, '
                        '"state": "running"} -->\n'
                        "Recorded DeployBot pipeline control."
                    ),
                },
            ]
        )

        control = client.pipeline_control()

        self.assertEqual(control["state"], "paused")
        self.assertEqual(control["control_id"], "pause-2")

    def test_pipeline_control_migrates_legacy_pause_with_comment_identity(self) -> None:
        sha = "a" * 40
        client = object.__new__(GitHub)
        client.coordinator_logins = {"coordinator"}
        client.base_sha = Mock(return_value=sha)
        client.registry_comments = Mock(
            return_value=[
                {
                    "id": 42,
                    "created_at": "2026-06-21T17:17:13Z",
                    "user": {"login": "coordinator"},
                    "body": (
                        '<!-- deploybot-control:v1 {"reason": "ci-failed", '
                        '"recorded_at": "2026-06-21T17:17:13Z", '
                        '"schema": 1, "state": "paused"} -->\n'
                        "Recorded DeployBot pipeline control."
                    ),
                }
            ]
        )

        control = client.pipeline_control()

        self.assertEqual(control["control_id"], "legacy-comment:42")
        self.assertEqual(control["main_sha"], sha)
        self.assertTrue(control["legacy_control"])

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
