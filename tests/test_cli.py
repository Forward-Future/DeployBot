from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

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
    command_unblock,
    completed_batch_ids,
    entries_in_batch,
    generated_only_change,
    latest_batch_marker,
    latest_marker,
    new_batch,
    overlap_groups,
    queue_state_body,
    queue_timestamp,
    reusable_batch,
    structured_dependencies,
)
from agent_merge_queue.config import parse_config
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
    def test_dependency_directive_is_configurable(self) -> None:
        body = "Queue-after: #12, #14\nBlocked by #99"
        self.assertEqual(structured_dependencies(body, "Queue-after"), [12, 14])

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
        value.review_verdicts = (
            ReviewVerdict("Any bot", "blocked", ("one finding",)),
        )

        value.classify(CONFIG)

        self.assertEqual(value.state, "blocked")
        self.assertIn("one finding", value.reasons)

    def test_github_blocked_state_fails_closed_but_mergeable_states_do_not(self) -> None:
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
        self.assertEqual(latest_marker([forged, injected, valid], "trusted")["head_sha"], sha)

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
            latest_batch_marker(
                [batch_comment], "trusted", batch_id=batch["batch_id"]
            ),
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

        selected = active_batch(
            [first, second, late], {1: batch, 2: batch, 3: None}
        )

        self.assertEqual(selected["pull_requests"], [1, 2])
        self.assertEqual(
            [value.number for value in entries_in_batch([first, second, late], selected)],
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
            overlap_groups=[
                {"pull_requests": [2, 3], "source_paths": ["shared.py"]}
            ],
        )
        client = Mock()
        with (
            patch("agent_merge_queue.cli.freeze_queue", return_value=frozen),
            patch("agent_merge_queue.cli.command_merge", return_value="m" * 40) as merge,
            redirect_stdout(io.StringIO()),
        ):
            result = command_drain(client, json_output=True)

        merge.assert_called_once_with(client, "1", "batch", emit=False)
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
            patch(
                "agent_merge_queue.cli.freeze_queue", side_effect=[first, second]
            ),
            patch("agent_merge_queue.cli.command_merge", return_value="m" * 40) as merge,
            redirect_stdout(io.StringIO()),
        ):
            result = command_drain(client, json_output=True)

        merge.assert_called_once_with(client, "2", "second", emit=False)
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
                + json.dumps({
                    "schema": 1,
                    "head_sha": old_head,
                    "queued_at": "2026-06-20T00:00:00Z",
                })
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

    def test_reenabling_blocked_pr_toggles_queue_label_to_wake_coordinator(self) -> None:
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
        client.labels.return_value = {"merge-queue"}

        with redirect_stdout(io.StringIO()):
            command_block(client, "1", "investigate")

        block_body = client.comment.call_args.args[1]
        self.assertIn('"state": "blocked"', block_body)
        client.add_label.assert_called_once_with(1, "merge-queue-blocked")
        names = [call[0] for call in client.method_calls]
        self.assertLess(names.index("add_label"), names.index("comment"))

        client.reset_mock()
        client.config = CONFIG
        client.resolve_pr.return_value = 1
        client.snapshot.return_value = value
        client.comments.return_value = [queued]
        client.trusted_logins = {"trusted"}
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


if __name__ == "__main__":
    unittest.main()
