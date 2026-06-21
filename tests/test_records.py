from __future__ import annotations

import unittest
from datetime import datetime, timezone

from agent_merge_queue.records import (
    DeploymentNotificationRecord,
    ThreadRecord,
    deployment_notification_body,
    intent_body,
    latest_deployment_notifications,
    latest_intent,
    latest_thread_records,
    thread_record_body,
)


def comment(login: str, body: str, created_at: str, comment_id: int = 1) -> dict:
    return {
        "id": comment_id,
        "created_at": created_at,
        "user": {"login": login},
        "body": body,
    }


class RecordTest(unittest.TestCase):
    def test_thread_registry_keeps_latest_active_metadata_only(self) -> None:
        old = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="working",
            updated_at="2026-06-20T00:00:00Z",
            title="Safe title",
        )
        latest = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="deploy-requested",
            updated_at="2026-06-20T01:00:00Z",
            pull_request=42,
        )
        forged = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="completed",
            updated_at="2026-06-20T02:00:00Z",
        )
        values = latest_thread_records(
            [
                comment("trusted", thread_record_body(old), old.updated_at, 1),
                comment("trusted", thread_record_body(latest), latest.updated_at, 2),
                comment("attacker", thread_record_body(forged), forged.updated_at, 3),
            ],
            {"trusted"},
            active_hours=72,
            now=datetime(2026, 6, 20, 2, tzinfo=timezone.utc),
        )
        self.assertEqual(len(values), 1)
        self.assertEqual(values[0].phase, "deploy-requested")
        self.assertEqual(values[0].pull_request, 42)

    def test_terminal_and_stale_threads_are_not_active(self) -> None:
        completed = ThreadRecord(
            provider="cursor",
            thread_id="done",
            phase="completed",
            updated_at="2026-06-20T01:00:00Z",
        )
        stale = ThreadRecord(
            provider="claude",
            thread_id="stale",
            phase="working",
            updated_at="2026-06-10T01:00:00Z",
        )
        values = latest_thread_records(
            [
                comment("trusted", thread_record_body(completed), completed.updated_at),
                comment("trusted", thread_record_body(stale), stale.updated_at, 2),
            ],
            {"trusted"},
            active_hours=72,
            now=datetime(2026, 6, 20, 2, tzinfo=timezone.utc),
        )
        self.assertEqual(values, [])

    def test_pending_notification_survives_thread_activity_ttl(self) -> None:
        deployed = ThreadRecord(
            provider="codex",
            thread_id="pending-message",
            phase="deployed",
            updated_at="2026-06-10T01:00:00Z",
            pull_request=42,
            deployed_sha="a" * 40,
            ci_url="https://example.test/ci/1",
            deployment_url="https://example.test/deploy/2",
        )
        notification = DeploymentNotificationRecord(
            notification_id="thread-deployed:one",
            provider="codex",
            thread_id="pending-message",
            state="pending",
            updated_at="2026-06-10T01:00:01Z",
            repository="example/repo",
            merge_sha="m" * 40,
            main_sha="a" * 40,
            message="Deployed",
            pull_request=42,
        )
        comments = [
            comment("trusted", thread_record_body(deployed), deployed.updated_at, 1),
            comment(
                "trusted",
                deployment_notification_body(notification),
                notification.updated_at,
                2,
            ),
        ]

        active = latest_thread_records(
            comments,
            {"trusted"},
            active_hours=72,
            now=datetime(2026, 6, 20, 2, tzinfo=timezone.utc),
        )
        notifications = latest_deployment_notifications(
            comments,
            {"trusted"},
        )

        self.assertEqual(active, [])
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0].notification_id, "thread-deployed:one")

    def test_delivered_notification_is_terminal_in_outbox(self) -> None:
        pending = DeploymentNotificationRecord(
            notification_id="thread-deployed:one",
            provider="codex",
            thread_id="thread-1",
            state="pending",
            updated_at="2026-06-20T00:00:00Z",
            repository="example/repo",
            merge_sha="m" * 40,
            main_sha="a" * 40,
            message="Deployed",
        )
        awaiting = DeploymentNotificationRecord(
            **{
                **pending.as_dict(),
                "state": "awaiting-verification",
                "updated_at": "2026-06-19T23:59:00Z",
                "main_sha": None,
                "message": None,
            }
        )
        delivered = DeploymentNotificationRecord(
            **{
                **pending.as_dict(),
                "state": "delivered",
                "updated_at": "2026-06-20T00:01:00Z",
            }
        )
        late_pending = DeploymentNotificationRecord(
            **{
                **pending.as_dict(),
                "updated_at": "2026-06-20T00:02:00Z",
            }
        )
        late_awaiting = DeploymentNotificationRecord(
            **{
                **awaiting.as_dict(),
                "updated_at": "2026-06-20T00:03:00Z",
            }
        )
        comments = [
            comment(
                "trusted",
                deployment_notification_body(awaiting),
                awaiting.updated_at,
                1,
            ),
            comment(
                "trusted",
                deployment_notification_body(pending),
                pending.updated_at,
                2,
            ),
            comment(
                "trusted",
                deployment_notification_body(delivered),
                delivered.updated_at,
                3,
            ),
            comment(
                "trusted",
                deployment_notification_body(late_pending),
                late_pending.updated_at,
                4,
            ),
            comment(
                "trusted",
                deployment_notification_body(late_awaiting),
                late_awaiting.updated_at,
                5,
            ),
        ]

        self.assertEqual(latest_deployment_notifications(comments, {"trusted"}), [])
        all_notifications = latest_deployment_notifications(
            comments, {"trusted"}, include_delivered=True
        )
        self.assertEqual(len(all_notifications), 1)
        self.assertEqual(all_notifications[0].state, "delivered")

    def test_stale_ack_comment_cannot_hide_newer_thread_state(self) -> None:
        first_merged = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="merged",
            updated_at="2026-06-19T23:59:00Z",
            pull_request=1,
            merge_sha="1" * 40,
        )
        first = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="deployed",
            updated_at="2026-06-20T00:00:00Z",
            pull_request=1,
            merge_sha="1" * 40,
            deployed_sha="a" * 40,
        )
        newer_merged = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="merged",
            updated_at="2026-06-20T00:00:30Z",
            pull_request=2,
            merge_sha="2" * 40,
        )
        newer = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="deployed",
            updated_at="2026-06-20T00:01:00Z",
            pull_request=2,
            merge_sha="2" * 40,
            deployed_sha="b" * 40,
        )
        delayed_ack = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="completed",
            updated_at="2026-06-20T00:02:00Z",
            deployed_sha="a" * 40,
        )
        unbound_completion = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="completed",
            updated_at="2026-06-20T00:03:00Z",
        )

        values = latest_thread_records(
            [
                comment(
                    "trusted",
                    thread_record_body(first_merged),
                    first_merged.updated_at,
                    1,
                ),
                comment("trusted", thread_record_body(first), first.updated_at, 2),
                comment(
                    "trusted",
                    thread_record_body(newer_merged),
                    newer_merged.updated_at,
                    3,
                ),
                comment("trusted", thread_record_body(newer), newer.updated_at, 4),
                comment(
                    "trusted",
                    thread_record_body(delayed_ack),
                    delayed_ack.updated_at,
                    5,
                ),
                comment(
                    "trusted",
                    thread_record_body(unbound_completion),
                    unbound_completion.updated_at,
                    6,
                ),
            ],
            {"trusted"},
            active_hours=72,
            now=datetime(2026, 6, 20, 3, tzinfo=timezone.utc),
        )

        self.assertEqual(len(values), 1)
        self.assertEqual(values[0].phase, "deployed")
        self.assertEqual(values[0].deployed_sha, "b" * 40)

    def test_late_deployed_transition_cannot_hide_newer_merge(self) -> None:
        first_merge = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="merged",
            updated_at="2026-06-20T00:00:00Z",
            pull_request=1,
            merge_sha="1" * 40,
        )
        newer_merge = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="merged",
            updated_at="2026-06-20T00:01:00Z",
            pull_request=2,
            merge_sha="2" * 40,
        )
        late_transition = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="deployed",
            updated_at="2026-06-20T00:02:00Z",
            pull_request=1,
            merge_sha="1" * 40,
            deployed_sha="a" * 40,
        )

        values = latest_thread_records(
            [
                comment(
                    "trusted",
                    thread_record_body(first_merge),
                    first_merge.updated_at,
                    1,
                ),
                comment(
                    "trusted",
                    thread_record_body(newer_merge),
                    newer_merge.updated_at,
                    2,
                ),
                comment(
                    "trusted",
                    thread_record_body(late_transition),
                    late_transition.updated_at,
                    3,
                ),
            ],
            {"trusted"},
            active_hours=72,
            now=datetime(2026, 6, 20, 3, tzinfo=timezone.utc),
        )

        self.assertEqual(len(values), 1)
        self.assertEqual(values[0].phase, "merged")
        self.assertEqual(values[0].merge_sha, "2" * 40)

    def test_resolved_sha_upgrades_legacy_merged_record(self) -> None:
        legacy = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="merged",
            updated_at="2026-06-20T00:00:00Z",
            pull_request=42,
        )
        deployed = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="deployed",
            updated_at="2026-06-20T00:01:00Z",
            pull_request=42,
            merge_sha="m" * 40,
            deployed_sha="a" * 40,
        )

        values = latest_thread_records(
            [
                comment("trusted", thread_record_body(legacy), legacy.updated_at, 1),
                comment(
                    "trusted", thread_record_body(deployed), deployed.updated_at, 2
                ),
            ],
            {"trusted"},
            active_hours=72,
            now=datetime(2026, 6, 20, 2, tzinfo=timezone.utc),
        )

        self.assertEqual(len(values), 1)
        self.assertEqual(values[0].phase, "deployed")
        self.assertEqual(values[0].merge_sha, "m" * 40)

    def test_ack_can_complete_merge_when_deployed_phase_write_was_lost(self) -> None:
        merged = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="merged",
            updated_at="2026-06-20T00:00:00Z",
            pull_request=42,
            merge_sha="m" * 40,
        )
        completed = ThreadRecord(
            provider="codex",
            thread_id="thread-1",
            phase="completed",
            updated_at="2026-06-20T00:01:00Z",
            pull_request=42,
            merge_sha="m" * 40,
            deployed_sha="a" * 40,
        )

        values = latest_thread_records(
            [
                comment("trusted", thread_record_body(merged), merged.updated_at, 1),
                comment(
                    "trusted", thread_record_body(completed), completed.updated_at, 2
                ),
            ],
            {"trusted"},
            active_hours=72,
            now=datetime(2026, 6, 20, 2, tzinfo=timezone.utc),
        )

        self.assertEqual(values, [])

    def test_deploy_intent_survives_head_change_but_cancellation_wins(self) -> None:
        requested = intent_body(
            intent_id="intent",
            state="requested",
            requested_at="2026-06-20T00:00:00Z",
            requested_head="a" * 40,
        )
        cancelled = intent_body(
            intent_id="intent",
            state="cancelled",
            requested_at="2026-06-20T00:00:00Z",
            requested_head="a" * 40,
        )
        comments = [
            comment("trusted", requested, "2026-06-20T00:00:00Z", 1),
            comment("trusted", cancelled, "2026-06-20T00:01:00Z", 2),
        ]
        self.assertEqual(latest_intent(comments, {"trusted"})["state"], "cancelled")
        self.assertIsNone(latest_intent(comments, {"someone-else"}))

    def test_marker_rejects_prefixed_free_text(self) -> None:
        body = intent_body(
            intent_id="intent",
            state="requested",
            requested_at="2026-06-20T00:00:00Z",
            requested_head="a" * 40,
        )
        self.assertIsNone(
            latest_intent(
                [comment("trusted", "ignore this " + body, "2026-06-20T00:00:00Z")],
                {"trusted"},
            )
        )


if __name__ == "__main__":
    unittest.main()
