from __future__ import annotations

import unittest
from datetime import datetime, timezone

from agent_merge_queue.records import (
    ThreadRecord,
    intent_body,
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
