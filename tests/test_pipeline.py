from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from agent_merge_queue.config import parse_config
from agent_merge_queue.pipeline import (
    follow_release,
    notify,
    percentile,
    release_state,
    summarize_metrics,
)


CONFIG = parse_config(
    {"queue": {"required_checks": ["CI"], "trusted_actors": ["trusted"]}}
)


class PipelineTest(unittest.TestCase):
    def test_release_state_requires_exact_main_ci_then_deploy(self) -> None:
        sha = "a" * 40
        runs = [
            {
                "id": 1,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
                "created_at": "2026-06-20T00:00:00Z",
            },
            {
                "id": 2,
                "name": "Deploy",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
                "created_at": "2026-06-20T00:01:00Z",
            },
        ]
        self.assertEqual(
            release_state(main_sha=sha, runs=runs, config=CONFIG.pipeline)["state"],
            "verified",
        )
        self.assertEqual(
            release_state(main_sha="b" * 40, runs=runs, config=CONFIG.pipeline)[
                "state"
            ],
            "testing",
        )

    def test_follow_switches_to_newer_cumulative_main(self) -> None:
        old = "a" * 40
        new = "b" * 40
        client = Mock()
        client.config = CONFIG
        client.base_sha.side_effect = [old, new]
        client.workflow_runs.side_effect = [
            [],
            [
                {
                    "id": 1,
                    "name": "CI",
                    "head_sha": new,
                    "status": "completed",
                    "conclusion": "success",
                },
                {
                    "id": 2,
                    "name": "Deploy",
                    "head_sha": new,
                    "status": "completed",
                    "conclusion": "success",
                },
            ],
        ]
        with (
            patch("agent_merge_queue.pipeline.time.sleep"),
            patch("agent_merge_queue.pipeline.time.monotonic", side_effect=[0, 1, 2]),
        ):
            result = follow_release(client, timeout_seconds=10, poll_seconds=1)
        self.assertEqual(result["state"], "verified")
        self.assertEqual(result["main_sha"], new)

    def test_follow_reports_persistent_http_verification_failure(self) -> None:
        sha = "a" * 40
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                },
                "pipeline": {
                    "verifications": [
                        {
                            "name": "Health",
                            "url": "https://example.invalid/health",
                        }
                    ]
                },
            }
        )
        client = Mock()
        client.config = config
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
        failed = [{"name": "Health", "passed": False, "status": 503}]
        with (
            patch(
                "agent_merge_queue.pipeline.http_verifications",
                return_value=failed,
            ),
            patch("agent_merge_queue.pipeline.time.monotonic", side_effect=[0, 11]),
        ):
            result = follow_release(client, timeout_seconds=10, poll_seconds=1)

        self.assertEqual(result["state"], "verify-failed")
        self.assertEqual(result["verifications"], failed)

    def test_metrics_report_p50_and_p95(self) -> None:
        self.assertEqual(percentile([1, 2, 3, 100], 0.50), 2)
        self.assertEqual(percentile([1, 2, 3, 100], 0.95), 100)
        summary = summarize_metrics(
            [
                {"request_to_queue_seconds": 1},
                {"request_to_queue_seconds": 3},
            ]
        )
        self.assertEqual(summary["request_to_queue_seconds"]["p50"], 1)
        self.assertEqual(summary["request_to_queue_seconds"]["p95"], 3)

    def test_webhook_failure_never_blocks_delivery_state(self) -> None:
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                },
                "pipeline": {"webhook_url_env": "DEPLOYBOT_TEST_WEBHOOK"},
            }
        )
        with (
            patch.dict(
                "os.environ",
                {"DEPLOYBOT_TEST_WEBHOOK": "https://example.invalid/hook"},
            ),
            patch("agent_merge_queue.pipeline.urlopen", side_effect=OSError("offline")),
        ):
            self.assertFalse(notify(config.pipeline, "queued", {"pull_request": 1}))


if __name__ == "__main__":
    unittest.main()
