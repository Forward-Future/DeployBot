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
    {
        "queue": {"required_checks": ["CI"], "trusted_actors": ["trusted"]},
        "pipeline": {"release_admission": "verified"},
    }
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

    def test_release_state_ignores_skipped_deployment_wakeups(self) -> None:
        sha = "a" * 40
        runs = [
            {
                "id": 1,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
                "event": "workflow_dispatch",
                "created_at": "2026-06-20T00:00:00Z",
            },
            {
                "id": 2,
                "name": "Deploy",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "skipped",
                "created_at": "2026-06-20T00:01:00Z",
            },
        ]

        value = release_state(main_sha=sha, runs=runs, config=CONFIG.pipeline)

        self.assertEqual(value["state"], "awaiting-deploy")
        self.assertIsNone(value["latest_deploy"])

    def test_successful_deploy_survives_later_cancelled_duplicate(self) -> None:
        sha = "a" * 40
        runs = [
            {
                "id": 1,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
                "created_at": "2026-06-20T00:00:00Z",
                "updated_at": "2026-06-20T00:00:30Z",
            },
            {
                "id": 2,
                "name": "Deploy",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
                "created_at": "2026-06-20T00:01:00Z",
            },
            {
                "id": 3,
                "name": "Deploy",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "cancelled",
                "created_at": "2026-06-20T00:02:00Z",
            },
        ]

        value = release_state(main_sha=sha, runs=runs, config=CONFIG.pipeline)

        self.assertEqual(value["state"], "verified")
        self.assertEqual(value["latest_deploy"]["id"], 2)

    def test_successful_ci_survives_later_cancelled_duplicate(self) -> None:
        sha = "a" * 40
        runs = [
            {
                "id": 1,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
                "event": "workflow_dispatch",
                "created_at": "2026-06-20T00:00:00Z",
                "updated_at": "2026-06-20T00:01:00Z",
            },
            {
                "id": 2,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "cancelled",
                "event": "push",
                "created_at": "2026-06-20T00:00:01Z",
                "updated_at": "2026-06-20T00:00:02Z",
            },
        ]

        value = release_state(main_sha=sha, runs=runs, config=CONFIG.pipeline)

        self.assertEqual(value["state"], "awaiting-deploy")
        self.assertEqual(value["latest_ci"]["id"], 1)

    def test_later_failed_ci_is_not_hidden_by_older_success(self) -> None:
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
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "failure",
                "created_at": "2026-06-20T00:01:00Z",
            },
        ]

        value = release_state(main_sha=sha, runs=runs, config=CONFIG.pipeline)

        self.assertEqual(value["state"], "ci-failed")
        self.assertEqual(value["latest_ci"]["id"], 2)

    def test_cancelled_duplicate_does_not_hide_intervening_ci_failure(self) -> None:
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
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "failure",
                "created_at": "2026-06-20T00:01:00Z",
            },
            {
                "id": 3,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "cancelled",
                "created_at": "2026-06-20T00:02:00Z",
            },
        ]

        value = release_state(main_sha=sha, runs=runs, config=CONFIG.pipeline)

        self.assertEqual(value["state"], "ci-failed")
        self.assertEqual(value["latest_ci"]["id"], 2)

    def test_later_cancelled_rerun_remains_authoritative(self) -> None:
        sha = "a" * 40
        runs = [
            {
                "id": 1,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
                "event": "workflow_dispatch",
                "created_at": "2026-06-20T00:00:00Z",
                "updated_at": "2026-06-20T00:01:00Z",
            },
            {
                "id": 2,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "cancelled",
                "event": "push",
                "created_at": "2026-06-20T00:02:00Z",
                "updated_at": "2026-06-20T00:03:00Z",
            },
        ]

        value = release_state(main_sha=sha, runs=runs, config=CONFIG.pipeline)

        self.assertEqual(value["state"], "ci-failed")
        self.assertEqual(value["latest_ci"]["id"], 2)

    def test_later_failed_deploy_is_not_hidden_by_older_success(self) -> None:
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
            {
                "id": 3,
                "name": "Deploy",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "failure",
                "created_at": "2026-06-20T00:02:00Z",
            },
        ]

        value = release_state(main_sha=sha, runs=runs, config=CONFIG.pipeline)

        self.assertEqual(value["state"], "deploy-failed")
        self.assertEqual(value["latest_deploy"]["id"], 3)

    def test_new_successful_ci_supersedes_an_older_failed_deploy(self) -> None:
        sha = "a" * 40
        runs = [
            {
                "id": 1,
                "name": "Deploy",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "failure",
                "created_at": "2026-06-20T00:01:00Z",
            },
            {
                "id": 2,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
                "event": "workflow_dispatch",
                "created_at": "2026-06-20T00:02:00Z",
                "updated_at": "2026-06-20T00:03:00Z",
            },
        ]

        value = release_state(main_sha=sha, runs=runs, config=CONFIG.pipeline)

        self.assertEqual(value["state"], "awaiting-deploy")
        self.assertIsNone(value["latest_deploy"])

    def test_release_fence_compares_normalized_timestamps(self) -> None:
        sha = "a" * 40
        runs = [
            {
                "id": 1,
                "name": "CI",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
                "created_at": "2026-06-20T00:00:00Z",
                "updated_at": "2026-06-20T01:00:00+01:00",
            },
            {
                "id": 2,
                "name": "Deploy",
                "head_sha": sha,
                "status": "completed",
                "conclusion": "success",
                "created_at": "2026-06-20T00:30:00.000Z",
            },
        ]

        value = release_state(main_sha=sha, runs=runs, config=CONFIG.pipeline)

        self.assertEqual(value["state"], "verified")
        self.assertEqual(value["latest_deploy"]["id"], 2)

    def test_follow_dispatches_deploy_after_token_dispatched_ci(self) -> None:
        sha = "a" * 40
        ci = {
            "id": 1,
            "name": "CI",
            "head_sha": sha,
            "status": "completed",
            "conclusion": "success",
            "event": "workflow_dispatch",
            "created_at": "2026-06-20T00:00:00Z",
        }
        deploy = {
            "id": 2,
            "name": "Deploy",
            "head_sha": sha,
            "status": "completed",
            "conclusion": "success",
            "event": "workflow_dispatch",
            "created_at": "2026-06-20T00:01:00Z",
        }
        client = Mock()
        client.config = CONFIG
        client.base_sha.return_value = sha
        client.workflow_runs.side_effect = [[ci], [ci, deploy]]
        client.dispatch_deploy_workflows.return_value = [
            {"id": 9, "name": "Deploy", "ci_sha": sha, "ci_run_id": 1}
        ]
        with (
            patch("agent_merge_queue.pipeline.time.sleep"),
            patch("agent_merge_queue.pipeline.time.monotonic", side_effect=[0, 1]),
        ):
            result = follow_release(client, timeout_seconds=10, poll_seconds=1)

        client.dispatch_deploy_workflows.assert_called_once()
        dispatched_ci = client.dispatch_deploy_workflows.call_args.kwargs["ci_run"]
        self.assertEqual(dispatched_ci["id"], 1)
        self.assertEqual(dispatched_ci["head_sha"], sha)
        self.assertEqual(result["state"], "verified")
        self.assertEqual(result["dispatched_deployments"][0]["id"], 9)

    def test_follow_admits_at_ci_passed_without_waiting_for_deploy(self) -> None:
        sha = "a" * 40
        ci = {
            "id": 1,
            "name": "CI",
            "head_sha": sha,
            "status": "completed",
            "conclusion": "success",
            "event": "workflow_dispatch",
            "created_at": "2026-06-20T00:00:00Z",
        }
        client = Mock()
        client.config = CONFIG
        client.base_sha.return_value = sha
        client.workflow_runs.return_value = [ci]
        client.dispatch_deploy_workflows.return_value = [
            {"id": 9, "name": "Deploy", "ci_sha": sha, "ci_run_id": 1}
        ]
        with (
            patch("agent_merge_queue.pipeline.time.sleep") as sleep,
            patch("agent_merge_queue.pipeline.time.monotonic", return_value=0),
        ):
            result = follow_release(
                client,
                timeout_seconds=10,
                poll_seconds=1,
                admit_gate="ci-passed",
            )

        # CI is green, so admission returns immediately even though the deploy is
        # only just dispatched and never verified within this call.
        client.dispatch_deploy_workflows.assert_called_once()
        self.assertEqual(result["state"], "awaiting-deploy")
        self.assertEqual(result["dispatched_deployments"][0]["id"], 9)
        sleep.assert_not_called()

    def test_follow_admits_at_merged_while_ci_is_still_running(self) -> None:
        sha = "a" * 40
        client = Mock()
        client.config = CONFIG
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
            patch("agent_merge_queue.pipeline.time.sleep") as sleep,
            patch("agent_merge_queue.pipeline.time.monotonic", return_value=0),
        ):
            result = follow_release(
                client,
                timeout_seconds=10,
                poll_seconds=1,
                admit_gate="merged",
            )

        self.assertEqual(result["state"], "testing")
        sleep.assert_not_called()

    def test_merged_mode_dispatches_only_newest_main_deployment(self) -> None:
        old = "a" * 40
        newest = "b" * 40
        old_ci = {
            "id": 1,
            "name": "CI",
            "head_sha": old,
            "status": "completed",
            "conclusion": "success",
            "event": "workflow_dispatch",
        }
        newest_ci = {
            "id": 2,
            "name": "CI",
            "head_sha": newest,
            "status": "completed",
            "conclusion": "success",
            "event": "workflow_dispatch",
        }
        client = Mock()
        client.config = CONFIG
        client.base_sha.return_value = newest
        client.workflow_runs.return_value = [old_ci, newest_ci]
        client.dispatch_deploy_workflows.return_value = [
            {"id": 9, "name": "Deploy", "ci_sha": newest, "ci_run_id": 2}
        ]

        result = follow_release(
            client,
            timeout_seconds=10,
            poll_seconds=1,
            admit_gate="merged",
        )

        self.assertEqual(result["main_sha"], newest)
        client.dispatch_deploy_workflows.assert_called_once()
        dispatched_ci = client.dispatch_deploy_workflows.call_args.kwargs["ci_run"]
        self.assertEqual(dispatched_ci["id"], newest_ci["id"])
        self.assertEqual(dispatched_ci["head_sha"], newest)

    def test_merged_mode_retries_health_before_reporting_failure(self) -> None:
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
                "agent_merge_queue.pipeline.http_verifications",
                side_effect=[
                    [{"name": "Login", "passed": False}],
                    [{"name": "Login", "passed": True}],
                ],
            ),
            patch("agent_merge_queue.pipeline.time.sleep") as sleep,
            patch(
                "agent_merge_queue.pipeline.time.monotonic",
                side_effect=[0, 1, 2],
            ),
        ):
            result = follow_release(
                client,
                timeout_seconds=10,
                poll_seconds=1,
                admit_gate="merged",
            )

        self.assertEqual(result["state"], "verified")
        sleep.assert_called_once_with(1)

    def test_follow_absorbs_a_ci_rerun_during_failure_grace(self) -> None:
        sha = "a" * 40
        failed = {
            "id": 1,
            "name": "CI",
            "head_sha": sha,
            "status": "completed",
            "conclusion": "failure",
            "event": "workflow_dispatch",
        }
        retrying = {**failed, "status": "in_progress", "conclusion": None}
        passed = {**failed, "conclusion": "success"}
        deploy = {
            "id": 2,
            "name": "Deploy",
            "head_sha": sha,
            "status": "completed",
            "conclusion": "success",
        }
        client = Mock()
        client.config = CONFIG
        client.base_sha.return_value = sha
        client.workflow_runs.side_effect = [
            [failed],
            [retrying],
            [passed, deploy],
        ]

        with (
            patch("agent_merge_queue.pipeline.time.sleep"),
            patch(
                "agent_merge_queue.pipeline.time.monotonic",
                side_effect=[0, 1, 2],
            ),
        ):
            result = follow_release(client, timeout_seconds=10, poll_seconds=1)

        self.assertEqual(result["state"], "verified")

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
