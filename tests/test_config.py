from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from agent_merge_queue.config import (
    ConfigError,
    initialize_config,
    load_config,
    parse_config,
)


class ConfigTest(unittest.TestCase):
    def test_requires_at_least_one_gate(self) -> None:
        with self.assertRaisesRegex(ConfigError, "at least one"):
            parse_config(
                {
                    "queue": {
                        "required_checks": [],
                        "trusted_actors": ["trusted"],
                    }
                }
            )

    def test_parses_multiple_provider_types(self) -> None:
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                },
                "review": {
                    "providers": [
                        {
                            "kind": "github-approvals",
                            "name": "Humans",
                            "allowed_reviewers": ["reviewer-a", "reviewer-b"],
                            "minimum_approvals": 2,
                        },
                        {
                            "kind": "bot",
                            "name": "Any bot",
                            "login": "review-bot",
                            "check_name": "Review Bot",
                            "minimum_score": 4,
                            "score_pattern": r"Score:\s*(\d)",
                        },
                    ]
                },
            }
        )

        self.assertEqual(config.required_checks, ("CI",))
        self.assertEqual(config.review_providers[0].minimum_approvals, 2)
        self.assertEqual(
            config.review_providers[0].allowed_reviewers,
            ("reviewer-a", "reviewer-b"),
        )
        self.assertEqual(config.review_providers[1].login, "review-bot")

    def test_rejects_bot_without_login(self) -> None:
        with self.assertRaisesRegex(ConfigError, "login is required"):
            parse_config(
                {
                    "queue": {
                        "required_checks": ["CI"],
                        "trusted_actors": ["trusted"],
                    },
                    "review": {"providers": [{"kind": "bot", "name": "Broken"}]},
                }
            )

    def test_approval_provider_requires_explicit_reviewers(self) -> None:
        with self.assertRaisesRegex(ConfigError, "allowed_reviewers"):
            parse_config(
                {
                    "queue": {
                        "required_checks": ["CI"],
                        "trusted_actors": ["trusted"],
                    },
                    "review": {
                        "providers": [{"kind": "github-approvals", "name": "Humans"}]
                    },
                }
            )

    def test_score_gate_requires_an_explicit_parser(self) -> None:
        with self.assertRaisesRegex(ConfigError, "score_pattern is required"):
            parse_config(
                {
                    "queue": {
                        "required_checks": ["CI"],
                        "trusted_actors": ["trusted"],
                    },
                    "review": {
                        "providers": [
                            {
                                "kind": "bot",
                                "name": "Bot",
                                "login": "bot",
                                "minimum_score": 4,
                            }
                        ]
                    },
                }
            )

    def test_init_creates_a_loadable_safe_policy(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = initialize_config(cwd=root)
            config = load_config(cwd=root)

        self.assertEqual(path.name, ".mergequeue.toml")
        self.assertEqual(config.required_checks, ("CI",))
        self.assertEqual(
            config.trusted_actors,
            ("@repository-owner",),
        )
        self.assertEqual(
            config.coordinator_actors,
            ("@repository-owner", "github-actions[bot]"),
        )

    def test_requires_explicit_trusted_actors(self) -> None:
        with self.assertRaisesRegex(ConfigError, "trusted_actors"):
            parse_config({"queue": {"required_checks": ["CI"]}})

    def test_bot_provider_requires_positive_evidence(self) -> None:
        with self.assertRaisesRegex(ConfigError, "at least one bot check"):
            parse_config(
                {
                    "queue": {
                        "required_checks": [],
                        "trusted_actors": ["trusted"],
                    },
                    "review": {
                        "providers": [{"kind": "bot", "name": "Bot", "login": "bot"}]
                    },
                }
            )

        with self.assertRaisesRegex(ConfigError, "resolved threads alone"):
            parse_config(
                {
                    "queue": {
                        "required_checks": [],
                        "trusted_actors": ["trusted"],
                    },
                    "review": {
                        "providers": [
                            {
                                "kind": "bot",
                                "name": "Bot",
                                "login": "bot",
                                "require_resolved_threads": True,
                            }
                        ]
                    },
                }
            )

    def test_shared_actions_identity_cannot_authorize_pull_requests(self) -> None:
        with self.assertRaisesRegex(ConfigError, "coordinator_actors"):
            parse_config(
                {
                    "queue": {
                        "required_checks": ["CI"],
                        "trusted_actors": ["github-actions[bot]"],
                    }
                }
            )

    def test_parses_pipeline_integration_and_health_policy(self) -> None:
        config = parse_config(
            {
                "queue": {
                    "required_checks": ["CI"],
                    "trusted_actors": ["trusted"],
                },
                "pipeline": {
                    "ci_workflows": ["Main CI"],
                    "deploy_workflows": ["Production"],
                    "batch_settle_seconds": 30,
                    "ci_failure_grace_seconds": 45,
                    "promotion_workers": 3,
                    "auto_promote": False,
                    "verifications": [
                        {
                            "name": "Login boundary",
                            "url": "https://example.test/login",
                            "expected_status": 200,
                        }
                    ],
                },
                "integration": {"mode": "all"},
            }
        )
        self.assertEqual(config.pipeline.ci_workflows, ("Main CI",))
        self.assertEqual(config.pipeline.batch_settle_seconds, 30)
        self.assertEqual(config.pipeline.ci_failure_grace_seconds, 45)
        self.assertEqual(config.pipeline.promotion_workers, 3)
        self.assertFalse(config.pipeline.auto_promote)
        self.assertEqual(config.pipeline.verifications[0].expected_status, 200)
        self.assertEqual(config.integration.mode, "all")

    def test_rejects_invalid_integration_mode_and_boolean(self) -> None:
        with self.assertRaisesRegex(ConfigError, "integration.mode"):
            parse_config(
                {
                    "queue": {
                        "required_checks": ["CI"],
                        "trusted_actors": ["trusted"],
                    },
                    "integration": {"mode": "magic"},
                }
            )
        with self.assertRaisesRegex(ConfigError, "auto_promote"):
            parse_config(
                {
                    "queue": {
                        "required_checks": ["CI"],
                        "trusted_actors": ["trusted"],
                    },
                    "pipeline": {"auto_promote": "yes"},
                }
            )
        with self.assertRaisesRegex(ConfigError, "intent_scope"):
            parse_config(
                {
                    "queue": {
                        "required_checks": ["CI"],
                        "trusted_actors": ["trusted"],
                    },
                    "pipeline": {"intent_scope": "pull-request"},
                }
            )
        with self.assertRaisesRegex(ConfigError, "batch_settle_seconds"):
            parse_config(
                {
                    "queue": {
                        "required_checks": ["CI"],
                        "trusted_actors": ["trusted"],
                    },
                    "pipeline": {"batch_settle_seconds": -1},
                }
            )


if __name__ == "__main__":
    unittest.main()
