from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_merge_queue.doctor import diagnose


class DoctorTest(unittest.TestCase):
    def test_missing_github_cli_is_one_clean_failure(self) -> None:
        with patch("agent_merge_queue.doctor.shutil.which", return_value=None):
            rows = diagnose(config_path=None, repository=None)
        self.assertEqual(rows[0]["status"], "fail")
        self.assertIn("not found", rows[0]["detail"])

    def test_bad_config_is_reported_without_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / ".mergequeue.toml").write_text("not = [valid", encoding="utf-8")
            with (
                patch(
                    "agent_merge_queue.doctor.shutil.which", return_value="/usr/bin/gh"
                ),
                patch(
                    "agent_merge_queue.doctor._gh", return_value=(0, "authenticated")
                ),
                patch(
                    "agent_merge_queue.doctor._json",
                    return_value=(0, {"nameWithOwner": "owner/repo"}, ""),
                ),
            ):
                rows = diagnose(config_path=None, repository="owner/repo", cwd=root)
        config = next(value for value in rows if value["check"] == "configuration")
        self.assertEqual(config["status"], "fail")
        self.assertIn("invalid merge queue config", config["detail"])

    def test_auth_output_never_returns_token_or_scope_material(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / ".mergequeue.toml").write_text(
                '[queue]\nrequired_checks = ["CI"]\ntrusted_actors = ["trusted"]\n',
                encoding="utf-8",
            )

            def fake_json(*arguments: str, cwd: Path):
                joined = " ".join(arguments)
                if joined == "api user":
                    return 0, {"login": "safe-user"}, ""
                if "users/trusted" in joined:
                    return 0, {"login": "trusted"}, ""
                if "workflow list" in joined or "label list" in joined:
                    return 0, [], ""
                if "pr list" in joined:
                    return 0, [], ""
                if "protection" in joined:
                    return 1, None, "not available"
                return 0, {"nameWithOwner": "owner/repo"}, ""

            with (
                patch(
                    "agent_merge_queue.doctor.shutil.which", return_value="/usr/bin/gh"
                ),
                patch(
                    "agent_merge_queue.doctor._gh",
                    return_value=(0, "Token: gho_secret; scopes: repo"),
                ),
                patch("agent_merge_queue.doctor._json", side_effect=fake_json),
            ):
                rows = diagnose(config_path=None, repository="owner/repo", cwd=root)

        auth = next(value for value in rows if value["check"] == "authentication")
        self.assertEqual(
            auth["detail"], "GitHub authentication is active for safe-user"
        )
        self.assertNotIn("Token", str(rows))
        self.assertNotIn("scope", str(rows))

    def test_missing_labels_and_check_name_are_advisory(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / ".mergequeue.toml").write_text(
                """
[queue]
required_checks = ["Expected CI"]
trusted_actors = ["trusted"]
""",
                encoding="utf-8",
            )

            def fake_json(*arguments: str, cwd: Path):
                joined = " ".join(arguments)
                if "label list" in joined:
                    return 0, [], ""
                if "workflow list" in joined:
                    return 0, [], ""
                if "pr list" in joined:
                    return 0, [{"statusCheckRollup": [{"name": "Different CI"}]}], ""
                if "users/trusted" in joined:
                    return 0, {"login": "trusted"}, ""
                if "protection" in joined:
                    return 1, None, "not available"
                return 0, {"nameWithOwner": "owner/repo"}, ""

            with (
                patch(
                    "agent_merge_queue.doctor.shutil.which", return_value="/usr/bin/gh"
                ),
                patch(
                    "agent_merge_queue.doctor._gh", return_value=(0, "authenticated")
                ),
                patch("agent_merge_queue.doctor._json", side_effect=fake_json),
            ):
                rows = diagnose(config_path=None, repository="owner/repo", cwd=root)
        labels = next(value for value in rows if value["check"] == "labels")
        checks = next(value for value in rows if value["check"] == "required-checks")
        self.assertEqual(labels["status"], "warn")
        self.assertEqual(checks["status"], "warn")

    def test_disabled_issues_is_a_registry_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / ".mergequeue.toml").write_text(
                '[queue]\nrequired_checks = ["CI"]\ntrusted_actors = ["trusted"]\n',
                encoding="utf-8",
            )

            def fake_json(*arguments: str, cwd: Path):
                joined = " ".join(arguments)
                if "repo view" in joined:
                    return (
                        0,
                        {"nameWithOwner": "owner/repo", "hasIssuesEnabled": False},
                        "",
                    )
                if "users/trusted" in joined:
                    return 0, {"login": "trusted"}, ""
                if "workflow list" in joined or "label list" in joined:
                    return 0, [], ""
                if "pr list" in joined:
                    return 0, [], ""
                if "protection" in joined:
                    return 1, None, "not available"
                return 0, {"login": "owner"}, ""

            with (
                patch(
                    "agent_merge_queue.doctor.shutil.which", return_value="/usr/bin/gh"
                ),
                patch(
                    "agent_merge_queue.doctor._gh", return_value=(0, "authenticated")
                ),
                patch("agent_merge_queue.doctor._json", side_effect=fake_json),
            ):
                rows = diagnose(
                    config_path=None,
                    repository="owner/repo",
                    cwd=root,
                )

        registry = next(value for value in rows if value["check"] == "issue-registry")
        self.assertEqual(registry["status"], "fail")
        self.assertIn("disabled", registry["detail"])

    def test_overlap_mode_requires_actions_pull_request_creation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / ".mergequeue.toml").write_text(
                """
[queue]
required_checks = ["CI"]
trusted_actors = ["trusted"]
coordinator_actors = ["github-actions[bot]"]

[integration]
mode = "overlap"
""",
                encoding="utf-8",
            )

            def fake_json(*arguments: str, cwd: Path):
                joined = " ".join(arguments)
                if "repo view" in joined:
                    return (
                        0,
                        {"nameWithOwner": "owner/repo", "hasIssuesEnabled": True},
                        "",
                    )
                if "actions/permissions/workflow" in joined:
                    return 0, {"can_approve_pull_request_reviews": False}, ""
                if "users/trusted" in joined:
                    return 0, {"login": "trusted"}, ""
                if "workflow list" in joined or "label list" in joined:
                    return 0, [], ""
                if "pr list" in joined:
                    return 0, [], ""
                if "protection" in joined:
                    return 1, None, "not available"
                return 0, {"login": "owner"}, ""

            with (
                patch(
                    "agent_merge_queue.doctor.shutil.which", return_value="/usr/bin/gh"
                ),
                patch(
                    "agent_merge_queue.doctor._gh", return_value=(0, "authenticated")
                ),
                patch("agent_merge_queue.doctor._json", side_effect=fake_json),
            ):
                rows = diagnose(
                    config_path=None,
                    repository="owner/repo",
                    cwd=root,
                )

        permission = next(
            value for value in rows if value["check"] == "actions-integration-prs"
        )
        self.assertEqual(permission["status"], "fail")
        self.assertIn("cannot create", permission["detail"])


if __name__ == "__main__":
    unittest.main()
