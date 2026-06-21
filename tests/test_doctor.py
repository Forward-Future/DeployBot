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


if __name__ == "__main__":
    unittest.main()
