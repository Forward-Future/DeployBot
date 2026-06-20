from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CANONICAL = ROOT / "skills" / "deploybot" / "SKILL.md"


class DeployBotSkillTest(unittest.TestCase):
    def test_skill_is_packaged_for_codex_and_claude(self) -> None:
        expected = CANONICAL.read_text(encoding="utf-8")
        copies = [
            ROOT
            / "adapters"
            / "codex"
            / "agent-merge-queue"
            / "skills"
            / "deploybot"
            / "SKILL.md",
            ROOT
            / "adapters"
            / "claude-code"
            / "skills"
            / "deploybot"
            / "SKILL.md",
        ]
        for path in copies:
            with self.subTest(path=path):
                self.assertEqual(path.read_text(encoding="utf-8"), expected)

    def test_status_guidance_is_read_only(self) -> None:
        skill = CANONICAL.read_text(encoding="utf-8")
        self.assertIn("deploybot status --json", skill)
        self.assertIn("Never call `freeze_queue` merely to view status", skill)
        self.assertIn("exact `deploy` instruction", skill)

    def test_cursor_adapter_exposes_status_workflow(self) -> None:
        rule = (
            ROOT
            / "adapters"
            / "cursor"
            / ".cursor"
            / "rules"
            / "deploybot.mdc"
        ).read_text(encoding="utf-8")
        self.assertIn("queue_plan", rule)
        self.assertIn("never freeze a queue merely to inspect it", rule)


if __name__ == "__main__":
    unittest.main()
