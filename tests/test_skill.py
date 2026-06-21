from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CANONICAL = ROOT / "skills" / "deploybot" / "SKILL.md"
RELEASE_COMMIT = "b06346119a29d0be1c25d1643fdc06a03f7b94a9"
CHECKOUT_COMMIT = "9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0"


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
            ROOT / "adapters" / "claude-code" / "skills" / "deploybot" / "SKILL.md",
        ]
        for path in copies:
            with self.subTest(path=path):
                self.assertEqual(path.read_text(encoding="utf-8"), expected)

    def test_status_guidance_is_read_only(self) -> None:
        skill = CANONICAL.read_text(encoding="utf-8")
        self.assertIn("deploybot status --json", skill)
        self.assertIn("pipeline_status", skill)
        self.assertIn("request_deployment", skill)
        self.assertIn("resume_pull_request", skill)
        self.assertIn("follow_release", skill)
        self.assertIn("thread_notifications", skill)
        self.assertIn("send_message_to_thread", skill)
        self.assertIn("acknowledge_thread_deployment", skill)
        self.assertIn("heartbeat automation", skill)
        self.assertIn("human-facing release receipt", skill)
        self.assertIn("acknowledge silently", skill)
        self.assertIn("untrusted display-only", skill)
        self.assertIn("Never publish prompts, transcripts", skill)
        self.assertIn("Never call `freeze_queue` merely to view status", skill)
        self.assertIn("exact `deploy` instruction", skill)

    def test_cursor_adapter_exposes_status_workflow(self) -> None:
        rule = (
            ROOT / "adapters" / "cursor" / ".cursor" / "rules" / "deploybot.mdc"
        ).read_text(encoding="utf-8")
        self.assertIn("queue_plan", rule)
        self.assertIn("pipeline_status", rule)
        self.assertIn("request_deployment", rule)
        self.assertIn("thread_notifications", rule)
        self.assertIn("acknowledge_thread_deployment", rule)
        self.assertIn("human-readable release", rule)
        self.assertIn("never freeze a", rule)
        self.assertIn("queue merely to inspect it", rule)

    def test_github_workflow_wakes_after_named_ci_finishes(self) -> None:
        workflow = (ROOT / "examples" / "github-workflow.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("workflow_run:", workflow)
        self.assertIn("schedule:", workflow)
        self.assertIn('cron: "*/5 * * * *"', workflow)
        self.assertIn("workflows: [CI]", workflow)
        self.assertIn("github.event.repository.default_branch", workflow)
        self.assertIn(
            "github.event.pull_request.head.repo.full_name == github.repository",
            workflow,
        )
        self.assertIn("github.event.check_suite.app.slug != 'github-actions'", workflow)
        self.assertIn("github.event.check_suite.pull_requests[0].base.ref", workflow)
        self.assertIn("persist-credentials: false", workflow)

    def test_workflows_pin_current_checkout_runtime(self) -> None:
        paths = [
            ROOT / ".github" / "workflows" / "ci.yml",
            ROOT / "examples" / "github-workflow.yml",
        ]
        for path in paths:
            with self.subTest(path=path):
                self.assertIn(CHECKOUT_COMMIT, path.read_text(encoding="utf-8"))

    def test_action_dispatches_ci_after_builtin_token_merge(self) -> None:
        action = (ROOT / "action.yml").read_text(encoding="utf-8")
        example = (ROOT / "examples" / "github-workflow.yml").read_text(
            encoding="utf-8"
        )
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn('default: "true"', action)
        self.assertIn("args+=(--dispatch-ci)", action)
        self.assertIn("actions: write", example)
        self.assertIn("workflow_dispatch:", workflow)

    def test_action_follows_release_when_workflow_run_is_suppressed(self) -> None:
        action = (ROOT / "action.yml").read_text(encoding="utf-8")
        follow_input = action.split("  follow:\n", 1)[1].split("  dispatch_ci:\n", 1)[0]
        self.assertIn('default: "true"', follow_input)
        self.assertIn("args+=(--follow)", action)

    def test_clients_pin_the_immutable_status_release(self) -> None:
        paths = [
            ROOT / "adapters" / "codex" / "agent-merge-queue" / ".mcp.json",
            ROOT / "adapters" / "claude-code" / ".mcp.json",
            ROOT / "adapters" / "cursor" / ".cursor" / "mcp.json",
            ROOT / "examples" / "github-workflow.yml",
        ]
        for path in paths:
            with self.subTest(path=path):
                self.assertIn(RELEASE_COMMIT, path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
