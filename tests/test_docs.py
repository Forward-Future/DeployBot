from __future__ import annotations

import argparse
import ast
import re
import unittest
from dataclasses import fields
from pathlib import Path

from agent_merge_queue.cli import build_parser
from agent_merge_queue.config import (
    IntegrationConfig,
    PipelineConfig,
    QueueConfig,
    ReviewProviderConfig,
    VerificationConfig,
)
from agent_merge_queue import __version__


ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "docs" / "reference.md"


class DocumentationTest(unittest.TestCase):
    def test_readme_links_the_complete_reference(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("docs/reference.md", readme)
        self.assertTrue(REFERENCE.is_file())

    def test_reference_names_every_cli_command(self) -> None:
        parser = build_parser()
        subparsers = next(
            action
            for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        )
        reference = REFERENCE.read_text(encoding="utf-8")
        for command in subparsers.choices:
            with self.subTest(command=command):
                self.assertIn(f"deploybot {command}", reference)

        parsers = [parser, *subparsers.choices.values()]
        thread_parser = subparsers.choices["thread"]
        thread_subparsers = next(
            action
            for action in thread_parser._actions
            if isinstance(action, argparse._SubParsersAction)
        )
        parsers.extend(thread_subparsers.choices.values())
        options = {
            option
            for current in parsers
            for action in current._actions
            for option in action.option_strings
            if option not in {"-h", "--help"}
        }
        for option in options:
            with self.subTest(option=option):
                self.assertIn(option, reference)

    def test_reference_names_every_mcp_tool(self) -> None:
        source = (ROOT / "src" / "agent_merge_queue" / "mcp_server.py").read_text(
            encoding="utf-8"
        )
        tree = ast.parse(source)
        tools: dict[str, set[str]] = {}
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in node.decorator_list:
                if (
                    isinstance(decorator, ast.Call)
                    and isinstance(decorator.func, ast.Attribute)
                    and isinstance(decorator.func.value, ast.Name)
                    and decorator.func.value.id == "mcp"
                    and decorator.func.attr == "tool"
                ):
                    tools[node.name] = {
                        argument.arg
                        for argument in node.args.args
                        if argument.arg not in {"repository", "config"}
                    }
        reference = REFERENCE.read_text(encoding="utf-8")
        self.assertTrue(tools)
        for tool, arguments in tools.items():
            with self.subTest(tool=tool):
                self.assertIn(f"`{tool}`", reference)
            for argument in arguments:
                with self.subTest(tool=tool, argument=argument):
                    self.assertIn(f"`{argument}`", reference)

    def test_reference_names_every_policy_field(self) -> None:
        documented = {
            "queue": {
                "base_branch",
                "queue_label",
                "blocked_label",
                "merge_method",
                "required_checks",
                "dependency_directive",
                "trusted_actors",
                "coordinator_actors",
            },
            "files": {
                "generated_paths",
                "generated_version_paths",
                "asset_version_pattern",
            },
            "review": {
                "kind",
                "name",
                "check_name",
                "login",
                "allowed_reviewers",
                "minimum_approvals",
                "minimum_score",
                "score_pattern",
                "require_formal_review",
                "require_resolved_threads",
            },
            "pipeline": {
                "intent_label",
                "pause_label",
                "registry_label",
                "registry_title",
                "thread_active_hours",
                "ci_workflows",
                "deploy_workflows",
                "ready_to_merge_target_minutes",
                "merge_to_live_target_minutes",
                "auto_promote",
                "intent_scope",
                "pause_on_failure",
                "webhook_url_env",
                "verifications",
                "name",
                "url",
                "expected_status",
            },
            "integration": {"mode", "branch_prefix", "title_prefix"},
        }
        queue_fields = {field.name for field in fields(QueueConfig)} - {
            "review_providers",
            "pipeline",
            "integration",
        }
        self.assertEqual(documented["queue"] | documented["files"], queue_fields)
        self.assertEqual(
            documented["review"],
            {field.name for field in fields(ReviewProviderConfig)},
        )
        self.assertEqual(
            documented["pipeline"],
            {field.name for field in fields(PipelineConfig)}
            | {field.name for field in fields(VerificationConfig)},
        )
        self.assertEqual(
            documented["integration"],
            {field.name for field in fields(IntegrationConfig)},
        )
        reference = REFERENCE.read_text(encoding="utf-8")
        for section, section_fields in documented.items():
            for field in section_fields:
                with self.subTest(section=section, field=field):
                    self.assertIn(f"`{field}`", reference)

    def test_reference_names_every_action_input(self) -> None:
        action = (ROOT / "action.yml").read_text(encoding="utf-8")
        inputs = action.split("inputs:\n", 1)[1].split("runs:\n", 1)[0]
        names = re.findall(r"^  ([a-z][a-z0-9_]*):$", inputs, re.MULTILINE)
        reference = REFERENCE.read_text(encoding="utf-8")
        self.assertTrue(names)
        for name in names:
            with self.subTest(name=name):
                self.assertIn(f"`{name}`", reference)

    def test_reference_version_matches_package(self) -> None:
        reference = REFERENCE.read_text(encoding="utf-8")
        self.assertIn(f"DeployBot v{__version__}", reference)


if __name__ == "__main__":
    unittest.main()
