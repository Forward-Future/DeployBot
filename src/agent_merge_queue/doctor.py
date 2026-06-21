"""Read-only DeployBot installation and policy diagnostics."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

from .config import ConfigError, QueueConfig, load_config


def row(
    check: str, status: str, detail: str, hint: str | None = None
) -> dict[str, Any]:
    return {"check": check, "status": status, "detail": detail, "hint": hint}


def _gh(*arguments: str, cwd: Path) -> tuple[int, str]:
    completed = subprocess.run(
        ["gh", *arguments],
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )
    output = completed.stdout.strip() or completed.stderr.strip()
    return completed.returncode, output


def _json(*arguments: str, cwd: Path) -> tuple[int, Any, str]:
    code, output = _gh(*arguments, cwd=cwd)
    if code:
        return code, None, output
    try:
        return 0, json.loads(output), ""
    except json.JSONDecodeError:
        return 1, None, "GitHub returned invalid JSON"


def diagnose(
    *,
    config_path: str | None,
    repository: str | None,
    cwd: Path | None = None,
) -> list[dict[str, Any]]:
    root = (cwd or Path.cwd()).resolve()
    rows: list[dict[str, Any]] = []
    if shutil.which("gh") is None:
        return [
            row(
                "tooling",
                "fail",
                "GitHub CLI not found",
                "Install gh from https://cli.github.com/.",
            )
        ]
    rows.append(row("tooling", "ok", "GitHub CLI is installed"))

    code, _ = _gh("auth", "status", cwd=root)
    authenticated = code == 0
    auth_detail = "GitHub authentication is not active"
    if authenticated:
        auth_detail = "GitHub authentication is active"
        user_code, user, _ = _json("api", "user", cwd=root)
        login = str((user or {}).get("login") or "") if user_code == 0 else ""
        if login:
            auth_detail += f" for {login}"
    rows.append(
        row(
            "authentication",
            "ok" if authenticated else "fail",
            auth_detail,
            None if authenticated else "Run `gh auth login`.",
        )
    )

    config: QueueConfig | None = None
    try:
        config = load_config(config_path, cwd=root)
        rows.append(row("configuration", "ok", "DeployBot policy is valid"))
    except ConfigError as exc:
        rows.append(row("configuration", "fail", str(exc), "Fix .mergequeue.toml."))

    if not authenticated:
        rows.append(row("repository", "warn", "Skipped: GitHub is not authenticated"))
        return rows

    repo = repository
    if not repo:
        code, value, detail = _json("repo", "view", "--json", "nameWithOwner", cwd=root)
        if code:
            rows.append(
                row(
                    "repository",
                    "fail",
                    detail,
                    "Run inside a GitHub checkout or pass --repository.",
                )
            )
            return rows
        repo = str(value.get("nameWithOwner") or "")
    code, repository, detail = _json(
        "repo",
        "view",
        repo,
        "--json",
        "nameWithOwner,hasIssuesEnabled",
        cwd=root,
    )
    if code:
        rows.append(row("repository", "fail", detail, "Check repository access."))
        return rows
    rows.append(row("repository", "ok", f"Can read {repo}"))
    if config is None:
        return rows

    issues_enabled = (repository or {}).get("hasIssuesEnabled")
    issues_status = "ok" if issues_enabled is True else "fail"
    if issues_enabled is None:
        issues_status = "warn"
    rows.append(
        row(
            "issue-registry",
            issues_status,
            (
                "GitHub Issues is enabled for durable DeployBot metadata"
                if issues_enabled is True
                else (
                    "GitHub Issues is disabled"
                    if issues_enabled is False
                    else "Could not confirm whether GitHub Issues is enabled"
                )
            ),
            (
                None
                if issues_enabled is True
                else "Enable Issues before recording deploy intent or pipeline state."
            ),
        )
    )

    code, workflows, detail = _json(
        "workflow", "list", "--repo", repo, "--json", "name,path,state", cwd=root
    )
    if code:
        rows.append(row("event-worker", "warn", detail))
    else:
        installed = [
            value
            for value in workflows
            if str(value.get("name") or "").lower() == "deploybot"
            and value.get("state") == "active"
        ]
        rows.append(
            row(
                "event-worker",
                "ok" if installed else "warn",
                "DeployBot workflow is active"
                if installed
                else "No active DeployBot workflow was found",
                None
                if installed
                else "Install examples/github-workflow.yml on the default branch.",
            )
        )

    code, labels, detail = _json(
        "label", "list", "--repo", repo, "--limit", "1000", "--json", "name", cwd=root
    )
    if code:
        rows.append(row("labels", "warn", detail, "Check token metadata permissions."))
    else:
        names = {str(item.get("name") or "") for item in labels}
        required = {
            config.queue_label,
            config.blocked_label,
            config.pipeline.intent_label,
            config.pipeline.pause_label,
            config.pipeline.registry_label,
        }
        missing = sorted(required - names)
        rows.append(
            row(
                "labels",
                "warn" if missing else "ok",
                "Missing: " + ", ".join(missing)
                if missing
                else "All DeployBot labels exist",
                "Run `deploybot ensure-labels`." if missing else None,
            )
        )

    owner = repo.split("/", 1)[0]
    actors = set(config.trusted_actors) | set(config.coordinator_actors)
    actors.update(
        reviewer
        for provider in config.review_providers
        for reviewer in provider.allowed_reviewers
    )
    unknown: list[str] = []
    skipped: list[str] = []
    for actor in sorted(actors):
        login = owner if actor == "@repository-owner" else actor
        if login.lower() == "github-actions[bot]":
            skipped.append(login)
            continue
        code, _, _ = _json("api", f"users/{login}", cwd=root)
        if code:
            unknown.append(login)
    rows.append(
        row(
            "actors",
            "fail" if unknown else "ok",
            (
                "Unknown GitHub login(s): " + ", ".join(unknown)
                if unknown
                else "Trusted identities resolve"
            )
            + (f"; built-in bot skipped: {', '.join(skipped)}" if skipped else ""),
            "Correct the actor and reviewer allowlists." if unknown else None,
        )
    )

    code, pulls, detail = _json(
        "pr",
        "list",
        "--repo",
        repo,
        "--state",
        "all",
        "--limit",
        "20",
        "--json",
        "statusCheckRollup",
        cwd=root,
    )
    if code:
        rows.append(row("required-checks", "warn", detail))
    else:
        observed = {
            str(check.get("name") or check.get("context") or "")
            for pull in pulls
            for check in pull.get("statusCheckRollup") or []
        }
        missing = sorted(set(config.required_checks) - observed)
        rows.append(
            row(
                "required-checks",
                "warn" if missing else "ok",
                "Never observed: " + ", ".join(missing)
                if missing
                else "Configured check names were observed",
                "Use the exact GitHub check display names." if missing else None,
            )
        )

    code, protection, detail = _json(
        "api", f"repos/{repo}/branches/{config.base_branch}/protection", cwd=root
    )
    if code:
        rows.append(
            row(
                "branch-protection",
                "warn",
                detail or "Could not read branch protection",
                "Confirm rulesets independently enforce required checks and forbid bypass.",
            )
        )
    else:
        contexts = set(
            (protection.get("required_status_checks") or {}).get("contexts") or []
        )
        missing = sorted(set(config.required_checks) - contexts)
        rows.append(
            row(
                "branch-protection",
                "warn" if missing else "ok",
                "Not independently required: " + ", ".join(missing)
                if missing
                else "Required checks are protected",
                "Update the base-branch ruleset." if missing else None,
            )
        )
    return rows
