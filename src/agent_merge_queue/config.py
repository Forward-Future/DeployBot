"""Configuration for the portable merge queue."""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ConfigError(ValueError):
    """The repository merge-queue policy is missing or invalid."""


@dataclass(frozen=True)
class ReviewProviderConfig:
    kind: str
    name: str
    check_name: str | None = None
    login: str | None = None
    allowed_reviewers: tuple[str, ...] = ()
    minimum_approvals: int = 1
    minimum_score: int | None = None
    score_pattern: str | None = None
    require_formal_review: bool = False
    require_resolved_threads: bool = False


@dataclass(frozen=True)
class QueueConfig:
    base_branch: str
    queue_label: str
    blocked_label: str
    merge_method: str
    required_checks: tuple[str, ...]
    dependency_directive: str
    trusted_actors: tuple[str, ...]
    coordinator_actors: tuple[str, ...]
    generated_paths: frozenset[str]
    generated_version_paths: frozenset[str]
    asset_version_pattern: str
    review_providers: tuple[ReviewProviderConfig, ...]


ALLOWED_REVIEW_PROVIDERS = {"bot", "check", "github-approvals"}
ALLOWED_MERGE_METHODS = {"merge", "squash", "rebase"}
DEFAULT_CONFIG = """\
[queue]
base_branch = "main"
queue_label = "merge-queue"
blocked_label = "merge-queue-blocked"
merge_method = "merge"
required_checks = ["CI"]
dependency_directive = "Merge-queue-depends-on"
# Add each person allowed to say deploy and enqueue a pull request.
trusted_actors = ["@repository-owner"]
# Coordinators may freeze and complete batches, but cannot enqueue pull requests.
coordinator_actors = ["@repository-owner", "github-actions[bot]"]

[files]
generated_paths = []
generated_version_paths = []
asset_version_pattern = '\\?v=[0-9a-f]{12}'

[review]
# Add zero or more providers. Required checks remain mandatory.
# [[review.providers]]
# kind = "github-approvals"
# name = "Human approval"
# allowed_reviewers = ["reviewer-login"]
# minimum_approvals = 1
"""


def _require_string(value: Any, field: str, default: str | None = None) -> str:
    if value is None and default is not None:
        return default
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{field} must be a non-empty string")
    return value.strip()


def _string_tuple(value: Any, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise ConfigError(f"{field} must be an array of non-empty strings")
    return tuple(dict.fromkeys(item.strip() for item in value))


def _provider(value: Any, index: int) -> ReviewProviderConfig:
    field = f"review.providers[{index}]"
    if not isinstance(value, dict):
        raise ConfigError(f"{field} must be a table")
    kind = _require_string(value.get("kind"), f"{field}.kind")
    if kind not in ALLOWED_REVIEW_PROVIDERS:
        allowed = ", ".join(sorted(ALLOWED_REVIEW_PROVIDERS))
        raise ConfigError(f"{field}.kind must be one of: {allowed}")
    name = _require_string(value.get("name"), f"{field}.name", kind)
    check_name = value.get("check_name")
    login = value.get("login")
    if check_name is not None:
        check_name = _require_string(check_name, f"{field}.check_name")
    if login is not None:
        login = _require_string(login, f"{field}.login")
    if kind == "check" and check_name is None:
        raise ConfigError(f"{field}.check_name is required for check providers")
    if kind == "bot" and login is None:
        raise ConfigError(f"{field}.login is required for bot providers")
    allowed_reviewers = _string_tuple(
        value.get("allowed_reviewers"), f"{field}.allowed_reviewers"
    )
    if kind == "github-approvals" and not allowed_reviewers:
        raise ConfigError(
            f"{field}.allowed_reviewers must explicitly name trusted reviewers"
        )

    approvals = value.get("minimum_approvals", 1)
    if not isinstance(approvals, int) or approvals < 1:
        raise ConfigError(f"{field}.minimum_approvals must be a positive integer")
    score = value.get("minimum_score")
    if score is not None and (not isinstance(score, int) or score < 0):
        raise ConfigError(f"{field}.minimum_score must be a non-negative integer")
    score_pattern = value.get("score_pattern")
    if score_pattern is not None:
        score_pattern = _require_string(score_pattern, f"{field}.score_pattern")
        try:
            compiled = re.compile(score_pattern)
        except re.error as error:
            raise ConfigError(f"{field}.score_pattern is invalid: {error}") from error
        if compiled.groups < 1:
            raise ConfigError(f"{field}.score_pattern must capture the numeric score")
    if score is not None and score_pattern is None:
        raise ConfigError(
            f"{field}.score_pattern is required when minimum_score is configured"
        )
    require_formal_review = bool(value.get("require_formal_review", False))
    require_resolved_threads = bool(value.get("require_resolved_threads", False))
    if kind == "bot" and not any(
        (
            check_name,
            score is not None,
            require_formal_review,
        )
    ):
        raise ConfigError(
            f"{field} must require at least one bot check, score, or formal review; "
            "resolved threads alone are not positive review evidence"
        )

    return ReviewProviderConfig(
        kind=kind,
        name=name,
        check_name=check_name,
        login=login,
        allowed_reviewers=allowed_reviewers,
        minimum_approvals=approvals,
        minimum_score=score,
        score_pattern=score_pattern,
        require_formal_review=require_formal_review,
        require_resolved_threads=require_resolved_threads,
    )


def parse_config(payload: dict[str, Any]) -> QueueConfig:
    if not isinstance(payload, dict):
        raise ConfigError("configuration root must be a table")
    queue = payload.get("queue") or {}
    files = payload.get("files") or {}
    review = payload.get("review") or {}
    if not isinstance(queue, dict) or not isinstance(files, dict):
        raise ConfigError("queue and files must be tables")
    if not isinstance(review, dict):
        raise ConfigError("review must be a table")

    merge_method = _require_string(
        queue.get("merge_method"), "queue.merge_method", "merge"
    )
    if merge_method not in ALLOWED_MERGE_METHODS:
        allowed = ", ".join(sorted(ALLOWED_MERGE_METHODS))
        raise ConfigError(f"queue.merge_method must be one of: {allowed}")
    required_checks = _string_tuple(
        queue.get("required_checks"), "queue.required_checks"
    )
    trusted_actors = _string_tuple(
        queue.get("trusted_actors"), "queue.trusted_actors"
    )
    if not trusted_actors or "YOUR_GITHUB_LOGIN" in trusted_actors:
        raise ConfigError(
            "queue.trusted_actors must explicitly name every user or bot whose "
            "queue markers should be trusted"
        )
    if "github-actions[bot]" in {value.lower() for value in trusted_actors}:
        raise ConfigError(
            "queue.trusted_actors cannot include github-actions[bot]; list it under "
            "queue.coordinator_actors so shared workflows cannot authorize a PR"
        )
    coordinator_actors = _string_tuple(
        queue.get("coordinator_actors"), "queue.coordinator_actors"
    ) or trusted_actors
    if not coordinator_actors or "YOUR_GITHUB_LOGIN" in coordinator_actors:
        raise ConfigError(
            "queue.coordinator_actors must explicitly name every user or bot "
            "whose batch markers should be trusted"
        )
    raw_providers = review.get("providers") or []
    if not isinstance(raw_providers, list):
        raise ConfigError("review.providers must be an array of tables")
    providers = tuple(
        _provider(value, index) for index, value in enumerate(raw_providers)
    )
    if not required_checks and not providers:
        raise ConfigError(
            "configure at least one required check or review provider; "
            "an unreviewed merge queue is not allowed"
        )

    version_pattern = _require_string(
        files.get("asset_version_pattern"),
        "files.asset_version_pattern",
        r"\?v=[0-9a-f]{12}",
    )
    try:
        re.compile(version_pattern)
    except re.error as error:
        raise ConfigError(f"files.asset_version_pattern is invalid: {error}") from error

    return QueueConfig(
        base_branch=_require_string(queue.get("base_branch"), "queue.base_branch", "main"),
        queue_label=_require_string(
            queue.get("queue_label"), "queue.queue_label", "merge-queue"
        ),
        blocked_label=_require_string(
            queue.get("blocked_label"),
            "queue.blocked_label",
            "merge-queue-blocked",
        ),
        merge_method=merge_method,
        required_checks=required_checks,
        dependency_directive=_require_string(
            queue.get("dependency_directive"),
            "queue.dependency_directive",
            "Merge-queue-depends-on",
        ),
        trusted_actors=trusted_actors,
        coordinator_actors=coordinator_actors,
        generated_paths=frozenset(
            _string_tuple(files.get("generated_paths"), "files.generated_paths")
        ),
        generated_version_paths=frozenset(
            _string_tuple(
                files.get("generated_version_paths"),
                "files.generated_version_paths",
            )
        ),
        asset_version_pattern=version_pattern,
        review_providers=providers,
    )


def resolve_config_path(path: str | None, cwd: Path) -> Path:
    candidate = path or os.environ.get("MERGE_QUEUE_CONFIG") or ".mergequeue.toml"
    resolved = Path(candidate).expanduser()
    if not resolved.is_absolute():
        resolved = cwd / resolved
    return resolved.resolve()


def load_config(path: str | None = None, *, cwd: Path | None = None) -> QueueConfig:
    root = (cwd or Path.cwd()).resolve()
    config_path = resolve_config_path(path, root)
    try:
        raw = config_path.read_bytes()
    except FileNotFoundError as error:
        raise ConfigError(
            f"merge queue config not found: {config_path}; run `deploybot init`"
        ) from error
    try:
        payload = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise ConfigError(f"invalid merge queue config {config_path}: {error}") from error
    return parse_config(payload)


def initialize_config(
    path: str | None = None, *, cwd: Path | None = None, force: bool = False
) -> Path:
    root = (cwd or Path.cwd()).resolve()
    config_path = resolve_config_path(path, root)
    if config_path.exists() and not force:
        raise ConfigError(f"merge queue config already exists: {config_path}")
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(DEFAULT_CONFIG, encoding="utf-8")
    return config_path
