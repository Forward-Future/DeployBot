#!/usr/bin/env python3
"""Manage a GitHub-backed, agent-owned pull-request merge queue."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from . import __version__
from .config import ConfigError, QueueConfig, initialize_config, load_config
from .doctor import diagnose
from .pipeline import (
    follow_release,
    notify,
    release_state,
    seconds_between,
    summarize_metrics,
)
from .records import (
    CONTROL_MARKER,
    INTEGRATION_MARKER,
    REPAIR_MARKER,
    THREAD_PHASES,
    ThreadRecord,
    control_body,
    integration_body,
    intent_body,
    latest_intent,
    latest_payload,
    latest_thread_records,
    parse_time,
    repair_body,
    thread_record_body,
)
from .reviews import ReviewVerdict, evaluate_reviews

MARKER_PREFIX = "agent-merge-queue:v1"
STATE_MARKER_PREFIX = "agent-merge-queue-state:v1"
BATCH_MARKER_PREFIX = "agent-merge-batch:v1"
BATCH_COMPLETE_PREFIX = "agent-merge-batch-complete:v1"
LEGACY_MARKER_PREFIX = "astrohub-merge-queue:v1"
LEGACY_BATCH_MARKER_PREFIX = "astrohub-merge-batch:v1"
MARKER = re.compile(
    rf"\A<!--\s*(?:{re.escape(MARKER_PREFIX)}|{re.escape(LEGACY_MARKER_PREFIX)})"
    rf"\s+(\{{.*\}})\s*-->\n"
    r"Queued for the agent-managed merge queue on `([0-9a-f]{40})`\.\s*\Z",
    re.DOTALL,
)
STATE_MARKER = re.compile(
    rf"\A<!--\s*{re.escape(STATE_MARKER_PREFIX)}\s+(\{{.*\}})\s*-->\n"
    r"Recorded merge queue state `([^`]+)` on `([0-9a-f]{40})`\.\s*\Z",
    re.DOTALL,
)
BATCH_MARKER = re.compile(
    rf"\A<!--\s*(?:{re.escape(BATCH_MARKER_PREFIX)}|"
    rf"{re.escape(LEGACY_BATCH_MARKER_PREFIX)})\s+(\{{.*\}})\s*-->\n"
    r"Frozen merge batch `([^`]+)`\.\s*\Z",
    re.DOTALL,
)
BATCH_COMPLETE_MARKER = re.compile(
    rf"\A<!--\s*{re.escape(BATCH_COMPLETE_PREFIX)}\s+(\{{.*\}})\s*-->\n"
    r"Completed merge batch `([^`]+)`\.\s*\Z",
    re.DOTALL,
)
PULL_REQUEST_NUMBER = re.compile(r"#(\d+)\b")
FAILED_CHECK_STATES = {
    "ACTION_REQUIRED",
    "CANCELLED",
    "ERROR",
    "FAILURE",
    "STALE",
    "TIMED_OUT",
}
MERGEABILITY_RETRIES = 6
REVIEW_THREADS_QUERY = """
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviewThreads(first: 100) {
        pageInfo { hasNextPage }
        nodes {
          id
          isResolved
          isOutdated
          comments(first: 100) {
            pageInfo { hasNextPage }
            nodes {
              author { login }
              commit { oid }
            }
          }
        }
      }
    }
  }
}
""".strip()


class QueueError(RuntimeError):
    """A queue operation could not be completed safely."""


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def normalize_check_state(check: dict[str, Any]) -> str:
    for key in ("conclusion", "state", "status"):
        value = str(check.get(key) or "").upper()
        if value:
            return value
    return "UNKNOWN"


def check_states(checks: Iterable[dict[str, Any]]) -> dict[str, str]:
    grouped: dict[str, tuple[str, int, str]] = {}
    for index, check in enumerate(checks):
        name = str(check.get("name") or check.get("context") or "")
        if not name:
            continue
        timestamp = str(
            check.get("startedAt")
            or check.get("createdAt")
            or check.get("completedAt")
            or ""
        )
        if timestamp.startswith("0001-01-01"):
            timestamp = ""
        state = normalize_check_state(check)
        # A newly queued run may not have a timestamp yet. Fail closed instead
        # of letting an older success hide that pending rerun.
        order = "\uffff" if not timestamp and state != "SUCCESS" else timestamp
        candidate = (order, index, state)
        if name not in grouped or candidate[:2] > grouped[name][:2]:
            grouped[name] = candidate

    result: dict[str, str] = {}
    for name, value in grouped.items():
        state = value[2]
        if state in FAILED_CHECK_STATES:
            result[name] = "failed"
        elif state == "SUCCESS":
            result[name] = "passed"
        else:
            result[name] = "pending"
    return result


def trusted_comment(
    comment: dict[str, Any],
    trusted_logins: str | Iterable[str],
) -> bool:
    user = comment.get("user") or {}
    values = (
        {trusted_logins.lower()}
        if isinstance(trusted_logins, str)
        else {value.lower() for value in trusted_logins}
    )
    return str(user.get("login") or "").lower() in values


def latest_marker(
    comments: Iterable[dict[str, Any]],
    trusted_logins: str | Iterable[str],
) -> dict[str, Any] | None:
    found: list[tuple[str, int, int, dict[str, Any]]] = []
    for index, comment in enumerate(comments):
        if not trusted_comment(comment, trusted_logins):
            continue
        body = str(comment.get("body") or "")
        state_match = STATE_MARKER.search(body)
        match = state_match or MARKER.search(body)
        if not match:
            continue
        try:
            value = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if (
            not isinstance(value, dict)
            or value.get("schema") != 1
            or value.get("head_sha") != match.group(3 if state_match else 2)
        ):
            continue
        if state_match:
            state = str(value.get("state") or "")
            if state not in {"queued", "blocked", "dequeued"}:
                continue
            if state != state_match.group(2):
                continue
        else:
            value = dict(value)
            value["state"] = "queued"
        try:
            comment_id = int(comment.get("id") or 0)
        except (TypeError, ValueError):
            comment_id = 0
        found.append((str(comment.get("created_at") or ""), comment_id, index, value))
    return max(found, key=lambda item: item[:3])[3] if found else None


def latest_batch_marker(
    comments: Iterable[dict[str, Any]],
    trusted_logins: str | Iterable[str],
    *,
    batch_id: str | None = None,
) -> dict[str, Any] | None:
    found: list[tuple[str, dict[str, Any]]] = []
    for comment in comments:
        if not trusted_comment(comment, trusted_logins):
            continue
        match = BATCH_MARKER.search(str(comment.get("body") or ""))
        if not match:
            continue
        try:
            value = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if (
            not isinstance(value, dict)
            or value.get("schema") != 1
            or value.get("batch_id") != match.group(2)
        ):
            continue
        if batch_id is not None and value.get("batch_id") != batch_id:
            continue
        found.append((str(comment.get("created_at") or ""), value))
    return max(found, key=lambda item: item[0])[1] if found else None


def completed_batch_ids(
    comments: Iterable[dict[str, Any]],
    trusted_logins: str | Iterable[str],
) -> set[str]:
    found: set[str] = set()
    for comment in comments:
        if not trusted_comment(comment, trusted_logins):
            continue
        match = BATCH_COMPLETE_MARKER.search(str(comment.get("body") or ""))
        if not match:
            continue
        try:
            value = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if (
            isinstance(value, dict)
            and value.get("schema") == 1
            and value.get("batch_id") == match.group(2)
        ):
            found.add(str(value["batch_id"]))
    return found


def queue_timestamp(
    previous: dict[str, Any] | None, *, already_queued: bool, now: str
) -> str:
    if already_queued and previous and previous.get("queued_at"):
        return str(previous["queued_at"])
    return now


def marker_queued_at(marker: dict[str, Any] | None) -> str | None:
    value = marker.get("queued_at") if marker else None
    return str(value) if value else None


def marker_priority_at(marker: dict[str, Any] | None) -> str | None:
    value = marker.get("priority_at") if marker else None
    return str(value) if value else None


def queue_state_body(
    state: str,
    head_sha: str,
    *,
    queued_at: str | None,
    priority_at: str | None = None,
    reason: str | None = None,
    intent_id: str | None = None,
    integration_batch_id: str | None = None,
) -> str:
    if state not in {"queued", "blocked", "dequeued"}:
        raise ValueError(f"unsupported queue state: {state}")
    marker: dict[str, Any] = {
        "head_sha": head_sha,
        "recorded_at": utc_now(),
        "schema": 1,
        "state": state,
    }
    if queued_at:
        marker["queued_at"] = queued_at
    if priority_at:
        marker["priority_at"] = priority_at
    if reason:
        marker["reason"] = reason
    if intent_id:
        marker["intent_id"] = intent_id
    if integration_batch_id:
        marker["integration_batch_id"] = integration_batch_id
    return (
        f"<!-- {STATE_MARKER_PREFIX} {json.dumps(marker, sort_keys=True)} -->\n"
        f"Recorded merge queue state `{state}` on `{head_sha}`."
    )


def effective_queue_marker(
    comments: Iterable[dict[str, Any]],
    trusted_logins: Iterable[str],
    coordinator_logins: Iterable[str],
    *,
    intent_scope: str = "head",
    integration_authorized: Callable[[dict[str, Any]], bool] | None = None,
) -> dict[str, Any] | None:
    """Accept coordinator state only when rooted in a live trusted deploy intent."""
    values = list(comments)
    direct = latest_marker(values, trusted_logins)
    intent = latest_intent(values, trusted_logins)
    delegated = latest_marker(values, coordinator_logins)
    if (
        delegated
        and intent
        and intent.get("state") == "requested"
        and delegated.get("intent_id") == intent.get("intent_id")
        and intent_scope == "head"
        and intent.get("requested_head") == delegated.get("head_sha")
    ):
        direct_time = str((direct or {}).get("recorded_at") or "")
        delegated_time = str(delegated.get("recorded_at") or "")
        return delegated if delegated_time >= direct_time else direct
    integration = latest_payload(values, INTEGRATION_MARKER, coordinator_logins)
    if (
        delegated
        and integration
        and delegated.get("integration_batch_id") == integration.get("batch_id")
        and (integration_authorized is None or integration_authorized(integration))
    ):
        return delegated
    return direct


def coordinator_logins(client: Any) -> set[str]:
    value = getattr(client, "coordinator_logins", None)
    if isinstance(value, (set, frozenset, list, tuple)):
        return {str(item) for item in value}
    trusted = getattr(client, "trusted_logins", set())
    if isinstance(trusted, (set, frozenset, list, tuple)):
        return {str(item) for item in trusted}
    return set()


def queue_marker_for_client(
    client: Any, comments: Iterable[dict[str, Any]]
) -> dict[str, Any] | None:
    validator = getattr(client, "integration_sources_authorized", None)
    return effective_queue_marker(
        comments,
        client.trusted_logins,
        coordinator_logins(client),
        intent_scope=client.config.pipeline.intent_scope,
        integration_authorized=validator if callable(validator) else None,
    )


def require_running_pipeline(client: Any) -> None:
    reader = getattr(client, "pipeline_control", None)
    if not callable(reader):
        return
    value = reader()
    if isinstance(value, dict) and value.get("state") == "paused":
        raise QueueError(
            "DeployBot pipeline is paused: " + str(value.get("reason") or "unknown")
        )


def structured_dependencies(body: str, directive: str) -> list[int]:
    pattern = re.compile(rf"^{re.escape(directive)}:\s*(.*?)\s*$", re.I | re.MULTILINE)
    values: set[int] = set()
    for match in pattern.findall(body):
        values.update(int(value) for value in PULL_REQUEST_NUMBER.findall(match))
    return sorted(values)


def generated_only_change(
    path: str,
    patch: str | None,
    *,
    generated_paths: frozenset[str],
    generated_version_paths: frozenset[str],
    asset_version_pattern: str,
) -> bool:
    if path in generated_paths:
        return True
    if not any(fnmatch.fnmatch(path, pattern) for pattern in generated_version_paths):
        return False
    if not patch:
        return False

    version = re.compile(asset_version_pattern)
    removed: list[str] = []
    added: list[str] = []
    for line in patch.splitlines():
        if line.startswith("---") or line.startswith("+++"):
            continue
        if line.startswith("-"):
            removed.append(version.sub("?v=<generated>", line[1:]))
        elif line.startswith("+"):
            added.append(version.sub("?v=<generated>", line[1:]))
    return bool(removed or added) and removed == added


def overlap_groups(entries: Iterable["QueueEntry"]) -> list[dict[str, Any]]:
    values = list(entries)
    adjacency: dict[int, set[int]] = {entry.number: set() for entry in values}
    shared: dict[tuple[int, int], set[str]] = {}
    for index, left in enumerate(values):
        for right in values[index + 1 :]:
            paths = set(left.source_paths) & set(right.source_paths)
            if not paths:
                continue
            pair = tuple(sorted((left.number, right.number)))
            shared[pair] = paths
            adjacency[left.number].add(right.number)
            adjacency[right.number].add(left.number)

    groups: list[dict[str, Any]] = []
    visited: set[int] = set()
    for number in sorted(adjacency):
        if number in visited or not adjacency[number]:
            continue
        pending = [number]
        component: set[int] = set()
        while pending:
            current = pending.pop()
            if current in component:
                continue
            component.add(current)
            pending.extend(adjacency[current] - component)
        visited.update(component)
        paths: set[str] = set()
        for pair, pair_paths in shared.items():
            if pair[0] in component and pair[1] in component:
                paths.update(pair_paths)
        groups.append(
            {"pull_requests": sorted(component), "source_paths": sorted(paths)}
        )
    return groups


def batch_fingerprint(entries: Iterable["QueueEntry"]) -> str:
    material = [
        {
            "head_sha": entry.head_sha,
            "number": entry.number,
            "dependencies": entry.dependencies,
            "source_paths": entry.source_paths,
        }
        for entry in entries
    ]
    raw = json.dumps(material, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def new_batch(entries: list["QueueEntry"], *, frozen_at: str) -> dict[str, Any]:
    fingerprint = batch_fingerprint(entries)
    compact_time = frozen_at.replace("-", "").replace(":", "")
    return {
        "batch_id": f"{compact_time}-{fingerprint}",
        "fingerprint": fingerprint,
        "frozen_at": frozen_at,
        "dependencies": {str(entry.number): entry.dependencies for entry in entries},
        "heads": {str(entry.number): entry.head_sha for entry in entries},
        "pull_requests": [entry.number for entry in entries],
        "schema": 1,
        "source_paths": {str(entry.number): entry.source_paths for entry in entries},
    }


def reusable_batch(
    markers: list[dict[str, Any] | None],
    entries: list["QueueEntry"],
    fingerprint: str,
) -> bool:
    return (
        len(markers) == len(entries)
        and all(
            marker
            and marker.get("fingerprint") == fingerprint
            and str(marker.get("frozen_at") or "") >= str(entry.queued_at or "")
            for marker, entry in zip(markers, entries, strict=True)
        )
        and len({str(marker["batch_id"]) for marker in markers if marker}) == 1
    )


def batch_overlap_peers(
    batch: dict[str, Any], number: int, active_numbers: set[int]
) -> list[int]:
    paths_by_pr = batch.get("source_paths") or {}
    target_paths = set(paths_by_pr.get(str(number)) or [])
    return sorted(
        int(peer)
        for peer in batch.get("pull_requests") or []
        if int(peer) != number
        and int(peer) in active_numbers
        and target_paths.intersection(paths_by_pr.get(str(peer)) or [])
    )


def active_batch(
    entries: list["QueueEntry"],
    latest_markers: dict[int, dict[str, Any] | None],
    completed: set[str] | None = None,
) -> dict[str, Any] | None:
    completed = completed or set()
    candidates: dict[str, dict[str, Any]] = {}
    for entry in entries:
        marker = latest_markers.get(entry.number)
        if not marker:
            continue
        if str(marker.get("batch_id") or "") in completed:
            continue
        members = {int(value) for value in marker.get("pull_requests") or []}
        heads = marker.get("heads") or {}
        if (
            entry.number not in members
            or heads.get(str(entry.number)) != entry.head_sha
            or str(marker.get("frozen_at") or "") < str(entry.queued_at or "")
        ):
            continue
        candidates[str(marker["batch_id"])] = marker
    if not candidates:
        return None
    return max(candidates.values(), key=lambda value: str(value.get("frozen_at") or ""))


def entries_in_batch(
    entries: list["QueueEntry"], batch: dict[str, Any]
) -> list["QueueEntry"]:
    members = {int(value) for value in batch.get("pull_requests") or []}
    heads = batch.get("heads") or {}
    frozen_at = str(batch.get("frozen_at") or "")
    return [
        entry
        for entry in entries
        if entry.number in members
        and heads.get(str(entry.number)) == entry.head_sha
        and frozen_at >= str(entry.queued_at or "")
    ]


def split_blocked_entries(
    entries: list["QueueEntry"], blocked_label: str
) -> tuple[list["QueueEntry"], list["QueueEntry"]]:
    blocked = [
        entry
        for entry in entries
        if blocked_label in entry.labels or entry.state == "blocked"
    ]
    eligible = [entry for entry in entries if entry not in blocked]
    return eligible, blocked


@dataclass
class QueueEntry:
    number: int
    title: str
    url: str
    head_sha: str
    queued_head_sha: str | None
    queued_at: str | None
    queue_state: str | None
    is_draft: bool
    base_branch: str
    mergeable: str
    merge_state: str
    labels: list[str]
    checks: dict[str, str]
    review_verdicts: tuple[ReviewVerdict, ...]
    source_paths: list[str]
    generated_paths: list[str]
    dependencies: list[int]
    state: str = "waiting"
    reasons: list[str] | None = None
    priority_at: str | None = None
    integration_batch_id: str | None = None

    def classify(
        self,
        config: QueueConfig,
        *,
        require_marker: bool = True,
        allow_blocked_label: bool = False,
    ) -> None:
        blocked: list[str] = []
        waiting: list[str] = []
        if self.base_branch != config.base_branch:
            blocked.append(
                f"base branch is {self.base_branch}, not {config.base_branch}"
            )
        if self.is_draft:
            blocked.append("pull request is draft")
        if require_marker and config.queue_label not in self.labels:
            blocked.append("queue authorization label is missing")
        if config.blocked_label in self.labels and not allow_blocked_label:
            blocked.append(f"{config.blocked_label} label is set")
        if require_marker:
            if not self.queued_head_sha:
                blocked.append("queue marker is missing")
            elif self.queued_head_sha != self.head_sha:
                blocked.append("head changed after it was queued")
            elif self.queue_state == "blocked":
                blocked.append("a trusted actor blocked this pull request")
            elif self.queue_state != "queued":
                blocked.append("queue authorization was revoked")

        for name in config.required_checks:
            status = self.checks.get(name)
            if status == "failed":
                blocked.append(f"{name} failed")
            elif status != "passed":
                waiting.append(f"{name} is not complete")

        for verdict in self.review_verdicts:
            if verdict.state == "blocked":
                blocked.extend(verdict.reasons)
            elif verdict.state != "passed":
                waiting.extend(verdict.reasons)

        if self.mergeable == "CONFLICTING" or self.merge_state == "DIRTY":
            blocked.append("pull request conflicts with main")
        elif self.merge_state in {"BLOCKED", "DRAFT"}:
            blocked.append(
                f"GitHub reports the pull request merge state as {self.merge_state}"
            )
        elif self.merge_state == "UNKNOWN" or self.mergeable != "MERGEABLE":
            waiting.append("GitHub is still computing mergeability")

        self.reasons = blocked + waiting
        if blocked:
            self.state = "blocked"
        elif waiting:
            self.state = "waiting"
        else:
            self.state = "ready"


class GitHub:
    def __init__(
        self,
        config: QueueConfig,
        repository: str | None = None,
        *,
        cwd: Path | None = None,
    ) -> None:
        if shutil.which("gh") is None:
            raise QueueError("the GitHub CLI is required")
        self.config = config
        self.cwd = (cwd or Path.cwd()).resolve()
        self.repository = repository or self._resolve_repository()
        if self.repository.count("/") != 1:
            raise QueueError(f"invalid repository name: {self.repository}")
        self.owner, self.name = self.repository.split("/", 1)
        self._registry_issue_cache: int | None = None
        self.trusted_logins = {
            self.owner if value == "@repository-owner" else value
            for value in self.config.trusted_actors
        }
        self.coordinator_logins = {
            self.owner if value == "@repository-owner" else value
            for value in self.config.coordinator_actors
        }

    def _run(self, *arguments: str) -> str:
        completed = subprocess.run(
            ["gh", *arguments],
            cwd=self.cwd,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise QueueError(detail or f"gh {' '.join(arguments)} failed")
        return completed.stdout

    def _json(self, *arguments: str) -> Any:
        raw = self._run(*arguments)
        try:
            return json.loads(raw)
        except json.JSONDecodeError as error:
            raise QueueError("GitHub returned invalid JSON") from error

    def _resolve_repository(self) -> str:
        data = self._json("repo", "view", "--json", "nameWithOwner")
        return str(data["nameWithOwner"])

    def resolve_pr(self, selector: str | None) -> int:
        arguments = ["pr", "view"]
        if selector:
            arguments.append(selector)
        arguments.extend(["--repo", self.repository, "--json", "number"])
        return int(self._json(*arguments)["number"])

    def label_specs(self) -> tuple[tuple[str, str, str], ...]:
        return (
            (
                self.config.queue_label,
                "0E8A16",
                "Approved for the agent-managed merge queue",
            ),
            (
                self.config.blocked_label,
                "B60205",
                "Merge queue item needs agent attention",
            ),
            (
                self.config.pipeline.intent_label,
                "1D76DB",
                "User requested deployment; waiting for exact-head gates",
            ),
            (
                self.config.pipeline.pause_label,
                "D93F0B",
                "DeployBot pipeline is paused after a delivery failure",
            ),
            (
                self.config.pipeline.registry_label,
                "5319E7",
                "DeployBot metadata registry (never transcript contents)",
            ),
        )

    def ensure_labels(self) -> None:
        for name, color, description in self.label_specs():
            self._run(
                "label",
                "create",
                name,
                "--repo",
                self.repository,
                "--color",
                color,
                "--description",
                description,
                "--force",
            )

    def ensure_labels_exist(self) -> None:
        values = self._json(
            "label",
            "list",
            "--repo",
            self.repository,
            "--limit",
            "1000",
            "--json",
            "name",
        )
        existing = {str(value.get("name") or "") for value in values}
        for name, color, description in self.label_specs():
            if name in existing:
                continue
            self._run(
                "label",
                "create",
                name,
                "--repo",
                self.repository,
                "--color",
                color,
                "--description",
                description,
                "--force",
            )

    def viewer_login(self) -> str:
        value = self._json("api", "user")
        return str(value.get("login") or "")

    def require_actor(self, allowed: Iterable[str], action: str) -> str:
        login = self.viewer_login()
        if login.lower() not in {value.lower() for value in allowed}:
            raise QueueError(f"{login or 'current GitHub identity'} cannot {action}")
        return login

    def open_pull_request_numbers(self) -> list[int]:
        return [
            int(value["number"])
            for value in self._paged_api(
                f"repos/{self.repository}/pulls?state=open&per_page=100"
            )
        ]

    def integration_pull_request_numbers(self) -> list[int]:
        prefix = self.config.integration.branch_prefix.rstrip("/") + "/"
        return [
            int(value["number"])
            for value in self._paged_api(
                f"repos/{self.repository}/pulls?state=open&per_page=100"
            )
            if str((value.get("head") or {}).get("ref") or "").startswith(prefix)
        ]

    def intent_numbers(self) -> list[int]:
        values = self._paged_api(
            f"repos/{self.repository}/pulls?state=open&per_page=100"
        )
        return [
            int(value["number"])
            for value in values
            if self.config.pipeline.intent_label
            in {str(label.get("name") or "") for label in value.get("labels") or []}
        ]

    def active_integration_sources(self) -> set[int]:
        sources: set[int] = set()
        for number in self.integration_pull_request_numbers():
            marker = latest_payload(
                self.comments(number),
                INTEGRATION_MARKER,
                self.coordinator_logins,
            )
            if marker:
                sources.update(
                    int(value) for value in marker.get("pull_requests") or []
                )
        return sources

    def base_sha(self) -> str:
        value = self._json(
            "api", f"repos/{self.repository}/commits/{self.config.base_branch}"
        )
        return str(value["sha"])

    def workflow_runs(self) -> list[dict[str, Any]]:
        return self._paged_object_items(
            f"repos/{self.repository}/actions/runs?branch={self.config.base_branch}&per_page=100",
            "workflow_runs",
        )

    def source_deploy_authorized(self, number: int, expected_head: str) -> bool:
        source = self._json(
            "pr",
            "view",
            str(number),
            "--repo",
            self.repository,
            "--json",
            "headRefOid",
        )
        if source.get("headRefOid") != expected_head:
            return False
        comments = self.comments(number)
        intent = latest_intent(comments, self.trusted_logins)
        direct = latest_marker(comments, self.trusted_logins)
        return bool(
            (
                intent
                and intent.get("state") == "requested"
                and intent.get("requested_head") == expected_head
            )
            or (
                direct
                and direct.get("state") == "queued"
                and direct.get("head_sha") == expected_head
            )
        )

    def integration_sources_authorized(self, integration: dict[str, Any]) -> bool:
        expected_heads = integration.get("heads") or {}
        sources = integration.get("pull_requests") or []
        if not sources:
            return False
        for value in sources:
            try:
                number = int(value)
            except (TypeError, ValueError):
                return False
            expected = str(expected_heads.get(str(number)) or "")
            if not expected or not self.source_deploy_authorized(number, expected):
                return False
        return True

    def dispatch_ci_workflows(self) -> list[dict[str, Any]]:
        configured = self.config.pipeline.ci_workflows
        if not configured:
            raise QueueError("no CI workflow is configured for post-merge dispatch")
        values = self._json(
            "workflow",
            "list",
            "--repo",
            self.repository,
            "--all",
            "--limit",
            "1000",
            "--json",
            "id,name,state",
        )
        workflows = values if isinstance(values, list) else []
        dispatched: list[dict[str, Any]] = []
        for name in configured:
            matches = [
                value
                for value in workflows
                if str(value.get("name") or "") == name
                and str(value.get("state") or "") == "active"
            ]
            if len(matches) != 1:
                raise QueueError(
                    f"configured CI workflow {name!r} did not resolve to one active workflow"
                )
            workflow_id = int(matches[0]["id"])
            self._run(
                "workflow",
                "run",
                str(workflow_id),
                "--repo",
                self.repository,
                "--ref",
                self.config.base_branch,
            )
            dispatched.append({"id": workflow_id, "name": name})
        return dispatched

    def dispatch_deploy_workflows(
        self, *, ci_run: dict[str, Any]
    ) -> list[dict[str, Any]]:
        configured = self.config.pipeline.deploy_workflows
        if not configured:
            raise QueueError("no deployment workflow is configured")
        ci_sha = str(ci_run.get("head_sha") or "")
        ci_run_id = int(ci_run.get("id") or 0)
        if not ci_sha or not ci_run_id:
            raise QueueError("successful CI identity is incomplete")
        values = self._json(
            "workflow",
            "list",
            "--repo",
            self.repository,
            "--all",
            "--limit",
            "1000",
            "--json",
            "id,name,state",
        )
        workflows = values if isinstance(values, list) else []
        dispatched: list[dict[str, Any]] = []
        for name in configured:
            matches = [
                value
                for value in workflows
                if str(value.get("name") or "") == name
                and str(value.get("state") or "") == "active"
            ]
            if len(matches) != 1:
                raise QueueError(
                    f"configured deployment workflow {name!r} did not resolve "
                    "to one active workflow"
                )
            workflow_id = int(matches[0]["id"])
            self._run(
                "workflow",
                "run",
                str(workflow_id),
                "--repo",
                self.repository,
                "--ref",
                self.config.base_branch,
                "-f",
                f"ci_sha={ci_sha}",
                "-f",
                f"ci_run_id={ci_run_id}",
            )
            dispatched.append(
                {
                    "id": workflow_id,
                    "name": name,
                    "ci_sha": ci_sha,
                    "ci_run_id": ci_run_id,
                }
            )
        return dispatched

    def recent_merged_pull_requests(self, limit: int) -> list[dict[str, Any]]:
        values = self._paged_api(
            "repos/"
            f"{self.repository}/pulls?state=closed&sort=updated&direction=desc&per_page=100"
        )
        return [value for value in values if value.get("merged_at")][:limit]

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        comparison = self._json(
            "api", f"repos/{self.repository}/compare/{ancestor}...{descendant}"
        )
        return comparison.get("status") in {"ahead", "identical"}

    def registry_issue_number(self, *, create: bool) -> int | None:
        cached = getattr(self, "_registry_issue_cache", None)
        if cached is not None:
            return cached
        values = self._paged_api(
            f"repos/{self.repository}/issues?state=all&labels={self.config.pipeline.registry_label}&per_page=100"
        )
        matches = [
            value
            for value in values
            if "pull_request" not in value
            and str(value.get("title") or "") == self.config.pipeline.registry_title
        ]
        if matches:
            # GitHub Issues has no uniqueness constraint. Concurrent first-use
            # workers can both create a registry after observing none. The
            # lowest issue number is deterministic and comments remain trusted
            # by authenticated author, so all workers safely converge there.
            canonical = min(int(value["number"]) for value in matches)
            self._registry_issue_cache = canonical
            return canonical
        if not create:
            return None
        self.ensure_labels_exist()
        created = self._json(
            "api",
            "--method",
            "POST",
            f"repos/{self.repository}/issues",
            "-f",
            f"title={self.config.pipeline.registry_title}",
            "-f",
            "body=DeployBot stores minimal delivery metadata here. Never post prompts, transcripts, source code, or credentials.",
            "-f",
            f"labels[]={self.config.pipeline.registry_label}",
        )
        # Re-read after creation so a concurrent lower-numbered creator wins
        # before this worker writes any metadata to its candidate issue.
        canonical = self.registry_issue_number(create=False) or int(created["number"])
        self._registry_issue_cache = canonical
        return canonical

    def issue_comment(self, number: int, body: str) -> None:
        self._json(
            "api",
            "--method",
            "POST",
            f"repos/{self.repository}/issues/{number}/comments",
            "-f",
            f"body={body}",
        )

    def registry_comments(self) -> list[dict[str, Any]]:
        number = self.registry_issue_number(create=False)
        return self.comments(number) if number is not None else []

    def record_thread(self, record: ThreadRecord) -> None:
        number = self.registry_issue_number(create=True)
        if number is None:  # pragma: no cover - create owns this invariant.
            raise QueueError("could not create DeployBot registry")
        self.issue_comment(number, thread_record_body(record))

    def thread_records(self) -> list[dict[str, Any]]:
        trusted = self.trusted_logins | self.coordinator_logins
        return [
            record.as_dict()
            for record in latest_thread_records(
                self.registry_comments(),
                trusted,
                active_hours=self.config.pipeline.thread_active_hours,
            )
        ]

    def pipeline_control(self) -> dict[str, Any]:
        value = latest_payload(
            self.registry_comments(),
            CONTROL_MARKER,
            self.coordinator_logins,
        )
        return value or {"state": "running"}

    def set_pipeline_control(self, state: str, reason: str | None = None) -> None:
        number = self.registry_issue_number(create=True)
        if number is None:  # pragma: no cover
            raise QueueError("could not create DeployBot registry")
        self.issue_comment(number, control_body(state=state, reason=reason))

    def create_integration_pull_request(
        self,
        *,
        batch: dict[str, Any],
        entries: list[QueueEntry],
    ) -> dict[str, Any]:
        batch_id = str(batch["batch_id"])
        safe_id = re.sub(r"[^a-zA-Z0-9._-]+", "-", batch_id)[-80:]
        branch = f"{self.config.integration.branch_prefix}/{safe_id}"
        base_sha = self.base_sha()
        try:
            self._json(
                "api",
                "--method",
                "POST",
                f"repos/{self.repository}/git/refs",
                "-f",
                f"ref=refs/heads/{branch}",
                "-f",
                f"sha={base_sha}",
            )
        except QueueError as error:
            if "Reference already exists" not in str(error):
                raise

        merged_heads: list[str] = []
        conflict: dict[str, Any] | None = None
        for entry in entries:
            try:
                self._json(
                    "api",
                    "--method",
                    "POST",
                    f"repos/{self.repository}/merges",
                    "-f",
                    f"base={branch}",
                    "-f",
                    f"head={entry.head_sha}",
                    "-f",
                    f"commit_message=DeployBot batch {batch_id}: PR #{entry.number}",
                )
                merged_heads.append(entry.head_sha)
            except QueueError as error:
                conflict = {
                    "number": entry.number,
                    "head_sha": entry.head_sha,
                    "reason": str(error),
                }
                break

        existing = self._json(
            "api",
            f"repos/{self.repository}/pulls?state=open&head={self.owner}:{branch}",
        )
        if isinstance(existing, list) and existing:
            pull = existing[0]
        else:
            members = ", ".join(f"#{entry.number}" for entry in entries)
            body = (
                f"DeployBot cumulative batch `{batch_id}` for {members}.\n\n"
                "Every source head was frozen and independently authorized. "
                "If a merge conflict remains, an agent must resolve it without "
                "dropping either side before marking this PR ready."
            )
            pull = self._json(
                "api",
                "--method",
                "POST",
                f"repos/{self.repository}/pulls",
                "-f",
                f"title={self.config.integration.title_prefix}: {safe_id}",
                "-f",
                f"head={branch}",
                "-f",
                f"base={self.config.base_branch}",
                "-f",
                f"body={body}",
                "-F",
                f"draft={'true' if conflict else 'false'}",
            )
        number = int(pull["number"])
        marker = {
            "base_sha": base_sha,
            "batch_id": batch_id,
            "conflict": conflict,
            "created_at": utc_now(),
            "heads": {str(entry.number): entry.head_sha for entry in entries},
            "merged_heads": merged_heads,
            "pull_requests": [entry.number for entry in entries],
        }
        self.comment(number, integration_body(marker))
        # The cumulative PR now owns this batch. Remove source queue labels only
        # after its durable marker exists so repeated events cannot scaffold a
        # second integration PR for the same source work.
        for entry in entries:
            source_labels = self.labels(entry.number)
            for label in (
                self.config.queue_label,
                self.config.pipeline.intent_label,
            ):
                if label in source_labels:
                    self.remove_label(entry.number, label)
        return {
            "number": number,
            "url": pull.get("html_url"),
            "branch": branch,
            "conflict": conflict,
            "batch_id": batch_id,
        }

    def _paged_api(self, endpoint: str) -> list[dict[str, Any]]:
        data = self._json("api", "--paginate", "--slurp", endpoint)
        pages = data if isinstance(data, list) else []
        values: list[dict[str, Any]] = []
        for page in pages:
            if not isinstance(page, list):
                raise QueueError(f"unexpected GitHub response for {endpoint}")
            values.extend(item for item in page if isinstance(item, dict))
        return values

    def _paged_object_items(self, endpoint: str, key: str) -> list[dict[str, Any]]:
        data = self._json("api", "--paginate", "--slurp", endpoint)
        pages = data if isinstance(data, list) else []
        values: list[dict[str, Any]] = []
        for page in pages:
            items = page.get(key) if isinstance(page, dict) else None
            if not isinstance(items, list):
                raise QueueError(f"unexpected GitHub response for {endpoint}")
            values.extend(item for item in items if isinstance(item, dict))
        return values

    def comments(self, number: int) -> list[dict[str, Any]]:
        return self._paged_api(
            f"repos/{self.repository}/issues/{number}/comments?per_page=100"
        )

    def reviews(self, number: int) -> list[dict[str, Any]]:
        return self._paged_api(
            f"repos/{self.repository}/pulls/{number}/reviews?per_page=100"
        )

    def files(self, number: int) -> list[dict[str, Any]]:
        return self._paged_api(
            f"repos/{self.repository}/pulls/{number}/files?per_page=100"
        )

    def changed_paths(self, number: int) -> tuple[list[str], list[str]]:
        source_paths: list[str] = []
        generated_paths: list[str] = []
        for value in self.files(number):
            path = str(value.get("filename") or "")
            if not path:
                continue
            if path in self.config.generated_paths:
                generated_paths.append(path)
                source_paths.append(path)
                continue
            target = (
                generated_paths
                if generated_only_change(
                    path,
                    value.get("patch"),
                    generated_paths=self.config.generated_paths,
                    generated_version_paths=self.config.generated_version_paths,
                    asset_version_pattern=self.config.asset_version_pattern,
                )
                else source_paths
            )
            target.append(path)
        return source_paths, generated_paths

    def review_threads(self, number: int) -> list[dict[str, Any]]:
        data = self._json(
            "api",
            "graphql",
            "-f",
            f"query={REVIEW_THREADS_QUERY}",
            "-F",
            f"owner={self.owner}",
            "-F",
            f"name={self.name}",
            "-F",
            f"number={number}",
        )
        try:
            threads = data["data"]["repository"]["pullRequest"]["reviewThreads"]
        except (KeyError, TypeError) as error:
            raise QueueError("could not read pull-request review threads") from error
        if threads["pageInfo"]["hasNextPage"]:
            raise QueueError(f"PR #{number} has more than 100 review threads")

        for thread in threads["nodes"]:
            comments = thread["comments"]
            if comments["pageInfo"]["hasNextPage"]:
                raise QueueError(f"PR #{number} has a review thread over 100 comments")
        return [value for value in threads["nodes"] if isinstance(value, dict)]

    def snapshot(
        self,
        number: int,
        *,
        require_marker: bool = True,
        allow_blocked_label: bool = False,
        known_comments: list[dict[str, Any]] | None = None,
        known_source_paths: list[str] | None = None,
        known_generated_paths: list[str] | None = None,
        defer_paths_until_ready: bool = False,
    ) -> QueueEntry:
        fields = ",".join(
            (
                "baseRefName",
                "body",
                "headRefOid",
                "isDraft",
                "labels",
                "mergeStateStatus",
                "mergeable",
                "number",
                "state",
                "statusCheckRollup",
                "title",
                "url",
            )
        )
        pull = self._json(
            "pr",
            "view",
            str(number),
            "--repo",
            self.repository,
            "--json",
            fields,
        )
        if pull.get("state") != "OPEN":
            raise QueueError(f"PR #{number} is not open")

        comments = known_comments if known_comments is not None else self.comments(number)
        reviews = self.reviews(number) if self.config.review_providers else []
        needs_threads = any(
            provider.kind == "bot" and provider.require_resolved_threads
            for provider in self.config.review_providers
        )
        threads = self.review_threads(number) if needs_threads else []
        marker = queue_marker_for_client(self, comments)
        head_sha = str(pull["headRefOid"])
        checks = check_states(pull.get("statusCheckRollup") or [])
        source_paths = list(known_source_paths or [])
        generated_paths = list(known_generated_paths or [])
        paths_are_known = (
            known_source_paths is not None and known_generated_paths is not None
        )
        if not paths_are_known and not defer_paths_until_ready:
            source_paths, generated_paths = self.changed_paths(number)

        body = str(pull.get("body") or "")
        entry = QueueEntry(
            number=int(pull["number"]),
            title=str(pull["title"]),
            url=str(pull["url"]),
            head_sha=head_sha,
            queued_head_sha=str(marker.get("head_sha")) if marker else None,
            queued_at=marker_queued_at(marker),
            queue_state=str(marker.get("state")) if marker else None,
            is_draft=bool(pull["isDraft"]),
            base_branch=str(pull["baseRefName"]),
            mergeable=str(pull.get("mergeable") or "UNKNOWN").upper(),
            merge_state=str(pull.get("mergeStateStatus") or "UNKNOWN").upper(),
            labels=sorted(str(label["name"]) for label in pull.get("labels") or []),
            checks=checks,
            review_verdicts=evaluate_reviews(
                self.config.review_providers,
                head_sha=head_sha,
                checks=checks,
                comments=comments,
                reviews=reviews,
                threads=threads,
            ),
            source_paths=sorted(set(source_paths)),
            generated_paths=sorted(set(generated_paths)),
            dependencies=structured_dependencies(
                body, self.config.dependency_directive
            ),
            priority_at=marker_priority_at(marker),
            integration_batch_id=(
                str(marker.get("integration_batch_id") or "") or None
                if marker
                else None
            ),
        )
        entry.classify(
            self.config,
            require_marker=require_marker,
            allow_blocked_label=allow_blocked_label,
        )
        if not paths_are_known and defer_paths_until_ready and entry.state == "ready":
            entry.source_paths, entry.generated_paths = self.changed_paths(number)
        return entry

    def queued_numbers(self) -> list[int]:
        values = self._paged_api(
            f"repos/{self.repository}/pulls?state=open&per_page=100"
        )
        return [
            int(value["number"])
            for value in values
            if self.config.queue_label
            in {str(label.get("name") or "") for label in value.get("labels") or []}
        ]

    def queue(self) -> list[QueueEntry]:
        entries = [self.snapshot(number) for number in self.queued_numbers()]
        return sorted(
            entries,
            key=lambda entry: (
                entry.priority_at or entry.queued_at or "9999",
                entry.number if entry.priority_at else 0,
                entry.queued_at or "9999",
                entry.number,
            ),
        )

    def add_label(self, number: int, label: str) -> None:
        self._run(
            "pr",
            "edit",
            str(number),
            "--repo",
            self.repository,
            "--add-label",
            label,
        )

    def labels(self, number: int) -> set[str]:
        data = self._json(
            "pr",
            "view",
            str(number),
            "--repo",
            self.repository,
            "--json",
            "labels",
        )
        return {str(label.get("name") or "") for label in data.get("labels") or []}

    def remove_label(self, number: int, label: str) -> None:
        self._run(
            "pr",
            "edit",
            str(number),
            "--repo",
            self.repository,
            "--remove-label",
            label,
        )

    def comment(self, number: int, body: str) -> None:
        self._run(
            "pr",
            "comment",
            str(number),
            "--repo",
            self.repository,
            "--body",
            body,
        )

    def dependency_is_merged(self, number: int) -> bool:
        data = self._json(
            "pr",
            "view",
            str(number),
            "--repo",
            self.repository,
            "--json",
            "mergeCommit,mergedAt",
        )
        merge_commit = data.get("mergeCommit") or {}
        merge_sha = str(merge_commit.get("oid") or "")
        if not data.get("mergedAt") or not merge_sha:
            return False
        comparison = self._json(
            "api",
            (
                f"repos/{self.repository}/compare/"
                f"{merge_sha}...{self.config.base_branch}"
            ),
        )
        return comparison.get("status") in {"ahead", "identical"}

    def merge(
        self,
        number: int,
        head_sha: str,
        *,
        authorization_entry: QueueEntry | None = None,
    ) -> str:
        if authorization_entry is None:
            authorization = self._json(
                "pr",
                "view",
                str(number),
                "--repo",
                self.repository,
                "--json",
                "headRefOid,isDraft,labels,state",
            )
            labels = {
                str(label.get("name") or "")
                for label in authorization.get("labels") or []
            }
            if authorization.get("state") != "OPEN":
                raise QueueError(f"PR #{number} is no longer open")
            if authorization.get("isDraft"):
                raise QueueError(f"PR #{number} returned to draft before merge")
            if authorization.get("headRefOid") != head_sha:
                raise QueueError(f"PR #{number} changed immediately before merge")
            marker = queue_marker_for_client(self, self.comments(number))
            integration_batch_id = str(marker.get("integration_batch_id") or "") if marker else ""
        else:
            labels = set(authorization_entry.labels)
            if authorization_entry.is_draft:
                raise QueueError(f"PR #{number} returned to draft before merge")
            if authorization_entry.head_sha != head_sha:
                raise QueueError(f"PR #{number} changed immediately before merge")
            marker = {
                "state": authorization_entry.queue_state,
                "head_sha": authorization_entry.queued_head_sha,
            }
            integration_batch_id = authorization_entry.integration_batch_id or ""
        if self.config.queue_label not in labels or self.config.blocked_label in labels:
            raise QueueError(f"PR #{number} queue authorization was revoked")
        if (
            not marker
            or marker.get("state") != "queued"
            or marker.get("head_sha") != head_sha
        ):
            raise QueueError(f"PR #{number} durable queue authorization was revoked")
        if integration_batch_id:
            integration = latest_payload(
                self.comments(number),
                INTEGRATION_MARKER,
                getattr(self, "coordinator_logins", self.trusted_logins),
            )
            if not integration or integration.get("batch_id") != integration_batch_id:
                raise QueueError(f"PR #{number} integration authorization was revoked")
            expected_heads = integration.get("heads") or {}
            for source_number in integration.get("pull_requests") or []:
                source_number = int(source_number)
                expected = str(expected_heads.get(str(source_number)) or "")
                if not expected or not self.source_deploy_authorized(
                    source_number, expected
                ):
                    raise QueueError(
                        f"source PR #{source_number} deploy authorization was revoked"
                    )
                if not self.is_ancestor(expected, head_sha):
                    raise QueueError(
                        f"integration head does not contain source PR #{source_number}"
                    )

        result = self._json(
            "api",
            "--method",
            "PUT",
            f"repos/{self.repository}/pulls/{number}/merge",
            "-f",
            f"sha={head_sha}",
            "-f",
            f"merge_method={self.config.merge_method}",
        )
        if not result.get("merged"):
            raise QueueError(
                str(result.get("message") or f"PR #{number} was not merged")
            )
        return str(result["sha"])


def entry_dict(entry: QueueEntry) -> dict[str, Any]:
    value = asdict(entry)
    value["reasons"] = entry.reasons or []
    return value


def print_plan(entries: list[QueueEntry], *, json_output: bool) -> None:
    groups = overlap_groups(entries)
    if json_output:
        print(
            json.dumps(
                {
                    "queue": [entry_dict(entry) for entry in entries],
                    "overlap_groups": groups,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return
    if not entries:
        print("merge queue is empty")
        return
    for position, entry in enumerate(entries, start=1):
        review = ", ".join(
            f"{verdict.provider}: {verdict.state}" for verdict in entry.review_verdicts
        )
        review = review or "checks only"
        detail = "; ".join(entry.reasons or []) or "all merge gates passed"
        print(f"{position}. #{entry.number} {entry.state} ({review}) - {detail}")
    for group in groups:
        numbers = ", ".join(f"#{value}" for value in group["pull_requests"])
        paths = ", ".join(group["source_paths"])
        print(f"integration required: {numbers} overlap in {paths}")


def command_inspect(
    client: GitHub, selector: str | None, *, json_output: bool
) -> QueueEntry:
    number = client.resolve_pr(selector)
    entry = client.snapshot(
        number,
        require_marker=False,
        allow_blocked_label=True,
    )
    if json_output:
        print(json.dumps(entry_dict(entry), indent=2, sort_keys=True))
    else:
        print_plan([entry], json_output=False)
    return entry


def pipeline_status(client: GitHub) -> dict[str, Any]:
    queued = client.queue()
    queued_by_number = {entry.number: entry for entry in queued}
    stages: dict[str, list[dict[str, Any]]] = {
        "draft": [],
        "reviewing": [],
        "ready": [],
        "deploy_requested": [],
        "queued": [],
        "blocked": [],
    }
    for number in client.open_pull_request_numbers():
        if number in queued_by_number:
            value = entry_dict(queued_by_number[number])
            value["pipeline_stage"] = "queued"
            stages["queued"].append(value)
            continue
        entry = client.snapshot(
            number,
            require_marker=False,
            allow_blocked_label=True,
        )
        labels = set(entry.labels)
        if client.config.pipeline.intent_label in labels:
            stage = "deploy_requested"
        elif entry.is_draft:
            stage = "draft"
        elif entry.state == "ready":
            stage = "ready"
        elif entry.state == "waiting":
            stage = "reviewing"
        else:
            stage = "blocked"
        value = entry_dict(entry)
        value["pipeline_stage"] = stage
        stages[stage].append(value)
    main_sha = client.base_sha()
    delivery = release_state(
        main_sha=main_sha,
        runs=client.workflow_runs(),
        config=client.config.pipeline,
    )
    now = datetime.now(timezone.utc)
    alerts: list[dict[str, Any]] = []
    queue_target = client.config.pipeline.ready_to_merge_target_minutes * 60
    for entry in queued:
        timestamp = parse_time(entry.queued_at)
        elapsed = (now - timestamp).total_seconds() if timestamp else None
        if elapsed is not None and elapsed > queue_target:
            alerts.append(
                {
                    "stage": "queued-to-merge",
                    "pull_request": entry.number,
                    "elapsed_seconds": elapsed,
                    "target_seconds": queue_target,
                    "active_gate": "; ".join(entry.reasons or []) or "merge worker",
                }
            )
    if delivery["state"] not in {"verified", "testing"}:
        run = delivery.get("latest_ci") or delivery.get("latest_deploy") or {}
        created = str(run.get("created_at") or "")
        timestamp = parse_time(created)
        elapsed = (now - timestamp).total_seconds() if timestamp else None
        release_target = client.config.pipeline.merge_to_live_target_minutes * 60
        if elapsed is not None and elapsed > release_target:
            alerts.append(
                {
                    "stage": "merge-to-live",
                    "elapsed_seconds": elapsed,
                    "target_seconds": release_target,
                    "active_gate": delivery["state"],
                }
            )
    return {
        "repository": client.repository,
        "control": client.pipeline_control(),
        "threads": client.thread_records(),
        "pull_requests": stages,
        "queue": [entry_dict(entry) for entry in queued],
        "overlap_groups": overlap_groups(queued),
        "release": delivery,
        "alerts": alerts,
    }


def print_pipeline_status(value: dict[str, Any], *, json_output: bool) -> None:
    if json_output:
        print(json.dumps(value, indent=2, sort_keys=True))
        return
    stages = value["pull_requests"]
    print(
        "threads: "
        f"{len(value['threads'])} active; "
        f"deploy requests: {len(stages['deploy_requested'])}; "
        f"queue: {len(value['queue'])}; "
        f"release: {value['release']['state']}"
    )
    if value["control"].get("state") == "paused":
        print(f"pipeline paused: {value['control'].get('reason', 'unknown reason')}")
    for position, entry in enumerate(value["queue"], start=1):
        detail = "; ".join(entry.get("reasons") or []) or "ready"
        print(f"{position}. PR #{entry['number']} {entry['state']} - {detail}")
    for alert in value.get("alerts") or []:
        print(
            f"slow {alert['stage']}: {int(alert['elapsed_seconds'])}s; "
            f"active gate: {alert['active_gate']}"
        )


def command_thread_update(
    client: GitHub,
    *,
    provider: str,
    thread_id: str,
    phase: str,
    title: str | None,
    branch: str | None,
    pull_request: int | None,
    url: str | None,
) -> None:
    client.record_thread(
        ThreadRecord(
            provider=provider,
            thread_id=thread_id,
            phase=phase,
            updated_at=utc_now(),
            title=title,
            branch=branch,
            pull_request=pull_request,
            url=url,
        )
    )
    print(f"recorded {provider} thread {thread_id} as {phase}")


def command_request(
    client: GitHub,
    selector: str | None,
    *,
    provider: str | None,
    thread_id: str | None,
    thread_url: str | None,
) -> dict[str, Any]:
    number = client.resolve_pr(selector)
    actor = client.require_actor(client.trusted_logins, "request deployment")
    client.ensure_labels_exist()
    entry = client.snapshot(
        number,
        require_marker=False,
        allow_blocked_label=True,
        known_source_paths=[],
        known_generated_paths=[],
    )
    requested_at = utc_now()
    intent_id = hashlib.sha256(
        f"{client.repository}:{number}:{actor}:{requested_at}".encode()
    ).hexdigest()[:24]
    client.comment(
        number,
        intent_body(
            intent_id=intent_id,
            state="requested",
            requested_at=requested_at,
            requested_head=entry.head_sha,
            provider=provider,
            thread_id=thread_id,
            thread_url=thread_url,
        ),
    )
    if client.config.pipeline.intent_label not in entry.labels:
        client.add_label(number, client.config.pipeline.intent_label)
    if provider and thread_id:
        client.record_thread(
            ThreadRecord(
                provider=provider,
                thread_id=thread_id,
                phase="deploy-requested",
                updated_at=requested_at,
                branch=None,
                pull_request=number,
                url=thread_url,
            )
        )
    notify(
        client.config.pipeline,
        "deploy-requested",
        {
            "repository": client.repository,
            "pull_request": number,
            "head_sha": entry.head_sha,
            "intent_id": intent_id,
            "provider": provider,
            "thread_id": thread_id,
        },
    )
    promoted = False
    if client.config.pipeline.auto_promote and entry.state == "ready":
        queue_from_intent(
            client,
            client.snapshot(number, require_marker=False, allow_blocked_label=True),
            latest_intent(client.comments(number), client.trusted_logins) or {},
        )
        promoted = True
    result = {
        "pull_request": number,
        "intent_id": intent_id,
        "head_sha": entry.head_sha,
        "state": "queued" if promoted else "deploy-requested",
        "waiting": [] if promoted else entry.reasons or [],
    }
    print(json.dumps(result, sort_keys=True))
    return result


def command_cancel_request(client: GitHub, selector: str | None) -> None:
    number = client.resolve_pr(selector)
    client.require_actor(client.trusted_logins, "cancel deployment")
    comments = client.comments(number)
    intent = latest_intent(comments, client.trusted_logins)
    if not intent or intent.get("state") != "requested":
        raise QueueError(f"PR #{number} has no active deploy intent")
    client.comment(
        number,
        intent_body(
            intent_id=str(intent["intent_id"]),
            state="cancelled",
            requested_at=str(intent["requested_at"]),
            requested_head=str(intent["requested_head"]),
            provider=str(intent.get("provider") or "") or None,
            thread_id=str(intent.get("thread_id") or "") or None,
            thread_url=str(intent.get("thread_url") or "") or None,
        ),
    )
    labels = client.labels(number)
    for label in (client.config.pipeline.intent_label, client.config.queue_label):
        if label in labels:
            client.remove_label(number, label)
    print(f"cancelled deploy request for PR #{number}")


def command_refresh_request(client: GitHub, selector: str | None) -> dict[str, Any]:
    number = client.resolve_pr(selector)
    actor = client.require_actor(
        client.trusted_logins, "refresh deployment intent for a replacement head"
    )
    comments = client.comments(number)
    previous = latest_intent(comments, client.trusted_logins)
    if not previous or previous.get("state") != "requested":
        raise QueueError(f"PR #{number} has no active deploy intent to refresh")
    entry = client.snapshot(number, require_marker=False, allow_blocked_label=True)
    if entry.state != "ready":
        raise QueueError(
            f"PR #{number} replacement head is not ready: "
            + "; ".join(entry.reasons or ["unknown gate"])
        )
    refreshed_at = utc_now()
    intent_id = hashlib.sha256(
        f"{client.repository}:{number}:{actor}:{refreshed_at}:{entry.head_sha}".encode()
    ).hexdigest()[:24]
    client.comment(
        number,
        intent_body(
            intent_id=intent_id,
            state="requested",
            requested_at=refreshed_at,
            requested_head=entry.head_sha,
            provider=str(previous.get("provider") or "") or None,
            thread_id=str(previous.get("thread_id") or "") or None,
            thread_url=str(previous.get("thread_url") or "") or None,
            parent_intent_id=str(previous.get("intent_id") or "") or None,
        ),
    )
    intent = latest_intent(client.comments(number), client.trusted_logins)
    if not intent or intent.get("intent_id") != intent_id:
        raise QueueError("GitHub did not confirm the refreshed trusted intent")
    queue_from_intent(client, entry, intent)
    result = {
        "pull_request": number,
        "head_sha": entry.head_sha,
        "intent_id": intent_id,
        "parent_intent_id": previous.get("intent_id"),
        "state": "queued",
    }
    print(json.dumps(result, sort_keys=True))
    return result


def record_repair(
    client: GitHub,
    entry: QueueEntry,
    intent: dict[str, Any] | None,
    reason: str,
) -> dict[str, Any]:
    comments = client.comments(entry.number)
    previous = latest_payload(
        comments,
        REPAIR_MARKER,
        coordinator_logins(client),
    )
    if (
        previous
        and previous.get("head_sha") == entry.head_sha
        and previous.get("reason") == reason
    ):
        return previous
    payload = {
        "base_sha": client.base_sha(),
        "created_at": utc_now(),
        "head_sha": entry.head_sha,
        "intent_id": (intent or {}).get("intent_id"),
        "provider": (intent or {}).get("provider"),
        "pull_request": entry.number,
        "reason": reason,
        "resume_command": f"deploybot resume {entry.number}",
        "source_paths": entry.source_paths,
        "thread_id": (intent or {}).get("thread_id"),
        "thread_url": (intent or {}).get("thread_url"),
    }
    client.comment(entry.number, repair_body(payload))
    labels = client.labels(entry.number)
    if client.config.blocked_label not in labels:
        client.add_label(entry.number, client.config.blocked_label)
    if payload.get("provider") and payload.get("thread_id"):
        client.record_thread(
            ThreadRecord(
                provider=str(payload["provider"]),
                thread_id=str(payload["thread_id"]),
                phase="blocked",
                updated_at=str(payload["created_at"]),
                pull_request=entry.number,
                url=str(payload.get("thread_url") or "") or None,
            )
        )
    notify(
        client.config.pipeline,
        "repair-required",
        {"repository": client.repository, **payload},
    )
    return payload


def command_promote(
    client: GitHub,
    *,
    emit: bool = True,
    captured_entries: list[QueueEntry] | None = None,
) -> dict[str, Any]:
    promoted: list[int] = []
    waiting: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    integration_sources = client.active_integration_sources()

    def evaluate(
        number: int,
    ) -> tuple[str, dict[str, Any] | None, QueueEntry | None]:
        if number in integration_sources:
            return (
                "waiting",
                {"number": number, "reasons": ["cumulative integration PR is active"]},
                None,
            )
        comments = client.comments(number)
        intent = latest_intent(comments, client.trusted_logins)
        if not intent or intent.get("state") != "requested":
            return "skip", None, None
        entry = client.snapshot(
            number,
            require_marker=False,
            allow_blocked_label=True,
            known_comments=comments,
            defer_paths_until_ready=True,
        )
        if (
            client.config.pipeline.intent_scope == "head"
            and intent.get("requested_head") != entry.head_sha
        ):
            return (
                "waiting",
                {
                    "number": number,
                    "reasons": [
                        "deploy intent is bound to an older head; the trusted source "
                        "agent must run deploybot refresh-request"
                    ],
                },
                entry,
            )
        if client.config.blocked_label in entry.labels:
            return (
                "waiting",
                {
                    "number": number,
                    "reasons": [
                        "repair is blocked; the trusted source agent must run "
                        "deploybot resume after fresh gates pass"
                    ],
                },
                entry,
            )
        if entry.state == "ready":
            changed = queue_from_intent(
                client,
                entry,
                intent,
                comments=comments,
                labels=set(entry.labels),
            )
            if changed:
                return "promoted", {"number": number}, entry
            return "ready", None, entry
        if entry.state == "blocked":
            reason = "; ".join(entry.reasons or ["blocked"])
            record_repair(client, entry, intent, reason)
            return "blocked", {"number": number, "reason": reason}, entry
        return (
            "waiting",
            {"number": number, "reasons": entry.reasons or []},
            entry,
        )

    numbers = sorted(client.intent_numbers())
    if numbers:
        with ThreadPoolExecutor(
            max_workers=min(client.config.pipeline.promotion_workers, len(numbers))
        ) as executor:
            outcomes = executor.map(evaluate, numbers)
            for state, payload, entry in outcomes:
                if captured_entries is not None and entry is not None:
                    captured_entries.append(entry)
                if state == "promoted" and payload is not None:
                    promoted.append(int(payload["number"]))
                elif state == "blocked" and payload is not None:
                    blocked.append(payload)
                elif state == "waiting" and payload is not None:
                    waiting.append(payload)
    result = {"promoted": promoted, "waiting": waiting, "blocked": blocked}
    if emit:
        print(json.dumps(result, indent=2, sort_keys=True))
    return result


def promote_integrations(client: GitHub) -> list[int]:
    promoted: list[int] = []
    for number in client.integration_pull_request_numbers():
        comments = client.comments(number)
        integration = latest_payload(
            comments,
            INTEGRATION_MARKER,
            coordinator_logins(client),
        )
        if not integration or integration.get("conflict"):
            continue
        labels = client.labels(number)
        if client.config.queue_label in labels:
            continue
        entry = client.snapshot(number, require_marker=False, allow_blocked_label=True)
        if entry.state != "ready":
            continue
        batch_id = str(integration.get("batch_id") or "")
        client.comment(
            number,
            queue_state_body(
                "queued",
                entry.head_sha,
                queued_at=utc_now(),
                integration_batch_id=batch_id,
            ),
        )
        client.add_label(number, client.config.queue_label)
        for source_number in integration.get("pull_requests") or []:
            source_labels = client.labels(int(source_number))
            if client.config.queue_label in source_labels:
                client.remove_label(int(source_number), client.config.queue_label)
        promoted.append(number)
        notify(
            client.config.pipeline,
            "integration-queued",
            {
                "repository": client.repository,
                "pull_request": number,
                "batch_id": batch_id,
                "head_sha": entry.head_sha,
            },
        )
    return promoted


def command_resume(client: GitHub, selector: str | None) -> None:
    number = client.resolve_pr(selector)
    comments = client.comments(number)
    integration = latest_payload(
        comments,
        INTEGRATION_MARKER,
        coordinator_logins(client),
    )
    if integration:
        entry = client.snapshot(number, require_marker=False, allow_blocked_label=True)
        for source_number, source_head in (integration.get("heads") or {}).items():
            if not client.is_ancestor(str(source_head), entry.head_sha):
                raise QueueError(
                    f"integration head is still missing source PR #{source_number}"
                )
        if entry.state != "ready":
            raise QueueError(
                f"integration PR #{number} is not ready: "
                + "; ".join(entry.reasons or ["unknown gate"])
            )
        repaired = {key: value for key, value in integration.items() if key != "schema"}
        repaired["conflict"] = None
        repaired["resolved_at"] = utc_now()
        client.comment(number, integration_body(repaired))
        client.comment(
            number,
            queue_state_body(
                "queued",
                entry.head_sha,
                queued_at=utc_now(),
                integration_batch_id=str(integration.get("batch_id") or ""),
            ),
        )
        labels = client.labels(number)
        if client.config.queue_label not in labels:
            client.add_label(number, client.config.queue_label)
        if client.config.blocked_label in labels:
            client.remove_label(number, client.config.blocked_label)
        for source_number in integration.get("pull_requests") or []:
            source_labels = client.labels(int(source_number))
            if client.config.queue_label in source_labels:
                client.remove_label(int(source_number), client.config.queue_label)
        print(f"resumed and queued integration PR #{number} on {entry.head_sha}")
        return
    intent = latest_intent(comments, client.trusted_logins)
    if intent and intent.get("state") == "requested":
        entry = client.snapshot(number, require_marker=False, allow_blocked_label=True)
        if (
            client.config.pipeline.intent_scope == "head"
            and intent.get("requested_head") != entry.head_sha
        ):
            command_refresh_request(client, str(number))
            return
        queue_from_intent(client, entry, intent)
        print(f"resumed and queued PR #{number} on {entry.head_sha}")
        return
    if client.viewer_login().lower() in {
        value.lower() for value in client.trusted_logins
    }:
        command_enqueue(client, str(number))
        return
    raise QueueError(f"PR #{number} has no trusted deploy intent to resume")


def command_enqueue(client: GitHub, selector: str | None) -> None:
    number = client.resolve_pr(selector)
    client.require_actor(client.trusted_logins, "authorize a pull request")
    client.ensure_labels()
    entry = client.snapshot(
        number,
        require_marker=False,
        allow_blocked_label=True,
    )
    if entry.state != "ready":
        raise QueueError(
            f"PR #{number} is not ready to queue: "
            + "; ".join(entry.reasons or ["unknown gate"])
        )

    previous = (
        {
            "head_sha": entry.queued_head_sha,
            "priority_at": entry.priority_at,
            "queued_at": entry.queued_at,
            "state": entry.queue_state,
        }
        if entry.queued_head_sha
        else None
    )
    if (
        client.config.queue_label in entry.labels
        and previous
        and previous.get("head_sha") == entry.head_sha
        and previous.get("state") == "queued"
    ):
        if client.config.blocked_label in entry.labels:
            client.remove_label(number, client.config.blocked_label)
            client.remove_label(number, client.config.queue_label)
            client.add_label(number, client.config.queue_label)
            print(f"re-enabled queued PR #{number} on {entry.head_sha}")
            return
        print(f"PR #{number} is already queued on {entry.head_sha}")
        return

    queued_at = queue_timestamp(
        previous,
        already_queued=client.config.queue_label in entry.labels,
        now=utc_now(),
    )
    body = queue_state_body(
        "queued",
        entry.head_sha,
        queued_at=queued_at,
        priority_at=(
            str(previous.get("priority_at") or "") or None if previous else None
        ),
    )
    client.comment(number, body)
    if client.config.queue_label in entry.labels:
        # Re-adding the label emits a fresh `labeled` event for GitHub-hosted
        # coordinators when a replacement head is reviewed and re-enqueued.
        client.remove_label(number, client.config.queue_label)
    client.add_label(number, client.config.queue_label)
    if client.config.blocked_label in entry.labels:
        client.remove_label(number, client.config.blocked_label)
    print(f"queued PR #{number} on {entry.head_sha}")


def queue_from_intent(
    client: GitHub,
    entry: QueueEntry,
    intent: dict[str, Any],
    *,
    comments: list[dict[str, Any]] | None = None,
    labels: set[str] | None = None,
) -> bool:
    if entry.state != "ready":
        raise QueueError(
            f"PR #{entry.number} is not ready to queue: "
            + "; ".join(entry.reasons or ["unknown gate"])
        )
    intent_id = str(intent.get("intent_id") or "")
    if intent.get("state") != "requested" or not intent_id:
        raise QueueError(f"PR #{entry.number} has no active deploy intent")
    if (
        client.config.pipeline.intent_scope == "head"
        and intent.get("requested_head") != entry.head_sha
    ):
        raise QueueError(
            f"PR #{entry.number} deploy intent is bound to an older head; "
            "run deploybot refresh-request after fresh review"
        )
    current_comments = comments if comments is not None else client.comments(entry.number)
    previous = queue_marker_for_client(client, current_comments)
    current_labels = labels if labels is not None else client.labels(entry.number)
    if (
        client.config.queue_label in current_labels
        and previous
        and previous.get("state") == "queued"
        and previous.get("head_sha") == entry.head_sha
        and previous.get("intent_id") == intent_id
    ):
        entry.queued_head_sha = entry.head_sha
        entry.queued_at = marker_queued_at(previous)
        entry.priority_at = marker_priority_at(previous)
        entry.queue_state = "queued"
        return False
    queued_at = queue_timestamp(
        previous,
        already_queued=client.config.queue_label in entry.labels,
        now=utc_now(),
    )
    priority_at = (
        marker_priority_at(previous)
        or str(intent.get("requested_at") or "")
        or queued_at
    )
    client.comment(
        entry.number,
        queue_state_body(
            "queued",
            entry.head_sha,
            queued_at=queued_at,
            priority_at=priority_at,
            intent_id=intent_id,
        ),
    )
    if client.config.queue_label in current_labels:
        client.remove_label(entry.number, client.config.queue_label)
    client.add_label(entry.number, client.config.queue_label)
    if client.config.blocked_label in current_labels:
        client.remove_label(entry.number, client.config.blocked_label)
    entry.labels = sorted(
        (current_labels - {client.config.blocked_label}) | {client.config.queue_label}
    )
    entry.queued_head_sha = entry.head_sha
    entry.queued_at = queued_at
    entry.priority_at = priority_at
    entry.queue_state = "queued"
    notify(
        client.config.pipeline,
        "queued",
        {
            "repository": client.repository,
            "pull_request": entry.number,
            "head_sha": entry.head_sha,
            "intent_id": intent_id,
        },
    )
    return True


@dataclass
class FreezeResult:
    batch: dict[str, Any] | None
    queue: list[QueueEntry]
    blocked_queue: list[QueueEntry]
    next_batch: list[QueueEntry]
    overlap_groups: list[dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        return {
            "batch": self.batch,
            "blocked_queue": [entry_dict(entry) for entry in self.blocked_queue],
            "next_batch": [entry_dict(entry) for entry in self.next_batch],
            "overlap_groups": self.overlap_groups,
            "queue": [entry_dict(entry) for entry in self.queue],
        }


def freeze_queue(
    client: GitHub,
    *,
    known_entries: list[QueueEntry] | None = None,
    held_numbers: set[int] | None = None,
) -> FreezeResult:
    held_numbers = held_numbers or set()
    if known_entries is None:
        all_entries = [
            entry for entry in client.queue() if entry.number not in held_numbers
        ]
    else:
        queued_numbers = set(client.queued_numbers()) - held_numbers
        entries_by_number = {
            entry.number: entry
            for entry in known_entries
            if entry.number in queued_numbers
        }
        for number in sorted(queued_numbers - entries_by_number.keys()):
            entries_by_number[number] = client.snapshot(number)
        all_entries = sorted(
            entries_by_number.values(),
            key=lambda entry: (
                entry.priority_at or entry.queued_at or "9999",
                entry.number if entry.priority_at else 0,
                entry.queued_at or "9999",
                entry.number,
            ),
        )
    entries, blocked_entries = split_blocked_entries(
        all_entries, client.config.blocked_label
    )
    if not entries:
        return FreezeResult(None, [], blocked_entries, [], [])

    comments = {entry.number: client.comments(entry.number) for entry in entries}
    latest = {
        number: latest_batch_marker(
            values,
            coordinator_logins(client),
        )
        for number, values in comments.items()
    }
    completed = {
        batch_id
        for values in comments.values()
        for batch_id in completed_batch_ids(values, coordinator_logins(client))
    }
    batch = active_batch(entries, latest, completed)
    if batch is None:
        batch = new_batch(entries, frozen_at=utc_now())
    selected = entries_in_batch(entries, batch)
    selected_numbers = {entry.number for entry in selected}
    next_batch = [entry for entry in entries if entry.number not in selected_numbers]

    missing_markers = [
        entry
        for entry in selected
        if latest_batch_marker(
            comments[entry.number],
            coordinator_logins(client),
            batch_id=str(batch["batch_id"]),
        )
        is None
    ]
    if missing_markers:
        body = (
            f"<!-- {BATCH_MARKER_PREFIX} {json.dumps(batch, sort_keys=True)} -->\n"
            f"Frozen merge batch `{batch['batch_id']}`."
        )
        for entry in missing_markers:
            client.comment(entry.number, body)

    return FreezeResult(
        batch,
        selected,
        blocked_entries,
        next_batch,
        overlap_groups(selected),
    )


def complete_batch(
    client: GitHub,
    batch: dict[str, Any],
    unmerged_entries: Iterable[QueueEntry],
) -> None:
    batch_id = str(batch["batch_id"])
    marker = {
        "batch_id": batch_id,
        "completed_at": utc_now(),
        "schema": 1,
    }
    body = (
        f"<!-- {BATCH_COMPLETE_PREFIX} {json.dumps(marker, sort_keys=True)} -->\n"
        f"Completed merge batch `{batch_id}`."
    )
    for entry in unmerged_entries:
        client.comment(entry.number, body)


def command_freeze(client: GitHub, *, json_output: bool) -> FreezeResult:
    result = freeze_queue(client)

    if json_output:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    else:
        if result.batch is None:
            print("no unblocked pull requests to freeze")
        else:
            print(f"frozen batch {result.batch['batch_id']}")
            print_plan(result.queue, json_output=False)
        if result.blocked_queue:
            print(
                "blocked queue: "
                + ", ".join(f"#{entry.number}" for entry in result.blocked_queue)
            )
        if result.next_batch:
            print(
                "next batch: "
                + ", ".join(f"#{entry.number}" for entry in result.next_batch)
            )
    return result


def command_block(client: GitHub, selector: str | None, reason: str) -> None:
    number = client.resolve_pr(selector)
    client.ensure_labels()
    entry = client.snapshot(number, require_marker=False, allow_blocked_label=True)
    comments = client.comments(number)
    previous = queue_marker_for_client(client, comments)
    if (
        not previous
        or previous.get("state") != "queued"
        or previous.get("head_sha") != entry.head_sha
    ):
        raise QueueError(f"PR #{number} does not have current queue authorization")
    labels = client.labels(number)
    if client.config.queue_label not in labels:
        raise QueueError(f"PR #{number} is not in the merge queue")
    client.comment(
        number,
        queue_state_body(
            "blocked",
            entry.head_sha,
            queued_at=str(previous.get("queued_at") or "") or None,
            priority_at=str(previous.get("priority_at") or "") or None,
            reason=reason,
            intent_id=str(previous.get("intent_id") or "") or None,
        ),
    )
    record_repair(
        client,
        entry,
        latest_intent(comments, client.trusted_logins),
        reason,
    )
    print(f"blocked PR #{number}: {reason}")


def command_unblock(client: GitHub, selector: str | None) -> None:
    number = client.resolve_pr(selector)
    entry = client.snapshot(number, require_marker=False, allow_blocked_label=True)
    previous = queue_marker_for_client(client, client.comments(number))
    if (
        not previous
        or previous.get("state") != "blocked"
        or previous.get("head_sha") != entry.head_sha
    ):
        raise QueueError(f"PR #{number} does not have a current trusted block")
    client.comment(
        number,
        queue_state_body(
            "queued",
            entry.head_sha,
            queued_at=str(previous.get("queued_at") or "") or None,
            priority_at=str(previous.get("priority_at") or "") or None,
            intent_id=str(previous.get("intent_id") or "") or None,
        ),
    )
    labels = client.labels(number)
    if client.config.blocked_label in labels:
        client.remove_label(number, client.config.blocked_label)
        if client.config.queue_label in labels:
            client.remove_label(number, client.config.queue_label)
            client.add_label(number, client.config.queue_label)
    print(f"unblocked PR #{number}")


def command_dequeue(client: GitHub, selector: str | None, reason: str) -> None:
    number = client.resolve_pr(selector)
    entry = client.snapshot(number, require_marker=False, allow_blocked_label=True)
    previous = queue_marker_for_client(client, client.comments(number))
    labels = client.labels(number)
    if client.config.blocked_label not in labels:
        client.add_label(number, client.config.blocked_label)
        labels.add(client.config.blocked_label)
    client.comment(
        number,
        queue_state_body(
            "dequeued",
            entry.head_sha,
            queued_at=(
                str(previous.get("queued_at") or "") or None if previous else None
            ),
            priority_at=(
                str(previous.get("priority_at") or "") or None if previous else None
            ),
            reason=reason,
            intent_id=(
                str(previous.get("intent_id") or "") or None if previous else None
            ),
        ),
    )
    for label in (client.config.queue_label, client.config.blocked_label):
        if label in labels:
            client.remove_label(number, label)
    print(f"removed PR #{number} from the merge queue")


def command_merge(
    client: GitHub,
    selector: str | None,
    batch_id: str,
    *,
    emit: bool = True,
    frozen_entry: QueueEntry | None = None,
    frozen_batch: dict[str, Any] | None = None,
    active_numbers: set[int] | None = None,
) -> str:
    if frozen_entry is None or frozen_batch is None:
        require_running_pipeline(client)
    number = frozen_entry.number if frozen_entry is not None else client.resolve_pr(selector)
    batch = frozen_batch
    if batch is not None and str(batch.get("batch_id") or "") != batch_id:
        raise QueueError(f"frozen context does not match batch {batch_id}")
    if batch is None:
        batch = latest_batch_marker(
            client.comments(number),
            coordinator_logins(client),
            batch_id=batch_id,
        )
    if batch is None:
        raise QueueError(f"PR #{number} has no trusted marker for batch {batch_id}")
    frozen = {int(value) for value in batch.get("pull_requests") or []}
    if number not in frozen:
        raise QueueError(f"PR #{number} is not a member of batch {batch_id}")
    if frozen_entry is None:
        entries = client.queue()
        frozen_entries = [value for value in entries if value.number in frozen]
        entry = next((value for value in frozen_entries if value.number == number), None)
        current_numbers = {value.number for value in frozen_entries}
    else:
        entry = frozen_entry
        current_numbers = active_numbers or {number}
    if entry is None:
        raise QueueError(f"PR #{number} is not in the merge queue")
    if entry.state != "ready":
        raise QueueError(
            f"PR #{number} is not merge-ready: "
            + "; ".join(entry.reasons or ["unknown gate"])
        )

    expected_head = str((batch.get("heads") or {}).get(str(number)) or "")
    if expected_head != entry.head_sha:
        raise QueueError(f"PR #{number} changed after batch {batch_id} was frozen")
    if entry.queued_at and str(batch.get("frozen_at") or "") < entry.queued_at:
        raise QueueError(f"batch {batch_id} predates the current queue authorization")

    peers = batch_overlap_peers(batch, number, current_numbers)
    if peers:
        peers = ", ".join(f"#{value}" for value in peers)
        raise QueueError(
            f"PR #{number} overlaps {peers}; create one cumulative integration PR"
        )

    expected_dependencies = sorted(
        int(value) for value in (batch.get("dependencies") or {}).get(str(number), [])
    )
    if entry.dependencies != expected_dependencies:
        raise QueueError(f"PR #{number} dependencies changed after batch freeze")
    missing_dependencies = [
        value
        for value in expected_dependencies
        if not client.dependency_is_merged(value)
    ]
    if missing_dependencies:
        raise QueueError(
            "unmerged dependencies: "
            + ", ".join(f"#{value}" for value in missing_dependencies)
        )

    fresh = client.snapshot(
        number,
        known_source_paths=entry.source_paths,
        known_generated_paths=entry.generated_paths,
    )
    if fresh.state != "ready":
        raise QueueError(
            f"PR #{number} changed before merge: "
            + "; ".join(fresh.reasons or ["unknown gate"])
        )
    if str((batch.get("heads") or {}).get(str(number)) or "") != fresh.head_sha:
        raise QueueError(f"PR #{number} changed after batch {batch_id} was frozen")
    if fresh.dependencies != expected_dependencies:
        raise QueueError(f"PR #{number} dependencies changed before merge")
    if fresh.queued_at and str(batch.get("frozen_at") or "") < fresh.queued_at:
        raise QueueError(f"batch {batch_id} predates the current queue authorization")

    merge_sha = client.merge(
        number,
        fresh.head_sha,
        authorization_entry=fresh,
    )
    merged_comments = client.comments(number)
    intent = latest_intent(merged_comments, client.trusted_logins)
    integration = latest_payload(
        merged_comments,
        INTEGRATION_MARKER,
        coordinator_logins(client),
    )
    if intent and intent.get("provider") and intent.get("thread_id"):
        client.record_thread(
            ThreadRecord(
                provider=str(intent["provider"]),
                thread_id=str(intent["thread_id"]),
                phase="merged",
                updated_at=utc_now(),
                pull_request=number,
                url=str(intent.get("thread_url") or "") or None,
            )
        )
    notify(
        client.config.pipeline,
        "merged",
        {
            "repository": client.repository,
            "pull_request": number,
            "head_sha": fresh.head_sha,
            "merge_sha": merge_sha,
            "intent_id": (intent or {}).get("intent_id"),
        },
    )
    if integration:
        for source_number in integration.get("pull_requests") or []:
            source_intent = latest_intent(
                client.comments(int(source_number)), client.trusted_logins
            )
            if (
                source_intent
                and source_intent.get("provider")
                and source_intent.get("thread_id")
            ):
                client.record_thread(
                    ThreadRecord(
                        provider=str(source_intent["provider"]),
                        thread_id=str(source_intent["thread_id"]),
                        phase="merged",
                        updated_at=utc_now(),
                        pull_request=int(source_number),
                        url=str(source_intent.get("thread_url") or "") or None,
                    )
                )
    if emit:
        print(f"merged PR #{number} as {merge_sha}")
    return merge_sha


def mergeability_is_pending(entry: QueueEntry) -> bool:
    reasons = entry.reasons or []
    return entry.state == "waiting" and reasons == [
        "GitHub is still computing mergeability"
    ]


def drain_frozen_batch(
    client: GitHub, frozen: FreezeResult
) -> tuple[dict[str, Any], list[QueueEntry]]:
    if frozen.batch is None:  # pragma: no cover - caller owns this boundary.
        raise QueueError("cannot drain an empty frozen batch")
    batch_id = str(frozen.batch["batch_id"])
    overlap_numbers = {
        number for group in frozen.overlap_groups for number in group["pull_requests"]
    }
    pending = {entry.number: entry for entry in frozen.queue}
    merged: list[dict[str, Any]] = []
    waiting: list[dict[str, Any]] = []
    retry_count = 0

    while pending:
        progress = False
        retryable = False
        for number, entry in list(pending.items()):
            if number in overlap_numbers:
                continue
            if entry.state != "ready":
                retryable = retryable or mergeability_is_pending(entry)
                continue
            if any(dependency in pending for dependency in entry.dependencies):
                continue
            try:
                merge_sha = command_merge(
                    client,
                    str(number),
                    batch_id,
                    emit=False,
                    frozen_entry=entry,
                    frozen_batch=frozen.batch,
                    active_numbers=set(pending),
                )
            except QueueError as error:
                if "GitHub is still computing mergeability" in str(error):
                    retryable = True
                    continue
                waiting.append({"number": number, "reason": str(error)})
                del pending[number]
                progress = True
                continue
            merged.append({"number": number, "merge_sha": merge_sha})
            del pending[number]
            progress = True
        if progress:
            retry_count = 0
            continue
        if retryable and retry_count < MERGEABILITY_RETRIES:
            time.sleep(min(2**retry_count, 5))
            retry_count += 1
            for number in list(pending):
                if number not in overlap_numbers:
                    pending[number] = client.snapshot(number)
            continue
        break

    for number, entry in pending.items():
        if number in overlap_numbers:
            continue
        waiting.append(
            {
                "number": number,
                "reason": "; ".join(entry.reasons or [])
                or "dependency order could not be satisfied",
            }
        )
    result: dict[str, Any] = {
        "batch_id": batch_id,
        "merged": merged,
        "integration_required": frozen.overlap_groups,
        "waiting": waiting,
        "next_batch": [entry.number for entry in frozen.next_batch],
    }
    merged_numbers = {value["number"] for value in merged}
    unmerged = [entry for entry in frozen.queue if entry.number not in merged_numbers]
    complete_batch(client, frozen.batch, unmerged)
    return result, unmerged


def command_drain(
    client: GitHub,
    *,
    json_output: bool,
    emit: bool = True,
    initial_frozen: FreezeResult | None = None,
) -> dict[str, Any]:
    require_running_pipeline(client)
    batch_ids: list[str] = []
    merged: list[dict[str, Any]] = []
    waiting_by_number: dict[int, dict[str, Any]] = {}
    integration_by_members: dict[tuple[int, ...], dict[str, Any]] = {}
    next_batch: list[int] = []

    while True:
        frozen = initial_frozen or freeze_queue(client)
        initial_frozen = None
        if frozen.batch is None:
            break
        batch_id = str(frozen.batch["batch_id"])
        if batch_id in batch_ids:
            break
        batch_ids.append(batch_id)
        batch_result, _ = drain_frozen_batch(client, frozen)
        merged.extend(batch_result["merged"])
        for value in batch_result["merged"]:
            waiting_by_number.pop(int(value["number"]), None)
        for value in batch_result["waiting"]:
            waiting_by_number[int(value["number"])] = value
        for group in batch_result["integration_required"]:
            key = tuple(int(value) for value in group["pull_requests"])
            integration_by_members[key] = group
        next_batch = list(batch_result["next_batch"])
        if not next_batch:
            break

    result = {
        "batch_id": batch_ids[-1] if batch_ids else None,
        "batch_ids": batch_ids,
        "merged": merged,
        "integration_required": list(integration_by_members.values()),
        "waiting": list(waiting_by_number.values()),
        "next_batch": next_batch,
    }
    if not emit:
        return result
    if json_output:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        for value in merged:
            print(f"merged PR #{value['number']} as {value['merge_sha']}")
        for group in result["integration_required"]:
            numbers = ", ".join(f"#{value}" for value in group["pull_requests"])
            print(f"integration required: {numbers}")
        for value in result["waiting"]:
            print(f"waiting: PR #{value['number']} - {value['reason']}")
        if next_batch:
            print("next batch: " + ", ".join(f"#{number}" for number in next_batch))
        if not batch_ids:
            print("merge queue is empty")
    return result


def command_integrate(client: GitHub, *, all_entries: bool) -> dict[str, Any]:
    require_running_pipeline(client)
    frozen = freeze_queue(client)
    if frozen.batch is None:
        raise QueueError("merge queue is empty")
    selected = frozen.queue
    if not all_entries:
        members = {
            number
            for group in frozen.overlap_groups
            for number in group["pull_requests"]
        }
        selected = [entry for entry in selected if entry.number in members]
    if len(selected) < 2:
        raise QueueError("an integration pull request needs at least two queue items")
    result = client.create_integration_pull_request(
        batch=frozen.batch,
        entries=selected,
    )
    if result.get("conflict"):
        conflicting_number = int(result["conflict"]["number"])
        entry = next(value for value in selected if value.number == conflicting_number)
        intent = latest_intent(client.comments(entry.number), client.trusted_logins)
        record_repair(
            client,
            entry,
            intent,
            "integration branch conflict: " + str(result["conflict"]["reason"]),
        )
    notify(
        client.config.pipeline,
        "integration-created",
        {"repository": client.repository, **result},
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def command_follow(
    client: GitHub,
    *,
    timeout_seconds: int,
    poll_seconds: int,
    json_output: bool,
    emit: bool = True,
) -> dict[str, Any]:
    try:
        result = follow_release(
            client,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )
    except QueueError as error:
        if client.config.pipeline.pause_on_failure:
            client.set_pipeline_control(
                "paused", "release workflow dispatch failed: " + str(error)
            )
        raise
    if (
        result["state"] in {"ci-failed", "deploy-failed", "verify-failed"}
        and client.config.pipeline.pause_on_failure
    ):
        client.set_pipeline_control(
            "paused",
            f"{result['state']} on {result['main_sha']}",
        )
    if result["state"] == "verified":
        for record in client.thread_records():
            if record.get("phase") != "merged":
                continue
            client.record_thread(
                ThreadRecord(
                    provider=str(record["provider"]),
                    thread_id=str(record["thread_id"]),
                    phase="completed",
                    updated_at=utc_now(),
                    title=str(record.get("title") or "") or None,
                    branch=str(record.get("branch") or "") or None,
                    pull_request=record.get("pull_request"),
                    url=str(record.get("url") or "") or None,
                )
            )
        notify(
            client.config.pipeline,
            "verified",
            {"repository": client.repository, **result},
        )
    if not emit:
        return result
    if json_output:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"release {result['state']} on {result['main_sha']}")
    return result


def release_follow_needed(client: GitHub) -> bool:
    current = release_state(
        main_sha=client.base_sha(),
        runs=client.workflow_runs(),
        config=client.config.pipeline,
    )
    if current["state"] == "testing":
        # A repository with no current main CI has no release to follow. An
        # active or queued run is returned as latest_ci and should keep its
        # release owner.
        return current.get("latest_ci") is not None
    if current["state"] == "verified":
        # Verification can finish just before the original worker is replaced.
        # Only revisit it when merged thread records still need completion.
        return any(
            record.get("phase") == "merged" for record in client.thread_records()
        )
    return True


def should_settle_batch(client: GitHub, entries: list[QueueEntry]) -> bool:
    has_ready = any(
        client.config.queue_label in entry.labels and entry.state == "ready"
        for entry in entries
    )
    has_near_ready = any(
        client.config.queue_label not in entry.labels
        and client.config.blocked_label not in entry.labels
        and entry.state == "waiting"
        for entry in entries
    )
    return has_ready and has_near_ready


def near_ready_overlap_holds(
    client: GitHub, entries: list[QueueEntry]
) -> dict[int, list[int]]:
    """Keep ready overlap peers together while another active intent settles."""
    if client.config.integration.mode != "overlap":
        return {}
    ready = {
        entry.number: entry
        for entry in entries
        if entry.state == "ready"
        and client.config.queue_label in entry.labels
        and client.config.blocked_label not in entry.labels
    }
    waiting = {
        entry.number: entry
        for entry in entries
        if entry.state == "waiting"
        and client.config.queue_label not in entry.labels
        and client.config.blocked_label not in entry.labels
    }
    if not ready or not waiting:
        return {}

    def load_paths(entry: QueueEntry) -> tuple[int, list[str], list[str]]:
        source_paths, generated_paths = client.changed_paths(entry.number)
        return entry.number, source_paths, generated_paths

    values = list(waiting.values())
    with ThreadPoolExecutor(
        max_workers=min(client.config.pipeline.promotion_workers, len(values))
    ) as executor:
        for number, source_paths, generated_paths in executor.map(load_paths, values):
            waiting[number].source_paths = sorted(set(source_paths))
            waiting[number].generated_paths = sorted(set(generated_paths))

    waiting_numbers = set(waiting)
    holds: dict[int, list[int]] = {}
    for group in overlap_groups([*ready.values(), *waiting.values()]):
        members = {int(value) for value in group["pull_requests"]}
        waiting_peers = sorted(members & waiting_numbers)
        if not waiting_peers:
            continue
        for number in sorted(members & ready.keys()):
            entry = ready[number]
            comments = client.comments(number)
            marker = latest_batch_marker(comments, coordinator_logins(client))
            completed = completed_batch_ids(comments, coordinator_logins(client))
            if active_batch([entry], {number: marker}, completed) is not None:
                continue
            holds[number] = waiting_peers
    return holds


def command_react(
    client: GitHub,
    *,
    follow: bool,
    timeout_seconds: int,
    dispatch_ci: bool = False,
) -> dict[str, Any]:
    control = client.pipeline_control()
    if control.get("state") == "paused":
        result = {"state": "paused", "reason": control.get("reason")}
        print(json.dumps(result, indent=2, sort_keys=True))
        return result
    promoted_integrations = promote_integrations(client)
    captured_entries: list[QueueEntry] = []
    promoted = command_promote(
        client,
        emit=False,
        captured_entries=captured_entries,
    )
    if (
        client.config.pipeline.batch_settle_seconds
        and should_settle_batch(client, captured_entries)
    ):
        time.sleep(client.config.pipeline.batch_settle_seconds)
        settled_entries: list[QueueEntry] = []
        settled = command_promote(
            client,
            emit=False,
            captured_entries=settled_entries,
        )
        promoted["promoted"] = list(
            dict.fromkeys(promoted["promoted"] + settled["promoted"])
        )
        promoted["blocked"] = promoted["blocked"] + settled["blocked"]
        promoted["waiting"] = settled["waiting"]
        captured_entries = settled_entries
    overlap_holds = near_ready_overlap_holds(client, captured_entries)
    promoted["held"] = [
        {
            "number": number,
            "overlapping_waiting": waiting,
            "reason": "waiting for overlapping deploy intents to finish gates",
        }
        for number, waiting in overlap_holds.items()
    ]
    frozen = freeze_queue(
        client,
        known_entries=captured_entries,
        held_numbers=set(overlap_holds),
    )
    integrations: list[dict[str, Any]] = []
    if (
        frozen.batch is not None
        and client.config.integration.mode == "all"
        and not promoted_integrations
    ):
        integrations.append(
            client.create_integration_pull_request(
                batch=frozen.batch,
                entries=frozen.queue,
            )
        )
        drained: dict[str, Any] = {
            "batch_id": frozen.batch["batch_id"],
            "merged": [],
            "waiting": [],
            "integration_required": frozen.overlap_groups,
            "next_batch": [],
        }
    else:
        drained = command_drain(
            client,
            json_output=False,
            emit=False,
            initial_frozen=frozen,
        )
        if (
            frozen.batch is not None
            and client.config.integration.mode == "overlap"
            and frozen.overlap_groups
        ):
            overlap_members = {
                number
                for group in frozen.overlap_groups
                for number in group["pull_requests"]
            }
            overlap_entries = [
                entry for entry in frozen.queue if entry.number in overlap_members
            ]
            integrations.append(
                client.create_integration_pull_request(
                    batch=frozen.batch,
                    entries=overlap_entries,
                )
            )
    dispatched_ci: list[dict[str, Any]] = []
    if dispatch_ci and drained.get("merged"):
        try:
            dispatched_ci = client.dispatch_ci_workflows()
        except QueueError as error:
            if client.config.pipeline.pause_on_failure:
                client.set_pipeline_control(
                    "paused", "post-merge CI dispatch failed: " + str(error)
                )
            raise
    release: dict[str, Any] | None = None
    should_follow = bool(drained.get("merged"))
    if follow and not should_follow:
        # The worker that performed the merge may be replaced while waiting for
        # CI. Let a later event take over only when a release actually remains;
        # an idle all-mode integration batch must not occupy the coordinator for
        # the full follow timeout.
        should_follow = release_follow_needed(client)
    if follow and should_follow:
        release = command_follow(
            client,
            timeout_seconds=timeout_seconds,
            poll_seconds=10,
            json_output=False,
            emit=False,
        )
    result = {
        "state": "complete",
        "promoted": promoted,
        "promoted_integrations": promoted_integrations,
        "drain": drained,
        "dispatched_ci": dispatched_ci,
        "integrations": integrations,
        "release": release,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def command_control(client: GitHub, *, state: str, reason: str | None) -> None:
    client.set_pipeline_control(state, reason)
    print(f"DeployBot pipeline is {state}")


def delivery_metrics(client: GitHub, *, limit: int) -> dict[str, Any]:
    deploy_runs = [
        run
        for run in client.workflow_runs()
        if str(run.get("name") or "") in set(client.config.pipeline.deploy_workflows)
        and run.get("conclusion") == "success"
    ]
    samples: list[dict[str, Any]] = []
    for pull in client.recent_merged_pull_requests(limit):
        number = int(pull["number"])
        comments = client.comments(number)
        intent = latest_intent(comments, client.trusted_logins)
        marker = queue_marker_for_client(client, comments)
        merged_at = str(pull.get("merged_at") or "") or None
        merge_sha = str(pull.get("merge_commit_sha") or "")
        live_at: str | None = None
        if merge_sha:
            for run in sorted(
                deploy_runs, key=lambda item: str(item.get("updated_at") or "")
            ):
                deployed_sha = str(run.get("head_sha") or "")
                if deployed_sha and client.is_ancestor(merge_sha, deployed_sha):
                    live_at = str(run.get("updated_at") or "") or None
                    break
        requested_at = str((intent or {}).get("requested_at") or "") or None
        queued_at = str((marker or {}).get("queued_at") or "") or None
        samples.append(
            {
                "pull_request": number,
                "requested_at": requested_at,
                "queued_at": queued_at,
                "merged_at": merged_at,
                "live_at": live_at,
                "request_to_queue_seconds": seconds_between(requested_at, queued_at),
                "queue_to_merge_seconds": seconds_between(queued_at, merged_at),
                "merge_to_live_seconds": seconds_between(merged_at, live_at),
                "request_to_live_seconds": seconds_between(requested_at, live_at),
            }
        )
    return summarize_metrics(samples)


def print_doctor(rows: list[dict[str, Any]], *, json_output: bool) -> None:
    if json_output:
        print(json.dumps(rows, indent=2, sort_keys=True))
        return
    symbols = {"ok": "✓", "warn": "⚠", "fail": "✗"}
    for value in rows:
        line = f"{symbols[value['status']]} {value['check']}: {value['detail']}"
        if value.get("hint"):
            line += f" — {value['hint']}"
        print(line)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument(
        "--config",
        help="policy file (defaults to .mergequeue.toml or MERGE_QUEUE_CONFIG)",
    )
    parser.add_argument(
        "--repository",
        help="GitHub repository in owner/name form (defaults to the current repo)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    initialize = subparsers.add_parser("init", help="create a safe starter policy")
    initialize.add_argument("--force", action="store_true")
    subparsers.add_parser("ensure-labels", help="create or refresh queue labels")
    inspect = subparsers.add_parser(
        "inspect", help="evaluate one PR without queueing it"
    )
    inspect.add_argument("pr", nargs="?", help="PR number, URL, or branch")
    inspect.add_argument("--json", action="store_true", dest="json_output")
    plan = subparsers.add_parser("plan", help="show the current ordered queue")
    plan.add_argument("--json", action="store_true", dest="json_output")
    status = subparsers.add_parser(
        "status", help="show threads, PRs, queue, CI, and deployments"
    )
    status.add_argument("--json", action="store_true", dest="json_output")
    doctor = subparsers.add_parser("doctor", help="diagnose installation and policy")
    doctor.add_argument("--json", action="store_true", dest="json_output")
    thread = subparsers.add_parser("thread", help="record cross-client thread state")
    thread_commands = thread.add_subparsers(dest="thread_command", required=True)
    thread_update = thread_commands.add_parser("update")
    thread_update.add_argument("--provider", required=True)
    thread_update.add_argument("--thread-id", required=True)
    thread_update.add_argument("--phase", required=True, choices=sorted(THREAD_PHASES))
    thread_update.add_argument("--title")
    thread_update.add_argument("--branch")
    thread_update.add_argument("--pr", type=int, dest="pull_request")
    thread_update.add_argument("--url")
    freeze = subparsers.add_parser("freeze", help="persist one exact queue pass")
    freeze.add_argument("--json", action="store_true", dest="json_output")
    drain = subparsers.add_parser(
        "drain", help="freeze and merge every independent ready queue item"
    )
    drain.add_argument("--json", action="store_true", dest="json_output")

    request = subparsers.add_parser(
        "request", help="record the user's durable deploy intent"
    )
    request.add_argument("pr", nargs="?", help="PR number, URL, or branch")
    request.add_argument("--provider")
    request.add_argument("--thread-id")
    request.add_argument("--thread-url")
    cancel_request = subparsers.add_parser("cancel-request")
    cancel_request.add_argument("pr", nargs="?", help="PR number, URL, or branch")
    refresh_request = subparsers.add_parser("refresh-request")
    refresh_request.add_argument("pr", nargs="?", help="PR number, URL, or branch")
    subparsers.add_parser("promote", help="promote ready deploy requests into queue")
    react = subparsers.add_parser("react", help="event-driven promote and drain worker")
    react.add_argument("--follow", action="store_true")
    react.add_argument(
        "--dispatch-ci",
        action="store_true",
        help="dispatch configured CI once after this worker merges a batch",
    )
    react.add_argument("--timeout", type=int, default=1800)
    integrate = subparsers.add_parser("integrate", help="scaffold a cumulative PR")
    integrate.add_argument("--all", action="store_true", dest="all_entries")
    follow = subparsers.add_parser(
        "follow", help="follow exact main through deployment"
    )
    follow.add_argument("--timeout", type=int, default=1800)
    follow.add_argument("--poll", type=int, default=10)
    follow.add_argument("--json", action="store_true", dest="json_output")
    pause = subparsers.add_parser(
        "pause", help="pause merging after a delivery failure"
    )
    pause.add_argument("--reason", required=True)
    subparsers.add_parser("unpause", help="resume a paused delivery pipeline")
    metrics = subparsers.add_parser("metrics", help="show delivery timing percentiles")
    metrics.add_argument("--limit", type=int, default=25)
    metrics.add_argument("--json", action="store_true", dest="json_output")

    for name in ("enqueue", "unblock", "resume"):
        command = subparsers.add_parser(name)
        command.add_argument("pr", nargs="?", help="PR number, URL, or branch")

    merge = subparsers.add_parser("merge")
    merge.add_argument("pr", nargs="?", help="PR number, URL, or branch")
    merge.add_argument(
        "--batch",
        required=True,
        help="trusted batch identifier returned by the freeze command",
    )

    block = subparsers.add_parser("block")
    block.add_argument("pr", nargs="?", help="PR number, URL, or branch")
    block.add_argument("--reason", required=True)

    dequeue = subparsers.add_parser("dequeue")
    dequeue.add_argument("pr", nargs="?", help="PR number, URL, or branch")
    dequeue.add_argument("--reason", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "init":
            path = initialize_config(arguments.config, force=arguments.force)
            print(f"created {path}")
            return 0
        if arguments.command == "doctor":
            rows = diagnose(
                config_path=arguments.config,
                repository=arguments.repository,
            )
            print_doctor(rows, json_output=arguments.json_output)
            return 1 if any(value["status"] == "fail" for value in rows) else 0
        config = load_config(arguments.config)
        client = GitHub(config, arguments.repository)
        if arguments.command == "ensure-labels":
            client.ensure_labels()
            print("merge queue labels are ready")
        elif arguments.command == "inspect":
            command_inspect(client, arguments.pr, json_output=arguments.json_output)
        elif arguments.command == "plan":
            print_plan(client.queue(), json_output=arguments.json_output)
        elif arguments.command == "status":
            print_pipeline_status(
                pipeline_status(client), json_output=arguments.json_output
            )
        elif arguments.command == "thread":
            command_thread_update(
                client,
                provider=arguments.provider,
                thread_id=arguments.thread_id,
                phase=arguments.phase,
                title=arguments.title,
                branch=arguments.branch,
                pull_request=arguments.pull_request,
                url=arguments.url,
            )
        elif arguments.command == "freeze":
            command_freeze(client, json_output=arguments.json_output)
        elif arguments.command == "drain":
            command_drain(client, json_output=arguments.json_output)
        elif arguments.command == "enqueue":
            command_enqueue(client, arguments.pr)
        elif arguments.command == "request":
            command_request(
                client,
                arguments.pr,
                provider=arguments.provider,
                thread_id=arguments.thread_id,
                thread_url=arguments.thread_url,
            )
        elif arguments.command == "cancel-request":
            command_cancel_request(client, arguments.pr)
        elif arguments.command == "refresh-request":
            command_refresh_request(client, arguments.pr)
        elif arguments.command == "promote":
            command_promote(client)
        elif arguments.command == "resume":
            command_resume(client, arguments.pr)
        elif arguments.command == "react":
            if arguments.timeout < 1:
                raise QueueError("--timeout must be positive")
            command_react(
                client,
                follow=arguments.follow,
                timeout_seconds=arguments.timeout,
                dispatch_ci=arguments.dispatch_ci,
            )
        elif arguments.command == "integrate":
            command_integrate(client, all_entries=arguments.all_entries)
        elif arguments.command == "follow":
            if arguments.timeout < 1 or arguments.poll < 1:
                raise QueueError("--timeout and --poll must be positive")
            command_follow(
                client,
                timeout_seconds=arguments.timeout,
                poll_seconds=arguments.poll,
                json_output=arguments.json_output,
            )
        elif arguments.command == "pause":
            command_control(client, state="paused", reason=arguments.reason)
        elif arguments.command == "unpause":
            command_control(client, state="running", reason=None)
        elif arguments.command == "metrics":
            if arguments.limit < 1:
                raise QueueError("--limit must be positive")
            value = delivery_metrics(client, limit=arguments.limit)
            if arguments.json_output:
                print(json.dumps(value, indent=2, sort_keys=True))
            else:
                print(f"delivery samples: {value['sample_count']}")
                for name, summary in value.items():
                    if name.endswith("_seconds"):
                        print(
                            f"{name}: p50={summary['p50']}s "
                            f"p95={summary['p95']}s max={summary['max']}s"
                        )
        elif arguments.command == "block":
            command_block(client, arguments.pr, arguments.reason)
        elif arguments.command == "unblock":
            command_unblock(client, arguments.pr)
        elif arguments.command == "dequeue":
            command_dequeue(client, arguments.pr, arguments.reason)
        elif arguments.command == "merge":
            command_merge(client, arguments.pr, arguments.batch)
        else:
            raise QueueError(f"unknown command: {arguments.command}")
    except (ConfigError, OSError, QueueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
