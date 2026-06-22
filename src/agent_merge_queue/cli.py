#!/usr/bin/env python3
"""Manage a GitHub-backed, agent-owned pull-request merge queue."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import quote

from . import __version__
from .config import ConfigError, QueueConfig, initialize_config, load_config
from .doctor import diagnose
from .pipeline import (
    follow_release,
    http_verifications,
    notify,
    release_state,
    seconds_between,
    summarize_metrics,
)
from .records import (
    INTEGRATION_MARKER,
    REPAIR_MARKER,
    RELEASE_WATERMARK_MARKER,
    THREAD_PHASES,
    DeploymentNotificationRecord,
    PullRequestThreadOwnerRecord,
    ThreadRecord,
    control_body,
    deployment_notification_body,
    integration_body,
    intent_body,
    latest_intent,
    latest_control,
    latest_release_repair,
    latest_deployment_notifications,
    latest_payload,
    latest_thread_records,
    parse_time,
    pull_request_thread_owner_body,
    pull_request_thread_owners,
    repair_body,
    release_repair_body,
    release_watermark_body,
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
RELEASE_REPAIR_LEASE_PREFIX = "DeployBot release repair lease v1 "
INTEGRATION_SEAL_PREFIX = "DeployBot integration seal v1 "
PR_OPENING_PHASES = {"pr-draft", "pr-review", "ready"}
FAILED_CHECK_STATES = {
    "ACTION_REQUIRED",
    "CANCELLED",
    "ERROR",
    "FAILURE",
    "STALE",
    "STARTUP_FAILURE",
    "TIMED_OUT",
}
PASSED_CHECK_STATES = {"NEUTRAL", "SKIPPED", "SUCCESS"}
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


def check_identity(check: dict[str, Any], name: str) -> tuple[str, str]:
    typename = str(check.get("__typename") or "")
    if not typename:
        typename = "StatusContext" if check.get("context") else "CheckRun"
    return (typename, name)


def check_states(checks: Iterable[dict[str, Any]]) -> dict[str, str]:
    grouped: dict[tuple[str, tuple[str, str]], tuple[str, int, str]] = {}
    for index, check in enumerate(checks):
        name = str(check.get("name") or check.get("context") or "")
        if not name:
            continue
        timestamp = str(
            check.get("startedAt")
            or check.get("started_at")
            or check.get("createdAt")
            or check.get("created_at")
            or check.get("completedAt")
            or check.get("completed_at")
            or ""
        )
        if timestamp.startswith("0001-01-01"):
            timestamp = ""
        state = normalize_check_state(check)
        # A newly queued run may not have a timestamp yet. Fail closed instead
        # of letting an older success hide that pending rerun.
        order = (
            "\uffff" if not timestamp and state not in PASSED_CHECK_STATES else timestamp
        )
        candidate = (order, index, state)
        key = (name, check_identity(check, name))
        if key not in grouped or candidate[:2] > grouped[key][:2]:
            grouped[key] = candidate

    states_by_name: dict[str, list[str]] = {}
    for (name, _identity), value in grouped.items():
        states_by_name.setdefault(name, []).append(value[2])

    result: dict[str, str] = {}
    for name, states in states_by_name.items():
        if any(state in FAILED_CHECK_STATES for state in states):
            result[name] = "failed"
        elif all(state in PASSED_CHECK_STATES for state in states):
            result[name] = "passed"
        else:
            result[name] = "pending"
    return result


def merge_known_check_states(
    observed: dict[str, str],
    known: dict[str, str] | None,
) -> dict[str, str]:
    result = dict(observed)
    for name, state in (known or {}).items():
        if result.get(name) == "failed" and state != "failed":
            continue
        result[name] = state
    return result


def latest_exact_workflow_runs(
    runs: Iterable[dict[str, Any]],
    names: Iterable[str],
    *,
    head_sha: str,
    event: str = "workflow_dispatch",
) -> dict[str, dict[str, Any]]:
    configured = set(names)
    latest: dict[str, dict[str, Any]] = {}
    for run in runs:
        name = str(run.get("name") or "")
        if (
            name not in configured
            or str(run.get("head_sha") or "") != head_sha
            or str(run.get("event") or "") != event
        ):
            continue
        previous = latest.get(name)
        key = (str(run.get("created_at") or ""), int(run.get("id") or 0))
        previous_key = (
            str((previous or {}).get("created_at") or ""),
            int((previous or {}).get("id") or 0),
        )
        if previous is None or key > previous_key:
            latest[name] = run
    return latest


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


def overlap_groups(
    entries: Iterable["QueueEntry"], *, include_generated: bool = True
) -> list[dict[str, Any]]:
    values = list(entries)
    adjacency: dict[int, set[int]] = {entry.number: set() for entry in values}
    shared_source: dict[tuple[int, int], set[str]] = {}
    shared_generated: dict[tuple[int, int], set[str]] = {}
    for index, left in enumerate(values):
        for right in values[index + 1 :]:
            source_paths = set(left.source_paths) & set(right.source_paths)
            generated_paths = (
                set(left.generated_paths) & set(right.generated_paths)
                if include_generated
                else set()
            )
            if not source_paths and not generated_paths:
                continue
            pair = tuple(sorted((left.number, right.number)))
            shared_source[pair] = source_paths
            shared_generated[pair] = generated_paths
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
        source_paths: set[str] = set()
        generated_paths: set[str] = set()
        for pair, pair_paths in shared_source.items():
            if pair[0] in component and pair[1] in component:
                source_paths.update(pair_paths)
                generated_paths.update(shared_generated[pair])
        groups.append(
            {
                "pull_requests": sorted(component),
                "source_paths": sorted(source_paths),
                "generated_paths": sorted(generated_paths),
            }
        )
    return groups


def batch_fingerprint(entries: Iterable["QueueEntry"]) -> str:
    material = [
        {
            "head_sha": entry.head_sha,
            "number": entry.number,
            "dependencies": entry.dependencies,
            "source_paths": entry.source_paths,
            "generated_paths": entry.generated_paths,
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
        "generated_paths": {
            str(entry.number): entry.generated_paths for entry in entries
        },
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
    generated_by_pr = batch.get("generated_paths") or {}
    target_paths = set(paths_by_pr.get(str(number)) or [])
    target_generated = set(generated_by_pr.get(str(number)) or [])
    return sorted(
        int(peer)
        for peer in batch.get("pull_requests") or []
        if int(peer) != number
        and int(peer) in active_numbers
        and (
            target_paths.intersection(paths_by_pr.get(str(peer)) or [])
            or target_generated.intersection(generated_by_pr.get(str(peer)) or [])
        )
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


def reason_requires_repair(reason: str) -> bool:
    """Return whether a gate needs source-owner action instead of more time."""
    if reason == "pull request is draft":
        return False
    if reason == "GitHub reports the pull request merge state as DRAFT":
        return False
    if reason == "GitHub is still computing mergeability":
        return False
    if reason.endswith(" is not complete"):
        return False
    if "score is missing for the current head" in reason:
        return False
    if reason.endswith(" has not reviewed the current head"):
        return False
    if re.fullmatch(r"\d+/\d+ exact-head approvals complete", reason):
        return False
    return True


def deployment_repair_required(entry: "QueueEntry") -> bool:
    if any(verdict.state == "blocked" for verdict in entry.review_verdicts):
        return True
    waiting_reasons = {
        reason
        for verdict in entry.review_verdicts
        if verdict.state == "waiting"
        for reason in verdict.reasons
    }
    waiting_reasons.update(
        f"{name} is not complete"
        for name, status in entry.checks.items()
        if status not in {"failed", "passed"}
    )
    transitional = {
        "pull request is draft",
        "GitHub reports the pull request merge state as DRAFT",
        "GitHub is still computing mergeability",
        *waiting_reasons,
    }
    return any(reason not in transitional for reason in entry.reasons or [])


def repair_marker_is_transitional(marker: dict[str, Any] | None) -> bool:
    if not marker:
        return False
    reasons = [
        value.strip()
        for value in str(marker.get("reason") or "").split(";")
        if value.strip()
    ]
    return bool(reasons) and not any(reason_requires_repair(value) for value in reasons)


def repair_overlap_hold_active(
    client: "GitHub",
    entry: "QueueEntry",
    intent: dict[str, Any],
    repair: dict[str, Any] | None,
    *,
    now: str | None = None,
) -> bool:
    """Keep genuine repairs in overlap scheduling without authorizing a merge."""
    if not repair or repair_marker_is_transitional(repair):
        return False
    if client.config.blocked_label not in entry.labels:
        return False
    if str(repair.get("intent_id") or "") != str(intent.get("intent_id") or ""):
        return False
    if str(repair.get("pull_request") or "") != str(entry.number):
        return False
    created_at = parse_time(
        str(repair.get("hold_started_at") or repair.get("created_at") or "")
    )
    current = parse_time(now or utc_now())
    if (
        created_at is None
        or current is None
        or created_at.tzinfo is None
        or current.tzinfo is None
    ):
        return False
    expires_at = created_at + timedelta(
        minutes=client.config.pipeline.repair_hold_minutes
    )
    return current <= expires_at


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
    repair_overlap_hold: bool = False

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

        required_checks_passed = bool(config.required_checks)
        for name in config.required_checks:
            status = self.checks.get(name)
            if status == "failed":
                blocked.append(f"{name} failed")
                required_checks_passed = False
            elif status != "passed":
                waiting.append(f"{name} is not complete")
                required_checks_passed = False

        for verdict in self.review_verdicts:
            if verdict.state == "blocked":
                blocked.extend(verdict.reasons)
            elif verdict.state != "passed":
                waiting.extend(verdict.reasons)

        if self.mergeable == "CONFLICTING" or self.merge_state == "DIRTY":
            blocked.append("pull request conflicts with main")
        elif self.merge_state == "BEHIND":
            blocked.append("GitHub reports the pull request head ref is out of date")
        elif self.merge_state in {"BLOCKED", "DRAFT"}:
            blocked.append(
                f"GitHub reports the pull request merge state as {self.merge_state}"
            )
        elif self.merge_state == "UNSTABLE" and not required_checks_passed:
            waiting.append("GitHub reports non-passing commit status")
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
        self._registry_comments_cache: list[dict[str, Any]] | None = None
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
            # The pre-read preserves existing label metadata in the normal
            # path; --force makes the remaining list/create race idempotent if
            # another first-use worker creates this label concurrently.
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

    def workflow_runs(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        endpoint = (
            f"repos/{self.repository}/actions/runs?"
            f"branch={self.config.base_branch}&per_page=100"
        )
        if limit is None:
            return self._paged_object_items(endpoint, "workflow_runs")
        data = self._json("api", endpoint)
        values = data.get("workflow_runs") if isinstance(data, dict) else None
        if not isinstance(values, list):
            raise QueueError(f"unexpected GitHub response for {endpoint}")
        return [value for value in values if isinstance(value, dict)][:limit]

    def workflow_runs_for_branch(self, branch: str) -> list[dict[str, Any]]:
        return self._paged_object_items(
            f"repos/{self.repository}/actions/runs?branch={quote(branch, safe='')}&per_page=100",
            "workflow_runs",
        )

    def workflow_runs_for_workflows(
        self,
        names: Iterable[str],
        *,
        limit: int = 100,
        status: str | None = None,
        since: datetime | None = None,
    ) -> list[dict[str, Any]]:
        configured = tuple(dict.fromkeys(names))
        if not configured:
            return []
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
        runs: list[dict[str, Any]] = []
        for name in configured:
            matches = [
                value
                for value in workflows
                if str(value.get("name") or "") == name
                and str(value.get("state") or "") == "active"
            ]
            if len(matches) != 1:
                raise QueueError(
                    f"configured workflow {name!r} did not resolve to one active workflow"
                )
            workflow_id = int(matches[0]["id"])
            status_filter = f"&status={quote(status, safe='')}" if status else ""
            page = 1
            workflow_runs: list[dict[str, Any]] = []
            while True:
                endpoint = (
                    f"repos/{self.repository}/actions/workflows/{workflow_id}/runs?"
                    f"branch={quote(self.config.base_branch, safe='')}{status_filter}&"
                    f"per_page={min(max(limit, 1), 100)}&page={page}"
                )
                data = self._json("api", endpoint)
                values = data.get("workflow_runs") if isinstance(data, dict) else None
                if not isinstance(values, list):
                    raise QueueError(f"unexpected GitHub response for {endpoint}")
                workflow_runs.extend(
                    value for value in values if isinstance(value, dict)
                )
                oldest = min(
                    (
                        parse_time(
                            str(
                                value.get("created_at") or value.get("updated_at") or ""
                            )
                        )
                        for value in values
                        if isinstance(value, dict)
                    ),
                    default=None,
                )
                if (
                    len(values) < min(max(limit, 1), 100)
                    or (since is not None and oldest is not None and oldest <= since)
                    or (since is None and len(workflow_runs) >= limit)
                ):
                    break
                page += 1
            runs.extend(workflow_runs)
        ordered = sorted(
            runs,
            key=lambda value: str(
                value.get("created_at") or value.get("updated_at") or ""
            ),
            reverse=True,
        )
        if since is not None:
            return [
                value
                for value in ordered
                if (parse_time(str(value.get("created_at") or "")) or since) >= since
            ]
        return ordered[:limit]

    def successful_workflow_runs(
        self,
        names: Iterable[str],
        *,
        limit: int = 100,
        since: datetime | None = None,
    ) -> list[dict[str, Any]]:
        return self.workflow_runs_for_workflows(
            names,
            limit=limit,
            status="success",
            since=since,
        )

    def commit_check_runs(self, head_sha: str) -> list[dict[str, Any]]:
        return self._paged_object_items(
            f"repos/{self.repository}/commits/{head_sha}/check-runs?per_page=100",
            "check_runs",
        )

    def pull_head(self, number: int) -> dict[str, str]:
        value = self._json(
            "pr",
            "view",
            str(number),
            "--repo",
            self.repository,
            "--json",
            "headRefName,headRefOid,state",
        )
        return {
            "branch": str(value.get("headRefName") or ""),
            "head_sha": str(value.get("headRefOid") or ""),
            "state": str(value.get("state") or ""),
        }

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

    def dispatch_ci_workflows(
        self,
        *,
        ref: str | None = None,
        names: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        configured = tuple(names or self.config.pipeline.ci_workflows)
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
                ref or self.config.base_branch,
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
        merged: list[dict[str, Any]] = []
        page = 1
        while len(merged) < limit:
            endpoint = (
                "repos/"
                f"{self.repository}/pulls?state=closed&sort=updated&direction=desc"
                f"&per_page=100&page={page}"
            )
            values = self._json("api", endpoint)
            if not isinstance(values, list):
                raise QueueError(f"unexpected GitHub response for {endpoint}")
            merged.extend(
                value
                for value in values
                if isinstance(value, dict) and value.get("merged_at")
            )
            if len(values) < 100:
                break
            page += 1
        return merged[:limit]

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        comparison = self._json(
            "api", f"repos/{self.repository}/compare/{ancestor}...{descendant}"
        )
        return comparison.get("status") in {"ahead", "identical"}

    def branch_ref_sha(self, branch: str) -> str:
        value = self._json("api", f"repos/{self.repository}/git/ref/heads/{branch}")
        sha = (value.get("object") or {}).get("sha") if isinstance(value, dict) else ""
        if not sha:
            raise QueueError(f"could not read integration branch {branch}")
        return str(sha)

    def integration_pulls(
        self, branch: str, *, state: str = "open"
    ) -> list[dict[str, Any]]:
        values = self._json(
            "api",
            f"repos/{self.repository}/pulls?state={state}&head={self.owner}:{branch}",
        )
        return values if isinstance(values, list) else []

    def open_integration_pulls(self, branch: str) -> list[dict[str, Any]]:
        return self.integration_pulls(branch, state="open")

    def latest_integration_marker(self, pull_number: int) -> dict[str, Any] | None:
        return latest_payload(
            self.comments(pull_number),
            INTEGRATION_MARKER,
            self.coordinator_logins,
        )

    @staticmethod
    def integration_marker_matches(
        marker: dict[str, Any],
        *,
        batch_id: str,
        heads: dict[str, str],
        pull_requests: list[int],
    ) -> bool:
        return (
            marker.get("batch_id") == batch_id
            and marker.get("heads") == heads
            and marker.get("pull_requests") == pull_requests
        )

    def create_integration_seal(
        self,
        *,
        parent_sha: str,
        payload: dict[str, Any],
    ) -> str:
        parent = self._json(
            "api", f"repos/{self.repository}/git/commits/{parent_sha}"
        )
        tree_sha = str((parent.get("tree") or {}).get("sha") or "")
        if not tree_sha:
            raise QueueError("GitHub did not return the integration candidate tree")
        value = self._json(
            "api",
            "--method",
            "POST",
            f"repos/{self.repository}/git/commits",
            "-f",
            f"message={INTEGRATION_SEAL_PREFIX}{json.dumps(payload, sort_keys=True)}",
            "-f",
            f"tree={tree_sha}",
            "-f",
            f"parents[]={parent_sha}",
        )
        seal_sha = str(value.get("sha") or "")
        if not seal_sha:
            raise QueueError("GitHub did not create the integration seal commit")
        return seal_sha

    def integration_seal(self, sha: str) -> dict[str, Any]:
        value = self._json("api", f"repos/{self.repository}/git/commits/{sha}")
        message = str(value.get("message") or "")
        if not message.startswith(INTEGRATION_SEAL_PREFIX):
            raise QueueError("existing integration branch is not immutably sealed")
        try:
            payload = json.loads(message[len(INTEGRATION_SEAL_PREFIX) :])
        except json.JSONDecodeError as error:
            raise QueueError("existing integration branch has an invalid seal") from error
        if not isinstance(payload, dict):
            raise QueueError("existing integration branch has an invalid seal")
        return payload

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
        created = self._json(
            "api",
            "--method",
            "POST",
            f"repos/{self.repository}/issues/{number}/comments",
            "-f",
            f"body={body}",
        )
        if number != getattr(self, "_registry_issue_cache", None):
            return
        cached = getattr(self, "_registry_comments_cache", None)
        if cached is not None and isinstance(created, dict):
            cached.append(created)
        elif cached is not None:
            # Tests and alternate GitHub adapters may not return the created
            # comment. Force the next reader to refresh instead of serving a
            # stale registry snapshot.
            self._registry_comments_cache = None

    def registry_comments(self) -> list[dict[str, Any]]:
        cached = getattr(self, "_registry_comments_cache", None)
        if cached is not None:
            return cached
        number = self.registry_issue_number(create=False)
        comments = self.comments(number) if number is not None else []
        self._registry_comments_cache = comments
        return comments

    def record_thread(self, record: ThreadRecord) -> None:
        number = self.registry_issue_number(create=True)
        if number is None:  # pragma: no cover - create owns this invariant.
            raise QueueError("could not create DeployBot registry")
        self.issue_comment(number, thread_record_body(record))

    def pull_request_thread_owners(
        self,
    ) -> dict[int, PullRequestThreadOwnerRecord]:
        # Controller-only actors may advance the delivery pipeline, but they
        # cannot claim the native thread that originated a pull request.
        return pull_request_thread_owners(
            self.registry_comments(), self.trusted_logins
        )

    def pull_request_thread_owner(
        self, pull_request: int
    ) -> PullRequestThreadOwnerRecord | None:
        return self.pull_request_thread_owners().get(pull_request)

    def claim_pull_request_thread_owner(
        self, record: PullRequestThreadOwnerRecord
    ) -> PullRequestThreadOwnerRecord:
        current = self.pull_request_thread_owner(record.pull_request)
        if current is not None:
            return current
        number = self.registry_issue_number(create=True)
        if number is None:  # pragma: no cover - create owns this invariant.
            raise QueueError("could not create DeployBot registry")
        self.issue_comment(number, pull_request_thread_owner_body(record))
        # The oldest trusted claim wins if two source agents race. Re-read the
        # append-only registry instead of assuming this write won ownership.
        self._registry_comments_cache = None
        current = self.pull_request_thread_owner(record.pull_request)
        if current is None:  # pragma: no cover - GitHub accepted the write.
            raise QueueError("GitHub did not confirm the pull request thread owner")
        return current

    def record_deployment_notification(
        self, record: DeploymentNotificationRecord
    ) -> None:
        number = self.registry_issue_number(create=True)
        if number is None:  # pragma: no cover - create owns this invariant.
            raise QueueError("could not create DeployBot registry")
        self.issue_comment(number, deployment_notification_body(record))

    def deployment_notifications(
        self, *, include_delivered: bool = False
    ) -> list[dict[str, Any]]:
        trusted = self.trusted_logins | self.coordinator_logins
        return [
            record.as_dict()
            for record in latest_deployment_notifications(
                self.registry_comments(),
                trusted,
                include_delivered=include_delivered,
            )
        ]

    def thread_records(self, *, include_terminal: bool = False) -> list[dict[str, Any]]:
        trusted = self.trusted_logins | self.coordinator_logins
        return [
            record.as_dict()
            for record in latest_thread_records(
                self.registry_comments(),
                trusted,
                active_hours=self.config.pipeline.thread_active_hours,
                include_terminal=include_terminal,
            )
        ]

    def pipeline_control(self) -> dict[str, Any]:
        control = latest_control(self.registry_comments(), self.coordinator_logins)
        if (
            control.get("state") == "paused"
            and control.get("legacy_control")
            and not control.get("main_sha")
        ):
            # v0.2.12 pause records predate release binding. The immutable
            # comment ID still supplies a unique compare-and-set token; bind
            # the migration view to the current main and recheck it at write.
            return {**control, "main_sha": self.base_sha()}
        return control

    def set_pipeline_control(
        self,
        state: str,
        reason: str | None = None,
        *,
        main_sha: str | None = None,
        requires_control_id: str | None = None,
        resumes_control_id: str | None = None,
    ) -> str:
        number = self.registry_issue_number(create=True)
        if number is None:  # pragma: no cover
            raise QueueError("could not create DeployBot registry")
        control_id = secrets.token_hex(16)
        self.issue_comment(
            number,
            control_body(
                state=state,
                control_id=control_id,
                reason=reason,
                main_sha=(main_sha or self.base_sha()) if state == "paused" else None,
                requires_control_id=requires_control_id,
                resumes_control_id=resumes_control_id,
            ),
        )
        return control_id

    def verified_main_sha(self) -> str | None:
        value = latest_payload(
            self.registry_comments(),
            RELEASE_WATERMARK_MARKER,
            self.coordinator_logins,
        )
        sha = str((value or {}).get("main_sha") or "")
        return sha if re.fullmatch(r"[0-9a-f]{40}", sha) else None

    def record_verified_main(self, main_sha: str) -> None:
        if self.verified_main_sha() == main_sha:
            return
        number = self.registry_issue_number(create=True)
        if number is None:  # pragma: no cover
            raise QueueError("could not create DeployBot registry")
        self.issue_comment(number, release_watermark_body(main_sha))

    def claim_release_repair(
        self,
        *,
        provider: str,
        thread_id: str,
        thread_url: str | None = None,
        main_sha: str | None = None,
    ) -> dict[str, Any]:
        """Atomically elect one native thread to repair the active release."""
        self.require_actor(
            self.coordinator_logins | self.trusted_logins,
            "claim a release repair",
        )
        current_sha = self.base_sha()
        expected_sha = main_sha or current_sha
        if expected_sha != current_sha:
            raise QueueError(
                f"release repair SHA {expected_sha} is stale; current main is {current_sha}"
            )
        release = release_state(
            main_sha=current_sha,
            runs=self.workflow_runs(),
            config=self.config.pipeline,
        )
        if release.get("state") == "verified" and self.config.pipeline.verifications:
            health = http_verifications(self.config.pipeline)
            if not all(item["passed"] for item in health):
                release = {
                    **release,
                    "state": "verify-failed",
                    "verifications": health,
                }
        if release.get("state") not in {
            "ci-failed",
            "deploy-failed",
            "verify-failed",
        }:
            raise QueueError(
                f"main {current_sha} is {release.get('state')}; no release repair is claimable"
            )
        branch = f"{self.config.pipeline.repair_branch_prefix}/{current_sha[:12]}"
        latest_ci = release.get("latest_ci") or {}
        latest_deploy = release.get("latest_deploy") or {}
        failed_run = (
            latest_deploy
            if release.get("state") == "deploy-failed"
            else latest_ci
        )
        payload = {
            "branch": branch,
            "claimed_at": utc_now(),
            "failure_state": release.get("state"),
            "main_sha": current_sha,
            "provider": provider,
            "run_id": failed_run.get("id"),
            "thread_id": thread_id,
            "thread_url": thread_url,
        }
        base_commit = self._json(
            "api", f"repos/{self.repository}/git/commits/{current_sha}"
        )
        tree_sha = str((base_commit.get("tree") or {}).get("sha") or "")
        if not tree_sha:
            raise QueueError("GitHub did not return the failed main commit tree")
        lease_commit = self._json(
            "api",
            "--method",
            "POST",
            f"repos/{self.repository}/git/commits",
            "-f",
            f"message={RELEASE_REPAIR_LEASE_PREFIX}{json.dumps(payload, sort_keys=True)}",
            "-f",
            f"tree={tree_sha}",
            "-f",
            f"parents[]={current_sha}",
        )
        lease_sha = str(lease_commit.get("sha") or "")
        if not lease_sha:
            raise QueueError("GitHub did not create the release repair lease commit")
        payload["lease_sha"] = lease_sha
        if self.base_sha() != current_sha:
            raise QueueError(
                f"release repair SHA {current_sha} is stale; main advanced before claim"
            )
        created = False
        try:
            self._json(
                "api",
                "--method",
                "POST",
                f"repos/{self.repository}/git/refs",
                "-f",
                f"ref=refs/heads/{branch}",
                "-f",
                f"sha={lease_sha}",
            )
            created = True
        except QueueError as error:
            if "Reference already exists" not in str(error):
                raise

        if self.base_sha() != current_sha:
            if created:
                try:
                    self._run(
                        "api",
                        "--method",
                        "DELETE",
                        f"repos/{self.repository}/git/refs/heads/{branch}",
                    )
                except QueueError:
                    pass
            raise QueueError(
                f"release repair SHA {current_sha} is stale; main advanced during claim"
            )

        if not created:
            ref = self._json(
                "api", f"repos/{self.repository}/git/ref/heads/{branch}"
            )
            lease_sha = str((ref.get("object") or {}).get("sha") or "")
            commit = self._json(
                "api", f"repos/{self.repository}/git/commits/{lease_sha}"
            )
            message = str(commit.get("message") or "")
            if not message.startswith(RELEASE_REPAIR_LEASE_PREFIX):
                raise QueueError(f"repair lease branch {branch} has invalid ownership")
            try:
                lease_payload = json.loads(message[len(RELEASE_REPAIR_LEASE_PREFIX) :])
            except json.JSONDecodeError as error:
                raise QueueError(
                    f"repair lease branch {branch} has invalid ownership"
                ) from error
            if (
                not isinstance(lease_payload, dict)
                or lease_payload.get("main_sha") != current_sha
                or not lease_payload.get("thread_id")
            ):
                raise QueueError(f"repair lease branch {branch} has invalid ownership")
            payload = {**lease_payload, "lease_sha": lease_sha}

        # The ref's owner-encoded commit is authoritative. Publishing the
        # registry record is retryable and idempotent even if the first process
        # exits immediately after winning the atomic ref create.
        number = self.registry_issue_number(create=True)
        if number is None:  # pragma: no cover
            raise QueueError("could not create DeployBot registry")
        self.issue_comment(number, release_repair_body(payload))
        same_owner = (
            payload.get("provider") == provider
            and payload.get("thread_id") == thread_id
        )
        return {**payload, "state": "owned" if same_owner else "claimed"}

    def create_integration_pull_request(
        self,
        *,
        batch: dict[str, Any],
        entries: list[QueueEntry],
    ) -> dict[str, Any]:
        batch_id = str(batch["batch_id"])
        safe_id = re.sub(r"[^a-zA-Z0-9._-]+", "-", batch_id)[-80:]
        branch = f"{self.config.integration.branch_prefix}/{safe_id}"
        staging_branch = (
            f"{self.config.integration.branch_prefix}-staging/"
            f"{safe_id}-{secrets.token_hex(8)}"
        )
        base_sha = self.base_sha()
        entry_heads = {str(entry.number): entry.head_sha for entry in entries}
        frozen_heads = batch.get("heads")
        frozen_pull_requests = batch.get("pull_requests")
        if isinstance(frozen_heads, dict) and isinstance(
            frozen_pull_requests, list
        ):
            heads = {str(number): str(sha) for number, sha in frozen_heads.items()}
            frozen_numbers = [int(number) for number in frozen_pull_requests]
            if set(heads) != {str(number) for number in frozen_numbers}:
                raise QueueError("frozen integration batch has inconsistent members")
            if any(heads.get(number) != sha for number, sha in entry_heads.items()):
                raise QueueError("current integration entries changed after freeze")
        else:
            heads = entry_heads
            frozen_numbers = [entry.number for entry in entries]
        batch_heads = heads
        batch_numbers = frozen_numbers
        heads = entry_heads
        pull_requests = [entry.number for entry in entries]
        frozen_sources = [(number, heads[str(number)]) for number in pull_requests]
        current_tip = base_sha
        staging_created = False
        try:
            self._json(
                "api",
                "--method",
                "POST",
                f"repos/{self.repository}/git/refs",
                "-f",
                f"ref=refs/heads/{staging_branch}",
                "-f",
                f"sha={base_sha}",
            )
            staging_created = True
            merged_heads: list[str] = []
            conflict: dict[str, Any] | None = None
            for number, head_sha in frozen_sources:
                try:
                    if self.is_ancestor(head_sha, current_tip):
                        if head_sha not in merged_heads:
                            merged_heads.append(head_sha)
                        continue
                    merge = self._json(
                        "api",
                        "--method",
                        "POST",
                        f"repos/{self.repository}/merges",
                        "-f",
                        f"base={staging_branch}",
                        "-f",
                        f"head={head_sha}",
                        "-f",
                        f"commit_message=DeployBot batch {batch_id}: PR #{number}",
                    )
                    if isinstance(merge, dict) and merge.get("sha"):
                        current_tip = str(merge["sha"])
                    else:
                        current_tip = self.branch_ref_sha(staging_branch)
                    if head_sha not in merged_heads:
                        merged_heads.append(head_sha)
                except QueueError as error:
                    conflict = {
                        "number": number,
                        "head_sha": head_sha,
                        "reason": str(error),
                    }
                    break

            seal = {
                "base_sha": base_sha,
                "batch_id": batch_id,
                "conflict": conflict,
                "heads": heads,
                "merged_heads": merged_heads,
                "pull_requests": pull_requests,
            }
            seal_sha = self.create_integration_seal(
                parent_sha=current_tip,
                payload=seal,
            )
            integration_sha = seal_sha
            try:
                self._json(
                    "api",
                    "--method",
                    "POST",
                    f"repos/{self.repository}/git/refs",
                    "-f",
                    f"ref=refs/heads/{branch}",
                    "-f",
                    f"sha={seal_sha}",
                )
            except QueueError as error:
                if "Reference already exists" not in str(error):
                    raise
                integration_sha = self.branch_ref_sha(branch)
                existing_seal = self.integration_seal(integration_sha)
                sealed_head_map_value = existing_seal.get("heads")
                sealed_numbers_value = existing_seal.get("pull_requests")
                if (
                    existing_seal.get("batch_id") != batch_id
                    or not isinstance(sealed_head_map_value, dict)
                    or not isinstance(sealed_numbers_value, list)
                ):
                    raise QueueError(
                        "existing sealed integration branch does not match "
                        "the active frozen batch"
                    )
                sealed_head_map = {
                    str(number): str(sha)
                    for number, sha in sealed_head_map_value.items()
                }
                sealed_numbers = [int(number) for number in sealed_numbers_value]
                if set(sealed_head_map) != {str(number) for number in sealed_numbers}:
                    raise QueueError(
                        "existing sealed integration branch has inconsistent members"
                    )
                if any(
                    number not in batch_numbers
                    or batch_heads.get(str(number)) != sealed_head_map[str(number)]
                    for number in sealed_numbers
                ) or any(
                    sealed_head_map.get(number) != sha
                    for number, sha in entry_heads.items()
                ):
                    raise QueueError(
                        "existing sealed integration branch does not match "
                        "the active frozen subset"
                    )
                heads = sealed_head_map
                pull_requests = sealed_numbers
                sealed_base = str(existing_seal.get("base_sha") or "")
                if (
                    not re.fullmatch(r"[0-9a-f]{40}", sealed_base)
                    or not self.is_ancestor(sealed_base, integration_sha)
                ):
                    raise QueueError(
                        "existing sealed integration branch does not contain "
                        "its recorded base"
                    )
                sealed_heads = [
                    str(value) for value in existing_seal.get("merged_heads") or []
                ]
                if len(sealed_heads) != len(set(sealed_heads)) or not set(
                    sealed_heads
                ).issubset(set(heads.values())):
                    raise QueueError(
                        "existing sealed integration branch has invalid merged heads"
                    )
                for sealed_head in sealed_heads:
                    if not self.is_ancestor(sealed_head, integration_sha):
                        raise QueueError(
                            "existing sealed integration branch does not contain "
                            "a recorded source head"
                        )
                sealed_conflict = existing_seal.get("conflict")
                if sealed_conflict is not None and not isinstance(
                    sealed_conflict, dict
                ):
                    raise QueueError(
                        "existing sealed integration branch has invalid conflict data"
                    )
                if isinstance(sealed_conflict, dict):
                    try:
                        conflict_number = int(sealed_conflict.get("number") or 0)
                    except (TypeError, ValueError) as error:
                        raise QueueError(
                            "existing sealed integration branch has invalid "
                            "conflict data"
                        ) from error
                    if (
                        conflict_number not in pull_requests
                        or str(sealed_conflict.get("head_sha") or "")
                        != heads[str(conflict_number)]
                        or not str(sealed_conflict.get("reason") or "")
                    ):
                        raise QueueError(
                            "existing sealed integration branch has invalid "
                            "conflict data"
                        )
                if sealed_conflict is None and set(sealed_heads) != set(heads.values()):
                    raise QueueError(
                        "existing sealed integration branch is incomplete without "
                        "a recorded conflict"
                    )
                seal = existing_seal
                conflict = sealed_conflict
                merged_heads = [str(value) for value in seal.get("merged_heads") or []]

            existing = self.open_integration_pulls(branch)
            reused_existing_pull = bool(existing)
            if existing:
                pull = existing[0]
            else:
                members = ", ".join(f"#{number}" for number in pull_requests)
                body = (
                    f"DeployBot cumulative batch `{batch_id}` for {members}.\n\n"
                    "Every source head was frozen and independently authorized. "
                    "If a merge conflict remains, an agent must resolve it without "
                    "dropping either side before marking this PR ready."
                )
                try:
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
                except QueueError as error:
                    # Another coordinator may have created the immutable PR
                    # after our read. Re-read instead of deleting its sealed ref.
                    existing = self.open_integration_pulls(branch)
                    if existing:
                        pull = existing[0]
                    else:
                        closed = [
                            value
                            for value in self.integration_pulls(
                                branch, state="closed"
                            )
                            if not value.get("merged_at")
                        ]
                        if not closed:
                            raise error
                        previous = closed[0]
                        reopened = self._json(
                            "api",
                            "--method",
                            "PATCH",
                            f"repos/{self.repository}/pulls/{previous['number']}",
                            "-f",
                            "state=open",
                        )
                        pull = reopened if isinstance(reopened, dict) else previous
                    reused_existing_pull = True

            author = str((pull.get("user") or {}).get("login") or "")
            identity_error: str | None = None
            if self.config.integration.require_non_actions_author:
                if author.lower() == "github-actions[bot]":
                    identity_error = (
                        "integration PRs require a GitHub App installation token; "
                        "pass the action's token input and do not use github.token"
                    )
                elif author.lower() not in {
                    login.lower() for login in self.coordinator_logins
                }:
                    identity_error = (
                        f"integration PR author {author or '<unknown>'} is not trusted; "
                        "add the GitHub App bot login to queue.coordinator_actors"
                    )
            if identity_error:
                cleanup_calls = (
                    (
                        "api",
                        "--method",
                        "PATCH",
                        f"repos/{self.repository}/pulls/{pull['number']}",
                        "-f",
                        "state=closed",
                    ),
                    (
                        "api",
                        "--method",
                        "DELETE",
                        f"repos/{self.repository}/git/refs/heads/{branch}",
                    ),
                )
                for cleanup_call in cleanup_calls:
                    try:
                        self._run(*cleanup_call)
                    except QueueError:
                        pass
                raise QueueError(identity_error)
            integration_number = int(pull["number"])
            if reused_existing_pull and conflict is not None and not pull.get("draft"):
                self._run(
                    "pr",
                    "ready",
                    str(integration_number),
                    "--undo",
                    "--repo",
                    self.repository,
                )
            if reused_existing_pull and conflict is None and pull.get("draft"):
                self._run(
                    "pr", "ready", str(integration_number), "--repo", self.repository
                )
            marker = {
                **seal,
                "author": author,
                "created_at": utc_now(),
                "integration_sha": integration_sha,
            }
            self.comment(integration_number, integration_body(marker))
            # The cumulative PR now owns the queue position, but the source intent
            # remains discoverable until delivery succeeds. This lets promotion
            # recover the exact-head request if the integration closes, conflicts,
            # or rejects a source that changed after the batch was frozen.
            for source_number in pull_requests:
                source_labels = self.labels(source_number)
                if self.config.queue_label in source_labels:
                    self.remove_label(source_number, self.config.queue_label)
            return {
                "number": integration_number,
                "url": pull.get("html_url"),
                "branch": branch,
                "conflict": conflict,
                "batch_id": batch_id,
            }
        finally:
            if staging_created:
                try:
                    self._run(
                        "api",
                        "--method",
                        "DELETE",
                        f"repos/{self.repository}/git/refs/heads/{staging_branch}",
                    )
                except QueueError:
                    pass

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

    def comments_for_pull_requests(
        self, numbers: Iterable[int]
    ) -> dict[int, list[dict[str, Any]]]:
        selected = sorted({int(number) for number in numbers})
        if not selected:
            return {}
        selections = "\n".join(
            f"""pr_{number}: pullRequest(number: {number}) {{
              comments(last: 100) {{
                pageInfo {{ hasPreviousPage }}
                nodes {{ databaseId body createdAt author {{ login }} }}
              }}
            }}"""
            for number in selected
        )
        query = f"""query($owner: String!, $name: String!) {{
          repository(owner: $owner, name: $name) {{
            {selections}
          }}
        }}"""
        data = self._json(
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-F",
            f"owner={self.owner}",
            "-F",
            f"name={self.name}",
        )
        try:
            repository = data["data"]["repository"]
        except (KeyError, TypeError) as error:
            raise QueueError("could not read pull-request comments") from error
        result: dict[int, list[dict[str, Any]]] = {}
        for number in selected:
            connection = repository.get(f"pr_{number}") or {}
            comments = connection.get("comments") or {}
            if (comments.get("pageInfo") or {}).get("hasPreviousPage"):
                result[number] = self.comments(number)
                continue
            result[number] = [
                {
                    "id": value.get("databaseId"),
                    "body": value.get("body"),
                    "created_at": value.get("createdAt"),
                    "user": {"login": (value.get("author") or {}).get("login")},
                }
                for value in comments.get("nodes") or []
                if isinstance(value, dict)
            ]
        return result

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
        known_checks: dict[str, str] | None = None,
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

        comments = (
            known_comments if known_comments is not None else self.comments(number)
        )
        reviews = self.reviews(number) if self.config.review_providers else []
        needs_threads = any(
            provider.kind == "bot" and provider.require_resolved_threads
            for provider in self.config.review_providers
        )
        threads = self.review_threads(number) if needs_threads else []
        marker = queue_marker_for_client(self, comments)
        head_sha = str(pull["headRefOid"])
        check_rollup = list(pull.get("statusCheckRollup") or [])
        checks = merge_known_check_states(check_states(check_rollup), known_checks)
        integration = latest_payload(
            comments,
            INTEGRATION_MARKER,
            coordinator_logins(self),
        )
        # GitHub's PR rollup can briefly retain a cancelled or failed run after
        # a replacement run for the same exact head has started. Reconcile a
        # reported failure with the commit check-runs endpoint before creating
        # a repair block. Integration PRs keep the broader fallback because
        # their controller-dispatched checks may be absent from the rollup.
        rollup_has_required_failure = any(
            checks.get(name) == "failed" for name in self.config.required_checks
        )
        integration_needs_exact_checks = bool(
            integration
            and any(
                checks.get(name) != "passed"
                for name in self.config.required_checks
            )
        )
        if rollup_has_required_failure or integration_needs_exact_checks:
            checks = merge_known_check_states(
                check_states(check_rollup + self.commit_check_runs(head_sha)),
                known_checks,
            )
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

    def pull_merge_sha(self, number: int) -> str | None:
        value = self._json(
            "pr",
            "view",
            str(number),
            "--repo",
            self.repository,
            "--json",
            "mergeCommit,state",
        )
        merge_sha = str((value.get("mergeCommit") or {}).get("oid") or "")
        return merge_sha if value.get("state") == "MERGED" and merge_sha else None

    def pull_release_details(self, number: int) -> dict[str, str]:
        value = self._json(
            "pr",
            "view",
            str(number),
            "--repo",
            self.repository,
            "--json",
            "body,title,url",
        )
        return {
            "body": str(value.get("body") or ""),
            "title": str(value.get("title") or ""),
            "url": str(value.get("url") or ""),
        }

    def externally_integrated_merge(
        self, number: int, expected_head: str
    ) -> str | None:
        value = self._json(
            "pr",
            "view",
            str(number),
            "--repo",
            self.repository,
            "--json",
            "headRefOid,mergeCommit,state",
        )
        merge_sha = str((value.get("mergeCommit") or {}).get("oid") or "")
        if (
            value.get("state") != "MERGED"
            or value.get("headRefOid") != expected_head
            or not merge_sha
        ):
            return None
        base_sha = self.base_sha()
        if not self.is_ancestor(expected_head, base_sha):
            return None
        if not self.is_ancestor(merge_sha, base_sha):
            return None
        return merge_sha

    def merge(
        self,
        number: int,
        head_sha: str,
    ) -> str:
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
        integration_batch_id = (
            str(marker.get("integration_batch_id") or "") if marker else ""
        )
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
        paths = ", ".join(group["source_paths"] + group["generated_paths"])
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


def integration_ci_active_gate(client: GitHub, entry: QueueEntry) -> str | None:
    pull = client.pull_head(entry.number)
    branch = str(pull.get("branch") or "")
    head_sha = str(pull.get("head_sha") or "")
    if pull.get("state") != "OPEN" or not branch:
        return "integration pull request is no longer open"
    if head_sha != entry.head_sha:
        return "integration head changed while waiting for exact CI"
    configured = tuple(client.config.pipeline.ci_workflows)
    latest = latest_exact_workflow_runs(
        client.workflow_runs_for_branch(branch),
        configured,
        head_sha=head_sha,
    )
    waiting: list[str] = []
    for name in configured:
        run = latest.get(name)
        if run is None:
            waiting.append(f"{name} has not been dispatched")
            continue
        status = str(run.get("status") or "pending")
        conclusion = str(run.get("conclusion") or "")
        if status != "completed":
            waiting.append(f"{name} is {status.replace('_', ' ')}")
        elif conclusion != "success":
            waiting.append(f"{name} {conclusion or 'failed'}")
    if waiting:
        return "waiting for exact integration CI: " + "; ".join(waiting)
    return None


def pipeline_status(client: GitHub) -> dict[str, Any]:
    queued = client.queue()
    queued_by_number = {entry.number: entry for entry in queued}
    raw_thread_owners = client.pull_request_thread_owners()
    thread_owners = raw_thread_owners if isinstance(raw_thread_owners, dict) else {}

    def add_opening_thread(
        number: int, value: dict[str, Any]
    ) -> dict[str, Any]:
        owner = thread_owners.get(number)
        if owner is not None:
            value["opening_thread"] = owner.as_dict()
        return value

    open_numbers = client.open_pull_request_numbers()
    open_number_set = set(open_numbers)
    inspect_numbers = [
        number for number in open_numbers if number not in queued_by_number
    ]

    def inspect(
        number: int,
    ) -> tuple[int, list[dict[str, Any]], QueueEntry]:
        comments = client.comments(number)
        return (
            number,
            comments,
            client.snapshot(
                number,
                require_marker=False,
                allow_blocked_label=True,
                known_comments=comments,
            ),
        )

    inspected: dict[int, tuple[list[dict[str, Any]], QueueEntry]] = {}
    if inspect_numbers:
        with ThreadPoolExecutor(
            max_workers=min(
                max(client.config.pipeline.promotion_workers, 8),
                len(inspect_numbers),
            )
        ) as executor:
            for number, comments, entry in executor.map(inspect, inspect_numbers):
                inspected[number] = (comments, entry)
    active_intents: list[tuple[QueueEntry, dict[str, Any], dict[str, Any]]] = []
    stages: dict[str, list[dict[str, Any]]] = {
        "draft": [],
        "reviewing": [],
        "ready": [],
        "deploy_requested": [],
        "queued": [],
        "blocked": [],
    }
    for number in open_numbers:
        if number in queued_by_number:
            value = add_opening_thread(
                number, entry_dict(queued_by_number[number])
            )
            value["pipeline_stage"] = "queued"
            stages["queued"].append(value)
            continue
        comments, entry = inspected[number]
        labels = set(entry.labels)
        intent = latest_intent(comments, client.trusted_logins)
        active_intent = bool(intent and intent.get("state") == "requested")
        stale_intent = bool(
            active_intent
            and client.config.pipeline.intent_scope == "head"
            and intent.get("requested_head") != entry.head_sha
        )
        if client.config.pipeline.intent_label in labels and (
            not active_intent
            or stale_intent
            or client.config.blocked_label in labels
            or deployment_repair_required(entry)
        ):
            stage = "blocked"
        elif client.config.pipeline.intent_label in labels:
            stage = "deploy_requested"
        elif entry.is_draft:
            stage = "draft"
        elif entry.state == "ready":
            stage = "ready"
        elif entry.state == "waiting":
            stage = "reviewing"
        else:
            stage = "blocked"
        value = add_opening_thread(number, entry_dict(entry))
        value["pipeline_stage"] = stage
        if client.config.pipeline.intent_label in labels:
            value["deploy_intent"] = (
                {
                    "intent_id": intent.get("intent_id"),
                    "requested_at": intent.get("requested_at"),
                    "requested_head": intent.get("requested_head"),
                    "head_matches": not stale_intent,
                    "state": intent.get("state"),
                }
                if intent
                else None
            )
        if (
            client.config.pipeline.intent_label in labels
            and active_intent
            and intent is not None
        ):
            active_intents.append((entry, intent, value))
        stages[stage].append(value)
    main_sha = client.base_sha()
    delivery = release_state(
        main_sha=main_sha,
        runs=client.workflow_runs_for_workflows(
            (
                *client.config.pipeline.ci_workflows,
                *client.config.pipeline.deploy_workflows,
            ),
            limit=100,
        ),
        config=client.config.pipeline,
    )
    coordinator_logins = getattr(client, "coordinator_logins", None)
    if not isinstance(coordinator_logins, set):
        coordinator_logins = client.trusted_logins
    release_repair = latest_release_repair(
        client.registry_comments(),
        coordinator_logins | client.trusted_logins,
        main_sha=main_sha,
    )
    now = datetime.now(timezone.utc)
    alerts: list[dict[str, Any]] = []
    queue_target = client.config.pipeline.ready_to_merge_target_minutes * 60
    integration_values = client.integration_pull_request_numbers()
    integration_numbers = (
        {int(value) for value in integration_values}
        if isinstance(integration_values, list)
        else set()
    )
    for entry, intent, value in active_intents:
        timestamp = parse_time(str(intent.get("requested_at") or ""))
        elapsed = (now - timestamp).total_seconds() if timestamp else None
        if elapsed is None or elapsed <= queue_target:
            continue
        requested_head = str(intent.get("requested_head") or "")
        if requested_head and requested_head != entry.head_sha:
            active_gate = "deploy intent is bound to an older head"
        elif client.config.blocked_label in entry.labels:
            active_gate = "repair is blocked; source thread must resume"
        else:
            active_gate = "; ".join(entry.reasons or []) or "promotion worker"
        alerts.append(
            {
                "stage": "request-to-ready",
                "pull_request": entry.number,
                "elapsed_seconds": elapsed,
                "target_seconds": queue_target,
                "active_gate": active_gate,
                "requested_head": requested_head or None,
                "current_head": entry.head_sha,
                "pipeline_stage": value["pipeline_stage"],
            }
        )
    for entry in queued:
        timestamp = parse_time(entry.queued_at)
        elapsed = (now - timestamp).total_seconds() if timestamp else None
        if elapsed is not None and elapsed > queue_target:
            active_gate = "; ".join(entry.reasons or []) or "merge worker"
            if not entry.reasons and entry.number in integration_numbers:
                active_gate = integration_ci_active_gate(client, entry) or active_gate
            alerts.append(
                {
                    "stage": "queued-to-merge",
                    "pull_request": entry.number,
                    "elapsed_seconds": elapsed,
                    "target_seconds": queue_target,
                    "active_gate": active_gate,
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
        "pull_request_thread_owners": [
            value.as_dict()
            for _, value in sorted(thread_owners.items())
            if value.pull_request in open_number_set
        ],
        "unbound_pull_requests": [
            {
                "pull_request": value["number"],
                "pipeline_stage": stage,
                "head_sha": value["head_sha"],
                "title": value["title"],
                "url": value["url"],
            }
            for stage, values in stages.items()
            for value in values
            if value["number"] not in thread_owners
        ],
        "notifications": client.deployment_notifications(),
        "pull_requests": stages,
        "queue": [
            add_opening_thread(entry.number, entry_dict(entry)) for entry in queued
        ],
        "overlap_groups": overlap_groups(queued),
        "active_intent_overlap_groups": overlap_groups(
            [entry for entry, _, _ in active_intents]
        ),
        "release": delivery,
        "release_repair": release_repair,
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
        f"notifications: {len(value.get('notifications') or [])} pending; "
        f"unbound PRs: {len(value.get('unbound_pull_requests') or [])}; "
        "deploy requests: "
        f"{sum(1 for entries in stages.values() for entry in entries if entry.get('deploy_intent'))}; "
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
    updated_at = utc_now()
    owner: PullRequestThreadOwnerRecord | None = None
    if pull_request is not None and phase in PR_OPENING_PHASES:
        pull = client.pull_head(pull_request)
        if pull.get("state") != "OPEN":
            raise QueueError(
                f"PR #{pull_request} is not open; its opening thread cannot be claimed"
            )
        owner = client.claim_pull_request_thread_owner(
            PullRequestThreadOwnerRecord(
                provider=provider,
                thread_id=thread_id,
                pull_request=pull_request,
                recorded_at=updated_at,
                thread_url=url,
            )
        )
    client.record_thread(
        ThreadRecord(
            provider=provider,
            thread_id=thread_id,
            phase=phase,
            updated_at=updated_at,
            title=title,
            branch=branch,
            pull_request=pull_request,
            url=url,
        )
    )
    print(f"recorded {provider} thread {thread_id} as {phase}")
    if owner is not None and (
        owner.provider.lower() != provider.lower() or owner.thread_id != thread_id
    ):
        print(
            f"PR #{pull_request} remains owned by its opening thread "
            f"{owner.provider}/{owner.thread_id}"
        )


def thread_notification_id(
    *,
    repository: str,
    provider: str,
    thread_id: str,
    merge_sha: str,
    pull_request: int | None,
) -> str:
    identifier = hashlib.sha256(
        f"{repository}:{provider.lower()}:{thread_id}:{merge_sha}:{pull_request}".encode()
    ).hexdigest()[:24]
    return f"thread-deployed:{identifier}"


def record_deployment_notification_obligation(
    client: GitHub,
    *,
    provider: str,
    thread_id: str,
    merge_sha: str,
    pull_request: int | None,
    thread_url: str | None,
    updated_at: str | None = None,
) -> DeploymentNotificationRecord:
    record = DeploymentNotificationRecord(
        notification_id=thread_notification_id(
            repository=client.repository,
            provider=provider,
            thread_id=thread_id,
            merge_sha=merge_sha,
            pull_request=pull_request,
        ),
        provider=provider,
        thread_id=thread_id,
        state="awaiting-verification",
        updated_at=updated_at or utc_now(),
        repository=client.repository,
        merge_sha=merge_sha,
        pull_request=pull_request,
        thread_url=thread_url,
    )
    client.record_deployment_notification(record)
    return record


def command_thread_acknowledge(
    client: GitHub,
    *,
    provider: str,
    thread_id: str,
    notification_id: str,
) -> dict[str, Any]:
    notification = next(
        (
            value
            for value in client.deployment_notifications(include_delivered=True)
            if str(value.get("notification_id") or "") == notification_id
        ),
        None,
    )
    if notification is None:
        raise QueueError(f"DeployBot notification {notification_id} was not found")
    if (
        str(notification.get("provider") or "").lower() != provider.lower()
        or str(notification.get("thread_id") or "") != thread_id
    ):
        raise QueueError(
            f"notification does not belong to DeployBot thread {provider}/{thread_id}"
        )
    if notification.get("state") == "awaiting-verification":
        raise QueueError(f"DeployBot notification {notification_id} is not deployed")
    already_delivered = notification.get("state") == "delivered"
    delivered = DeploymentNotificationRecord(
        notification_id=notification_id,
        provider=str(notification["provider"]),
        thread_id=str(notification["thread_id"]),
        state="delivered",
        updated_at=(
            str(notification["updated_at"]) if already_delivered else utc_now()
        ),
        repository=str(notification["repository"]),
        main_sha=str(notification["main_sha"]),
        message=str(notification["message"]),
        merge_sha=str(notification["merge_sha"]),
        pull_request=notification.get("pull_request"),
        thread_url=str(notification.get("thread_url") or "") or None,
        ci_url=str(notification.get("ci_url") or "") or None,
        deployment_url=str(notification.get("deployment_url") or "") or None,
    )
    if not already_delivered:
        client.record_deployment_notification(delivered)
    current_thread = next(
        (
            value
            for value in client.thread_records(include_terminal=True)
            if str(value.get("provider") or "").lower() == provider.lower()
            and str(value.get("thread_id") or "") == thread_id
        ),
        None,
    )
    if (
        current_thread
        and current_thread.get("phase") in {"merged", "deployed"}
        and (
            current_thread.get("phase") == "merged"
            or current_thread.get("deployed_sha") == notification.get("main_sha")
        )
        and current_thread.get("merge_sha") == notification.get("merge_sha")
    ):
        client.record_thread(
            ThreadRecord(
                provider=str(current_thread["provider"]),
                thread_id=str(current_thread["thread_id"]),
                phase="completed",
                updated_at=delivered.updated_at,
                title=str(current_thread.get("title") or "") or None,
                branch=str(current_thread.get("branch") or "") or None,
                pull_request=current_thread.get("pull_request"),
                url=str(current_thread.get("url") or "") or None,
                merge_sha=delivered.merge_sha,
                deployed_sha=delivered.main_sha,
                ci_url=delivered.ci_url,
                deployment_url=delivered.deployment_url,
            )
        )
    if already_delivered:
        print(f"thread notification already acknowledged for {provider}/{thread_id}")
    else:
        print(f"acknowledged thread notification for {provider}/{thread_id}")
    return delivered.as_dict()


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
    owner = client.pull_request_thread_owner(number)
    if owner is None:
        raise QueueError(
            f"PR #{number} has no recorded opening thread; the thread that opened "
            "the pull request must first run `deploybot thread update --provider "
            "CLIENT --thread-id ID --phase pr-review --pr "
            f"{number}`"
        )
    notification_provider = owner.provider if owner is not None else None
    notification_thread_id = owner.thread_id if owner is not None else None
    notification_thread_url = owner.thread_url if owner is not None else None
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
            provider=notification_provider,
            thread_id=notification_thread_id,
            thread_url=notification_thread_url,
        ),
    )
    if client.config.pipeline.intent_label not in entry.labels:
        client.add_label(number, client.config.pipeline.intent_label)
    if notification_provider and notification_thread_id:
        client.record_thread(
            ThreadRecord(
                provider=notification_provider,
                thread_id=notification_thread_id,
                phase="deploy-requested",
                updated_at=requested_at,
                branch=None,
                pull_request=number,
                url=notification_thread_url,
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
            "provider": notification_provider,
            "thread_id": notification_thread_id,
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
    if notification_provider and notification_thread_id:
        webhook_env = client.config.pipeline.webhook_url_env
        webhook_ready = bool(webhook_env and os.environ.get(webhook_env))
        result["notification_handoff"] = {
            "owner": "webhook" if webhook_ready else "pr-opening-thread",
            "required_action": (
                "none"
                if webhook_ready
                else "route-native-follow-up-monitor-to-pr-opening-thread"
            ),
        }
        result["notification_thread"] = {
            "provider": notification_provider,
            "thread_id": notification_thread_id,
            **(
                {"thread_url": notification_thread_url}
                if notification_thread_url
                else {}
            ),
        }
    else:
        result["notification_handoff"] = {
            "owner": "untracked",
            "required_action": "record-provider-and-stable-thread-id",
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
    *,
    resume_pull_request: int | None = None,
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
        and previous.get("intent_id") == (intent or {}).get("intent_id")
    ):
        return previous
    created_at = utc_now()
    hold_started_at = created_at
    if (
        previous
        and previous.get("intent_id") == (intent or {}).get("intent_id")
        and not repair_marker_is_transitional(previous)
    ):
        hold_started_at = str(
            previous.get("hold_started_at")
            or previous.get("created_at")
            or created_at
        )
    payload = {
        "base_sha": client.base_sha(),
        "created_at": created_at,
        "head_sha": entry.head_sha,
        "hold_started_at": hold_started_at,
        "intent_id": (intent or {}).get("intent_id"),
        "provider": (intent or {}).get("provider"),
        "pull_request": entry.number,
        "reason": reason,
        "resume_command": (
            f"deploybot resume {resume_pull_request or entry.number}"
        ),
        "source_paths": entry.source_paths,
        "thread_id": (intent or {}).get("thread_id"),
        "thread_url": (intent or {}).get("thread_url"),
    }
    if resume_pull_request is not None:
        payload["repair_pull_request"] = resume_pull_request
    client.comment(entry.number, repair_body(payload))
    labels = client.labels(entry.number)
    if client.config.blocked_label not in labels:
        client.add_label(entry.number, client.config.blocked_label)
    if client.config.blocked_label not in entry.labels:
        entry.labels.append(client.config.blocked_label)
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


def record_integration_conflict_repair(
    client: GitHub,
    result: dict[str, Any],
    entries: list[QueueEntry],
) -> dict[str, Any] | None:
    """Elect one tracked source thread to repair a conflicted integration."""

    conflict = result.get("conflict")
    if not isinstance(conflict, dict):
        return None
    integration_number = int(result["number"])
    conflicting_number = int(conflict["number"])
    owner: QueueEntry | None = None
    owner_intent: dict[str, Any] | None = None
    for entry in entries:
        intent = latest_intent(client.comments(entry.number), client.trusted_logins)
        if (
            intent
            and intent.get("state") == "requested"
            and intent.get("provider")
            and intent.get("thread_id")
        ):
            owner = entry
            owner_intent = intent
            break
    if owner is None:
        owner = next(
            entry for entry in entries if entry.number == conflicting_number
        )
        owner_intent = latest_intent(
            client.comments(owner.number), client.trusted_logins
        )
    reason = (
        f"integration PR #{integration_number} could not merge delegated PR "
        f"#{conflicting_number}: {conflict.get('reason') or 'merge conflict'}; "
        f"repair {result.get('branch') or 'the integration branch'} without "
        "dropping any frozen source head"
    )
    repair = record_repair(
        client,
        owner,
        owner_intent,
        reason,
        resume_pull_request=integration_number,
    )
    result["repair_owner"] = {
        "pull_request": owner.number,
        "provider": repair.get("provider"),
        "thread_id": repair.get("thread_id"),
    }
    return repair


def reconcile_externally_merged_threads(client: GitHub) -> list[dict[str, Any]]:
    reconciled: list[dict[str, Any]] = []
    records = client.thread_records()
    if not isinstance(records, list):
        return reconciled
    for record in records:
        if record.get("phase") != "deploy-requested" or not record.get("pull_request"):
            continue
        number = int(record["pull_request"])
        comments = client.comments(number)
        intent = latest_intent(comments, client.trusted_logins)
        expected_head = str((intent or {}).get("requested_head") or "")
        if not expected_head:
            continue
        try:
            merge_sha = client.externally_integrated_merge(number, expected_head)
        except QueueError:
            continue
        if not merge_sha:
            try:
                pull = client.pull_head(number)
            except QueueError:
                continue
            if pull.get("state") != "CLOSED":
                continue
            updated_at = utc_now()
            client.record_thread(
                ThreadRecord(
                    provider=str(record["provider"]),
                    thread_id=str(record["thread_id"]),
                    phase="abandoned",
                    updated_at=updated_at,
                    title=str(record.get("title") or "") or None,
                    branch=str(record.get("branch") or "") or None,
                    pull_request=number,
                    url=str(record.get("url") or "") or None,
                )
            )
            value = {"pull_request": number, "state": "abandoned"}
            reconciled.append(value)
            notify(
                client.config.pipeline,
                "deploy-abandoned",
                {"repository": client.repository, **value},
            )
            continue
        updated_at = utc_now()
        record_deployment_notification_obligation(
            client,
            provider=str(record["provider"]),
            thread_id=str(record["thread_id"]),
            merge_sha=merge_sha,
            pull_request=number,
            thread_url=str(record.get("url") or "") or None,
            updated_at=updated_at,
        )
        client.record_thread(
            ThreadRecord(
                provider=str(record["provider"]),
                thread_id=str(record["thread_id"]),
                phase="merged",
                updated_at=updated_at,
                title=str(record.get("title") or "") or None,
                branch=str(record.get("branch") or "") or None,
                pull_request=number,
                url=str(record.get("url") or "") or None,
                merge_sha=merge_sha,
            )
        )
        value = {
            "head_sha": expected_head,
            "merge_sha": merge_sha,
            "pull_request": number,
        }
        reconciled.append(value)
        notify(
            client.config.pipeline,
            "externally-merged",
            {"repository": client.repository, **value},
        )
    return reconciled


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
        repair = latest_payload(
            comments,
            REPAIR_MARKER,
            coordinator_logins(client),
        )
        entry.repair_overlap_hold = repair_overlap_hold_active(
            client,
            entry,
            intent,
            repair,
        )
        if (
            client.config.pipeline.intent_scope == "head"
            and intent.get("requested_head") != entry.head_sha
        ):
            reason = (
                "deploy intent is bound to an older head; the trusted source agent "
                "must run deploybot refresh-request after fresh gates pass"
            )
            repair = record_repair(client, entry, intent, reason)
            entry.repair_overlap_hold = repair_overlap_hold_active(
                client,
                entry,
                intent,
                repair,
            )
            return "blocked", {"number": number, "reason": reason}, entry
        if client.config.blocked_label in entry.labels:
            # A conflict repair targets the cumulative PR while it is open.
            # If that integration disappears, active_integration_sources() no
            # longer suppresses this source, so release the integration-owned
            # hold and let the original exact-head intent recover normally.
            integration_repair_abandoned = bool(
                repair and repair.get("repair_pull_request")
            )
            if repair_marker_is_transitional(repair) or integration_repair_abandoned:
                if deployment_repair_required(entry):
                    reason = "; ".join(entry.reasons or ["blocked"])
                    repair = record_repair(client, entry, intent, reason)
                    entry.repair_overlap_hold = repair_overlap_hold_active(
                        client,
                        entry,
                        intent,
                        repair,
                    )
                    return "blocked", {"number": number, "reason": reason}, entry
                client.remove_label(number, client.config.blocked_label)
                entry.labels = [
                    label
                    for label in entry.labels
                    if label != client.config.blocked_label
                ]
            else:
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
        if deployment_repair_required(entry):
            reason = "; ".join(entry.reasons or ["blocked"])
            repair = record_repair(client, entry, intent, reason)
            entry.repair_overlap_hold = repair_overlap_hold_active(
                client,
                entry,
                intent,
                repair,
            )
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


def promote_integrations(
    client: GitHub,
    *,
    known_checks_by_number: dict[int, dict[str, str]] | None = None,
) -> list[int]:
    promoted: list[int] = []
    delegated_integrations = client.active_integration_sources()
    for number in client.integration_pull_request_numbers():
        # A newer cumulative PR owns any integration PR listed among its
        # sources. Re-promoting that nested source would recursively integrate
        # the same frozen work into another cumulative PR.
        if number in delegated_integrations:
            continue
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
        entry = client.snapshot(
            number,
            require_marker=False,
            allow_blocked_label=True,
            known_checks=(known_checks_by_number or {}).get(number),
        )
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
            for label in (
                client.config.queue_label,
                client.config.blocked_label,
            ):
                if label in source_labels:
                    client.remove_label(int(source_number), label)
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


def settle_integration_checks(
    client: GitHub,
    *,
    timeout_seconds: int,
    poll_seconds: int,
    numbers: Iterable[int] | None = None,
) -> list[dict[str, Any]]:
    """Own token-created integration CI until each clean PR is merge-ready."""
    if numbers is None:
        values = client.integration_pull_request_numbers()
        if not isinstance(values, list):
            return []
        selected = [int(value) for value in values]
    else:
        selected = [int(value) for value in numbers]
    configured = tuple(client.config.pipeline.ci_workflows)
    targets: dict[int, dict[str, Any]] = {}
    for number in selected:
        comments = client.comments(number)
        integration = latest_payload(
            comments,
            INTEGRATION_MARKER,
            coordinator_logins(client),
        )
        if not integration or integration.get("conflict"):
            continue
        pull = client.pull_head(number)
        branch = str(pull.get("branch") or "")
        head_sha = str(pull.get("head_sha") or "")
        if pull.get("state") != "OPEN" or not branch or not head_sha:
            raise QueueError(f"integration PR #{number} is no longer open")
        targets[number] = {
            "branch": branch,
            "dispatched": [],
            "head_sha": head_sha,
            "requested": set(),
        }

    results: list[dict[str, Any]] = []
    pending = dict(targets)
    deadline = time.monotonic() + timeout_seconds
    while pending:
        for number, target in list(pending.items()):
            branch = str(target["branch"])
            head_sha = str(target["head_sha"])
            current = client.pull_head(number)
            if current.get("state") != "OPEN":
                raise QueueError(f"integration PR #{number} is no longer open")
            if current.get("head_sha") != head_sha:
                raise QueueError(
                    f"integration PR #{number} changed while CI ownership was active"
                )
            runs = client.workflow_runs_for_branch(branch)
            latest = latest_exact_workflow_runs(
                runs,
                configured,
                head_sha=head_sha,
            )
            failed = [
                name
                for name, run in latest.items()
                if str(run.get("status") or "") == "completed"
                and str(run.get("conclusion") or "") != "success"
            ]
            if failed:
                raise QueueError(
                    f"integration PR #{number} CI failed: " + ", ".join(failed)
                )
            missing = [name for name in configured if name not in latest]
            undispatched = [
                name for name in missing if name not in target["requested"]
            ]
            if undispatched:
                target["dispatched"].extend(
                    client.dispatch_ci_workflows(ref=branch, names=undispatched)
                )
                target["requested"].update(undispatched)
            if missing:
                continue
            if all(
                str(run.get("status") or "") == "completed"
                and str(run.get("conclusion") or "") == "success"
                for run in latest.values()
            ):
                exact_checks = integration_owned_check_states(client, head_sha)
                entry = client.snapshot(
                    number,
                    require_marker=False,
                    allow_blocked_label=True,
                    known_checks=exact_checks,
                )
                if entry.head_sha != head_sha:
                    raise QueueError(
                        f"integration PR #{number} changed while CI evidence "
                        "was being applied"
                    )
                if entry.state == "ready":
                    results.append(
                        {
                            "branch": branch,
                            "dispatched_ci": target["dispatched"],
                            "checks": exact_checks,
                            "head_sha": head_sha,
                            "pull_request": number,
                            "state": "ready",
                        }
                    )
                    del pending[number]
                    continue
                if entry.state == "blocked":
                    raise QueueError(
                        f"integration PR #{number} is blocked: "
                        + "; ".join(entry.reasons or ["unknown gate"])
                    )
        if not pending:
            break
        if time.monotonic() >= deadline:
            numbers = ", ".join(f"#{number}" for number in pending)
            noun = "PR" if len(pending) == 1 else "PRs"
            raise QueueError(f"integration {noun} {numbers} CI timed out")
        time.sleep(poll_seconds)
    return results


def integration_owned_check_states(client: GitHub, head_sha: str) -> dict[str, str]:
    """Use successful exact integration CI as the required-check aggregate."""

    checks = check_states(client.commit_check_runs(head_sha))
    # Token-authored integration PRs do not emit every pull_request-only wrapper
    # check. The configured workflow's exact-head success is the aggregate proof
    # for those required checks; review providers remain separate and unchanged.
    for name in client.config.integration.ci_satisfies_checks:
        if checks.get(name) != "failed":
            checks[name] = "passed"
    return checks


def completed_integration_ci_checks(
    client: GitHub,
    *,
    branch: str,
    head_sha: str,
) -> dict[str, str] | None:
    configured = tuple(client.config.pipeline.ci_workflows)
    if not configured:
        return None
    latest = latest_exact_workflow_runs(
        client.workflow_runs_for_branch(branch),
        configured,
        head_sha=head_sha,
    )
    if any(name not in latest for name in configured):
        return None
    if not all(
        str(run.get("status") or "") == "completed"
        and str(run.get("conclusion") or "") == "success"
        for run in latest.values()
    ):
        return None
    return integration_owned_check_states(client, head_sha)


def command_resume(client: GitHub, selector: str | None) -> None:
    number = client.resolve_pr(selector)
    comments = client.comments(number)
    integration = latest_payload(
        comments,
        INTEGRATION_MARKER,
        coordinator_logins(client),
    )
    if integration:
        pull = client.pull_head(number)
        branch = str(pull.get("branch") or "")
        head_sha = str(pull.get("head_sha") or "")
        known_checks = completed_integration_ci_checks(
            client,
            branch=branch,
            head_sha=head_sha,
        )
        entry = client.snapshot(
            number,
            require_marker=False,
            allow_blocked_label=True,
            known_checks=known_checks,
        )
        if entry.head_sha != head_sha:
            raise QueueError(
                f"integration PR #{number} changed while CI evidence was being applied"
            )
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
            for label in (
                client.config.queue_label,
                client.config.blocked_label,
            ):
                if label in source_labels:
                    client.remove_label(int(source_number), label)
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
    current_comments = (
        comments if comments is not None else client.comments(entry.number)
    )
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


def bounded_batch_entries(
    entries: list[QueueEntry], max_batch_size: int
) -> list[QueueEntry]:
    """Choose one FIFO window closed over overlap and queued dependencies."""
    by_number = {entry.number: entry for entry in entries}
    component_by_number = {
        entry.number: {entry.number} for entry in entries
    }
    for group in overlap_groups(entries, include_generated=False):
        component = {int(number) for number in group["pull_requests"]}
        for number in component:
            component_by_number[number] = component

    def closure(seed: int) -> set[int]:
        result = set(component_by_number[seed])
        pending = list(result)
        while pending:
            number = pending.pop()
            entry = by_number[number]
            for dependency in entry.dependencies:
                if dependency not in by_number:
                    continue
                for related in component_by_number[dependency]:
                    if related not in result:
                        result.add(related)
                        pending.append(related)
        return result

    selected: set[int] = set()
    for entry in entries:
        if entry.number in selected:
            continue
        candidate = closure(entry.number)
        if len(candidate) > max_batch_size:
            if not selected:
                # A true source-overlap/dependency closure is indivisible. Let
                # it ship alone rather than deadlocking forever or splitting an
                # atomic validation unit. Generated artifacts are excluded from
                # source overlap, so they cannot inflate this exception.
                selected.update(candidate)
            break
        if len(selected | candidate) > max_batch_size:
            break
        selected.update(candidate)
        if len(selected) == max_batch_size:
            break
    return [entry for entry in entries if entry.number in selected]


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
        known_by_number = {
            entry.number: entry
            for entry in known_entries
            if entry.number in queued_numbers
        }
        entries_by_number = dict(known_by_number)
        for number in sorted(queued_numbers - entries_by_number.keys()):
            entries_by_number[number] = client.snapshot(number)

        # Deferred path reads are safe while an intent is only waiting, but a
        # frozen batch must persist a complete overlap graph. GitHub can report
        # transient UNKNOWN mergeability for an already-queued PR; hydrate its
        # paths here so a later retry cannot merge overlapping sources as if
        # they were independent.
        needs_paths = [
            entry
            for entry in known_by_number.values()
            if not entry.source_paths and not entry.generated_paths
        ]

        def load_paths(entry: QueueEntry) -> tuple[int, list[str], list[str]]:
            source_paths, generated_paths = client.changed_paths(entry.number)
            return entry.number, source_paths, generated_paths

        if needs_paths:
            with ThreadPoolExecutor(
                max_workers=min(
                    client.config.pipeline.promotion_workers, len(needs_paths)
                )
            ) as executor:
                for number, source_paths, generated_paths in executor.map(
                    load_paths, needs_paths
                ):
                    entries_by_number[number].source_paths = sorted(set(source_paths))
                    entries_by_number[number].generated_paths = sorted(
                        set(generated_paths)
                    )
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
        # A single slow integration must not turn every ready change behind it
        # into one unbounded release train. Preserve FIFO order, but freeze only
        # the configured delivery window; the remainder becomes the next batch.
        bounded = bounded_batch_entries(
            entries, client.config.integration.max_batch_size
        )
        if not bounded:
            return FreezeResult(
                None,
                [],
                blocked_entries,
                entries,
                overlap_groups(entries),
            )
        batch = new_batch(bounded, frozen_at=utc_now())
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
    number = (
        frozen_entry.number if frozen_entry is not None else client.resolve_pr(selector)
    )
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
        entry = next(
            (value for value in frozen_entries if value.number == number), None
        )
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

    externally_integrated = False
    try:
        fresh = client.snapshot(
            number,
            known_source_paths=entry.source_paths,
            known_generated_paths=entry.generated_paths,
        )
    except QueueError as error:
        merge_sha = (
            client.externally_integrated_merge(number, expected_head)
            if "is not open" in str(error)
            else None
        )
        if not merge_sha:
            raise
        fresh = entry
        externally_integrated = True
    if not externally_integrated:
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
            raise QueueError(
                f"batch {batch_id} predates the current queue authorization"
            )

        merge_sha = client.merge(number, fresh.head_sha)
    merged_comments = client.comments(number)
    intent = latest_intent(merged_comments, client.trusted_logins)
    integration = latest_payload(
        merged_comments,
        INTEGRATION_MARKER,
        coordinator_logins(client),
    )
    if intent and intent.get("provider") and intent.get("thread_id"):
        updated_at = utc_now()
        record_deployment_notification_obligation(
            client,
            provider=str(intent["provider"]),
            thread_id=str(intent["thread_id"]),
            merge_sha=merge_sha,
            pull_request=number,
            thread_url=str(intent.get("thread_url") or "") or None,
            updated_at=updated_at,
        )
        client.record_thread(
            ThreadRecord(
                provider=str(intent["provider"]),
                thread_id=str(intent["thread_id"]),
                phase="merged",
                updated_at=updated_at,
                pull_request=number,
                url=str(intent.get("thread_url") or "") or None,
                merge_sha=merge_sha,
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
            "ownership": "external" if externally_integrated else "deploybot",
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
                updated_at = utc_now()
                record_deployment_notification_obligation(
                    client,
                    provider=str(source_intent["provider"]),
                    thread_id=str(source_intent["thread_id"]),
                    merge_sha=merge_sha,
                    pull_request=int(source_number),
                    thread_url=(str(source_intent.get("thread_url") or "") or None),
                    updated_at=updated_at,
                )
                client.record_thread(
                    ThreadRecord(
                        provider=str(source_intent["provider"]),
                        thread_id=str(source_intent["thread_id"]),
                        phase="merged",
                        updated_at=updated_at,
                        pull_request=int(source_number),
                        url=str(source_intent.get("thread_url") or "") or None,
                        merge_sha=merge_sha,
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
        if batch_result["merged"]:
            # One reaction owns one bounded release batch. Leave the FIFO
            # remainder for the event that runs after exact-main verification.
            break
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
    record_integration_conflict_repair(client, result, selected)
    notify(
        client.config.pipeline,
        "integration-created",
        {"repository": client.repository, **result},
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


RELEASE_NOTE_HEADINGS = {
    "changes",
    "feature summary",
    "features",
    "overview",
    "release notes",
    "summary",
    "what changed",
    "what s changed",
    "whats changed",
}
RELEASE_NOTE_EXCLUDED_HEADINGS = {
    "impact",
    "safety",
    "screenshots",
    "test plan",
    "testing",
    "validation",
    "why",
}


def _release_note_heading(line: str) -> str | None:
    match = re.match(r"^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$", line)
    if not match:
        return None
    return re.sub(r"[^a-z0-9]+", " ", match.group(1).lower()).strip()


def _release_note_heading_matches(heading: str, names: set[str]) -> bool:
    return any(heading == name or heading.startswith(name + " ") for name in names)


def _release_note_text(value: str, *, limit: int = 180) -> str:
    text = re.sub(r"^\s*\[[ xX]\]\s*", "", value.strip())
    text = re.sub(r"!\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)
    text = re.sub(
        r"(?i)\b(?:(?:https?|ftp)://|www\.)\S+",
        "[external link omitted]",
        text,
    )
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text
    shortened = text[: limit - 1].rsplit(" ", 1)[0].rstrip(".,;:")
    return (shortened or text[: limit - 1]).rstrip() + "…"


def _markdown_literal(value: str) -> str:
    """Render untrusted text without activating its Markdown or URLs."""

    longest_fence = max((len(run) for run in re.findall(r"`+", value)), default=0)
    fence = "`" * (longest_fence + 1)
    return f"{fence} {value} {fence}"


def pull_request_feature_summary(body: str, *, limit: int = 3) -> list[str]:
    """Extract a short user-facing feature list from a pull-request body."""

    body = re.sub(r"<!--.*?-->", "", body or "", flags=re.DOTALL)
    preferred_bullets: list[str] = []
    preferred_text: list[str] = []
    fallback_bullets: list[str] = []
    fallback_text: list[str] = []
    heading = ""
    bullet_pattern = re.compile(r"^\s*(?:[-*+]\s+|\d+[.)]\s+)(.+)$")

    for raw_line in body.splitlines():
        parsed_heading = _release_note_heading(raw_line)
        if parsed_heading is not None:
            heading = parsed_heading
            continue
        line = raw_line.strip()
        if not line or line in {"---", "***", "___"}:
            continue
        bullet = bullet_pattern.match(raw_line)
        text = _release_note_text(bullet.group(1) if bullet else line)
        if not text or text.startswith("!["):
            continue
        preferred = _release_note_heading_matches(heading, RELEASE_NOTE_HEADINGS)
        excluded = _release_note_heading_matches(
            heading, RELEASE_NOTE_EXCLUDED_HEADINGS
        )
        if bullet:
            if preferred:
                preferred_bullets.append(text)
            elif not excluded:
                fallback_bullets.append(text)
        elif preferred:
            preferred_text.append(text)
        elif not heading:
            fallback_text.append(text)

    candidates = (
        preferred_bullets or preferred_text or fallback_bullets or fallback_text
    )
    unique: list[str] = []
    seen: set[str] = set()
    for value in candidates:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        unique.append(value)
        if len(unique) == limit:
            break
    return unique


def pull_request_notification_details(
    client: GitHub,
    pull_request: object,
    cache: dict[int, dict[str, str]],
) -> dict[str, str]:
    try:
        number = int(pull_request)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return {}
    if number <= 0:
        return {}
    if number not in cache:
        try:
            value = client.pull_release_details(number)
        except QueueError:
            value = {}
        cache[number] = value if isinstance(value, dict) else {}
    return cache[number]


def thread_deployment_notification(
    *,
    repository: str,
    record: dict[str, Any],
    release: dict[str, Any],
    pull_request_details: dict[str, str] | None = None,
) -> dict[str, Any]:
    main_sha = str(record.get("deployed_sha") or release["main_sha"])
    provider = str(record["provider"])
    thread_id = str(record["thread_id"])
    pull_request = record.get("pull_request")
    details = pull_request_details or {}
    subject = f"PR #{pull_request}" if pull_request is not None else "Your change"
    title = _release_note_text(
        str(details.get("title") or record.get("title") or subject), limit=160
    )
    pull_request_url = str(details.get("url") or "")
    if pull_request is not None and not pull_request_url:
        pull_request_url = f"https://github.com/{repository}/pull/{pull_request}"
    features = pull_request_feature_summary(str(details.get("body") or ""))
    same_release = str(release["main_sha"]) == main_sha
    ci_url = str(record.get("ci_url") or "")
    deployment_url = str(record.get("deployment_url") or "")
    if same_release:
        ci_url = ci_url or str((release.get("latest_ci") or {}).get("url") or "")
        deployment_url = deployment_url or str(
            (release.get("latest_deploy") or {}).get("url") or ""
        )
    pull_request_label = subject
    if pull_request_url:
        pull_request_label = f"[{subject}]({pull_request_url})"
    if pull_request_url and title == subject:
        deployed_change = f"**[{title}]({pull_request_url})**"
    elif pull_request_url:
        deployed_change = f"{_markdown_literal(title)} ({pull_request_label})"
    else:
        deployed_change = _markdown_literal(title)
    message_lines = [
        "Deployment complete",
        "",
        f"{deployed_change} is now live.",
    ]
    if features:
        message_lines.extend(["", "What changed:"])
        message_lines.extend(f"- {_markdown_literal(feature)}" for feature in features)
    message_lines.extend(
        [
            "",
            "Release details:",
            f"- Exact main: `{main_sha}`",
            "- CI, deployment, and configured health checks passed.",
        ]
    )
    links: list[str] = []
    if ci_url:
        links.append(f"[CI run]({ci_url})")
    if deployment_url:
        links.append(f"[Deployment run]({deployment_url})")
    if links:
        message_lines.append("- " + " · ".join(links))
    message = "\n".join(message_lines)
    notification: dict[str, Any] = {
        "notification_id": thread_notification_id(
            repository=repository,
            provider=provider,
            thread_id=thread_id,
            merge_sha=str(record.get("merge_sha") or ""),
            pull_request=pull_request,
        ),
        "repository": repository,
        "provider": provider,
        "thread_id": thread_id,
        "main_sha": main_sha,
        "message": message,
    }
    optional = {
        "thread_url": record.get("url"),
        "pull_request": pull_request,
        "merge_sha": record.get("merge_sha"),
        "ci_url": ci_url or None,
        "deployment_url": deployment_url or None,
    }
    notification.update(
        {key: value for key, value in optional.items() if value is not None}
    )
    return notification


def record_pending_deployment_notification(
    client: GitHub,
    notification: dict[str, Any],
    *,
    updated_at: str | None = None,
) -> DeploymentNotificationRecord:
    record = DeploymentNotificationRecord(
        notification_id=str(notification["notification_id"]),
        provider=str(notification["provider"]),
        thread_id=str(notification["thread_id"]),
        state="pending",
        updated_at=updated_at or utc_now(),
        repository=str(notification["repository"]),
        merge_sha=str(notification["merge_sha"]),
        main_sha=str(notification["main_sha"]),
        message=str(notification["message"]),
        pull_request=notification.get("pull_request"),
        thread_url=str(notification.get("thread_url") or "") or None,
        ci_url=str(notification.get("ci_url") or "") or None,
        deployment_url=str(notification.get("deployment_url") or "") or None,
    )
    client.record_deployment_notification(record)
    return record


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
            main_sha=str(result["main_sha"]),
        )
    if result["state"] == "verified":
        client.record_verified_main(str(result["main_sha"]))
        existing_notifications = client.deployment_notifications(include_delivered=True)
        if not isinstance(existing_notifications, list):
            existing_notifications = []
        pull_request_details_cache: dict[int, dict[str, str]] = {}
        notification_states = {
            str(value["notification_id"]): str(value["state"])
            for value in existing_notifications
        }
        pending_notifications = {
            str(value["notification_id"]): {
                key: item
                for key, item in value.items()
                if key not in {"state", "updated_at"} and item is not None
            }
            for value in existing_notifications
            if value.get("state") == "pending"
        }
        for obligation in existing_notifications:
            if obligation.get("state") != "awaiting-verification":
                continue
            merge_sha = str(obligation.get("merge_sha") or "")
            if not merge_sha or not client.is_ancestor(
                merge_sha, str(result["main_sha"])
            ):
                continue
            deployed_record = {
                "provider": obligation["provider"],
                "thread_id": obligation["thread_id"],
                "pull_request": obligation.get("pull_request"),
                "url": obligation.get("thread_url"),
                "merge_sha": merge_sha,
                "deployed_sha": result["main_sha"],
                "ci_url": (result.get("latest_ci") or {}).get("url"),
                "deployment_url": (result.get("latest_deploy") or {}).get("url"),
            }
            notification = thread_deployment_notification(
                repository=client.repository,
                record=deployed_record,
                release=result,
                pull_request_details=pull_request_notification_details(
                    client,
                    obligation.get("pull_request"),
                    pull_request_details_cache,
                ),
            )
            notification_id = str(notification["notification_id"])
            record_pending_deployment_notification(client, notification)
            pending_notifications[notification_id] = notification
            notification_states[notification_id] = "pending"
        for record in client.thread_records(include_terminal=True):
            if record.get("phase") not in {"merged", "deployed"}:
                continue
            merge_sha = str(record.get("merge_sha") or "")
            if record.get("phase") == "merged":
                if not merge_sha and record.get("pull_request") is not None:
                    merge_sha = str(
                        client.pull_merge_sha(int(record["pull_request"])) or ""
                    )
                if not merge_sha or not client.is_ancestor(
                    merge_sha, str(result["main_sha"])
                ):
                    # The record may have arrived after follow_release captured
                    # its verified revision. Leave it merged for the next release.
                    continue
            else:
                deployed_sha = str(record.get("deployed_sha") or "")
                if (
                    not merge_sha
                    or not deployed_sha
                    or not client.is_ancestor(merge_sha, deployed_sha)
                ):
                    continue
            deployed_sha = str(record.get("deployed_sha") or result["main_sha"])
            same_release = deployed_sha == str(result["main_sha"])
            ci_url = str(record.get("ci_url") or "") or (
                str((result.get("latest_ci") or {}).get("url") or "")
                if same_release
                else ""
            )
            deployment_url = str(record.get("deployment_url") or "") or (
                str((result.get("latest_deploy") or {}).get("url") or "")
                if same_release
                else ""
            )
            deployed_record = {
                **record,
                "merge_sha": merge_sha,
                "deployed_sha": deployed_sha,
                "ci_url": ci_url or None,
                "deployment_url": deployment_url or None,
            }
            pull_request = record.get("pull_request")
            notification_id = thread_notification_id(
                repository=client.repository,
                provider=str(record["provider"]),
                thread_id=str(record["thread_id"]),
                merge_sha=merge_sha,
                pull_request=(int(pull_request) if pull_request is not None else None),
            )
            if notification_id not in notification_states:
                notification = thread_deployment_notification(
                    repository=client.repository,
                    record=deployed_record,
                    release=result,
                    pull_request_details=pull_request_notification_details(
                        client,
                        pull_request,
                        pull_request_details_cache,
                    ),
                )
                record_pending_deployment_notification(client, notification)
                pending_notifications[notification_id] = notification
                notification_states[notification_id] = "pending"
            if notification_id in pending_notifications and (
                record.get("phase") == "merged" or not record.get("deployed_sha")
            ):
                client.record_thread(
                    ThreadRecord(
                        provider=str(record["provider"]),
                        thread_id=str(record["thread_id"]),
                        phase="deployed",
                        updated_at=utc_now(),
                        title=str(record.get("title") or "") or None,
                        branch=str(record.get("branch") or "") or None,
                        pull_request=record.get("pull_request"),
                        url=str(record.get("url") or "") or None,
                        merge_sha=merge_sha,
                        deployed_sha=deployed_sha,
                        ci_url=ci_url or None,
                        deployment_url=deployment_url or None,
                    )
                )
        notifications = list(pending_notifications.values())
        for notification in notifications:
            notify(client.config.pipeline, "thread-deployed", notification)
        result = {**result, "thread_notifications": notifications}
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
        # Revisit it while a merge still needs an outbox entry. A pending
        # native-thread receipt only needs coordinator retries when a webhook
        # is actually configured; otherwise the source-thread heartbeat owns
        # delivery and repeated scheduled followers cannot make progress.
        notifications = client.deployment_notifications(include_delivered=True)
        webhook_env = client.config.pipeline.webhook_url_env
        webhook_ready = bool(webhook_env and os.environ.get(webhook_env))
        has_open_notification = isinstance(notifications, list) and any(
            value.get("state") == "awaiting-verification"
            or (value.get("state") == "pending" and webhook_ready)
            for value in notifications
        )
        return has_open_notification or any(
            record.get("phase") == "merged"
            for record in client.thread_records(include_terminal=True)
        )
    return True


def should_settle_batch(client: GitHub, entries: list[QueueEntry]) -> bool:
    has_ready = any(
        client.config.queue_label in entry.labels and entry.state == "ready"
        for entry in entries
    )
    has_near_ready = any(
        (
            entry.repair_overlap_hold
            and client.config.blocked_label in entry.labels
        )
        or (
            client.config.queue_label not in entry.labels
            and client.config.blocked_label not in entry.labels
            and entry.state == "waiting"
        )
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
        if (
            entry.repair_overlap_hold
            and client.config.blocked_label in entry.labels
        )
        or (
            entry.state == "waiting"
            and client.config.queue_label not in entry.labels
            and client.config.blocked_label not in entry.labels
        )
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


def combine_drain_results(
    first: dict[str, Any], second: dict[str, Any]
) -> dict[str, Any]:
    batch_ids: list[str] = []
    for value in (first, second):
        candidates = list(value.get("batch_ids") or [])
        if not candidates and value.get("batch_id"):
            candidates = [str(value["batch_id"])]
        batch_ids.extend(
            candidate for candidate in candidates if candidate not in batch_ids
        )
    waiting = {
        int(value["number"]): value
        for result in (first, second)
        for value in result.get("waiting") or []
    }
    return {
        "batch_id": batch_ids[-1] if batch_ids else None,
        "batch_ids": batch_ids,
        "integration_required": list(first.get("integration_required") or [])
        + list(second.get("integration_required") or []),
        "merged": list(first.get("merged") or []) + list(second.get("merged") or []),
        "next_batch": list(second.get("next_batch") or []),
        "waiting": list(waiting.values()),
    }


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

    # An authorized PR may have landed outside the controller. Materialize its
    # durable release obligation before deciding whether an empty watermark
    # can safely establish a first-install baseline.
    reconciled_merges = reconcile_externally_merged_threads(client)

    # Once a batch reaches main, close admission until that exact cumulative
    # revision is live. Otherwise a busy queue can keep advancing main faster
    # than CI/deploy can verify it, starving an already-merged small change.
    release_completed_before_merge = False
    release_before_batch: dict[str, Any] | None = None
    if client.config.pipeline.hold_merges_while_releasing:
        workflow_runs = client.workflow_runs()
        if not isinstance(workflow_runs, list):
            workflow_runs = []
        current_main_sha = client.base_sha()
        release_before_merge = release_state(
            main_sha=current_main_sha,
            runs=workflow_runs,
            config=client.config.pipeline,
        )
        raw_watermark = client.verified_main_sha()
        release_already_verified = raw_watermark == current_main_sha
        has_release_owner = (
            not release_already_verified
            and release_before_merge.get("latest_ci") is not None
        )
        if not release_already_verified and any(
            value.get("merge_sha")
            and client.is_ancestor(str(value["merge_sha"]), current_main_sha)
            for value in reconciled_merges
        ):
            has_release_owner = True
        if (
            not release_already_verified
            and isinstance(raw_watermark, str)
            and raw_watermark != current_main_sha
        ):
            has_release_owner = True
        if not release_already_verified and not has_release_owner:
            thread_records = client.thread_records(include_terminal=True)
            if not isinstance(thread_records, list):
                thread_records = []
            has_release_owner = any(
                record.get("phase") == "merged"
                and bool(record.get("merge_sha"))
                and client.is_ancestor(
                    str(record["merge_sha"]),
                    str(release_before_merge["main_sha"]),
                )
                for record in thread_records
            )
        if not release_already_verified and not has_release_owner:
            notifications = client.deployment_notifications(include_delivered=True)
            if not isinstance(notifications, list):
                notifications = []
            has_release_owner = any(
                record.get("state") == "awaiting-verification"
                for record in notifications
            )
        if raw_watermark is None and not has_release_owner:
            # First installation (or registry recovery) has no trustworthy
            # prior boundary. Seed current main only when no exact run or
            # durable merged obligation owns it; historical runs for older
            # SHAs cannot make an unobservable release finish.
            client.record_verified_main(current_main_sha)
        release_is_verified = release_already_verified or (
            release_before_merge.get("state") == "verified"
        )
        if (
            not release_already_verified
            and release_is_verified
            and client.config.pipeline.verifications
        ):
            health = http_verifications(client.config.pipeline)
            release_before_merge = {
                **release_before_merge,
                "state": (
                    "verified"
                    if all(item["passed"] for item in health)
                    else "verify-failed"
                ),
                "verifications": health,
            }
            release_is_verified = release_before_merge["state"] == "verified"
        if release_is_verified:
            client.record_verified_main(current_main_sha)
        if has_release_owner and not release_is_verified:
            release = release_before_merge
            if follow:
                release = command_follow(
                    client,
                    timeout_seconds=timeout_seconds,
                    poll_seconds=10,
                    json_output=False,
                    emit=False,
                )
            if release.get("state") != "verified":
                result = {
                    "state": "release-held",
                    "release": release,
                    "promoted": {},
                    "promoted_integrations": [],
                    "drain": {},
                    "dispatched_ci": [],
                    "integrations": [],
                    "integration_checks": [],
                    "reconciled_merges": reconciled_merges,
                }
                print(json.dumps(result, indent=2, sort_keys=True))
                return result
            release_completed_before_merge = True
            release_before_batch = release

    def own_integration_checks(
        numbers: Iterable[int] | None = None,
    ) -> list[dict[str, Any]]:
        try:
            return settle_integration_checks(
                client,
                timeout_seconds=timeout_seconds,
                poll_seconds=10,
                numbers=numbers,
            )
        except QueueError as error:
            if client.config.pipeline.pause_on_failure:
                client.set_pipeline_control(
                    "paused", "integration CI ownership failed: " + str(error)
                )
            raise

    integration_checks = own_integration_checks()
    promoted_integrations = promote_integrations(
        client,
        known_checks_by_number={
            int(value["pull_request"]): dict(value.get("checks") or {})
            for value in integration_checks
        },
    )
    captured_entries: list[QueueEntry] = []
    promoted = command_promote(
        client,
        emit=False,
        captured_entries=captured_entries,
    )
    if client.config.pipeline.batch_settle_seconds and should_settle_batch(
        client, captured_entries
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

    drained: dict[str, Any] = {}
    queued_integrations: set[int] = set()
    if client.config.integration.mode == "all":
        queued_numbers = set(client.queued_numbers())
        queued_integrations = queued_numbers.intersection(
            client.integration_pull_request_numbers()
        )
        if queued_integrations:
            # A ready integration PR owns an earlier frozen source batch. Drain
            # those integration PRs first, while holding newly queued sources,
            # so the older integration cannot suppress all-mode integration of
            # the new work or let that work merge directly.
            integration_frozen = freeze_queue(
                client,
                held_numbers=queued_numbers - queued_integrations,
            )
            drained = command_drain(
                client,
                json_output=False,
                emit=False,
                initial_frozen=integration_frozen,
            )

    overlap_holds = near_ready_overlap_holds(client, captured_entries)
    promoted["held"] = [
        {
            "number": number,
            "overlapping_waiting": waiting,
            "reason": "waiting for overlapping deploy intents to finish gates",
        }
        for number, waiting in overlap_holds.items()
    ]
    merged_integrations = {
        int(value["number"]) for value in drained.get("merged") or []
    }
    if queued_integrations - merged_integrations:
        frozen = FreezeResult(None, [], [], [], [])
    else:
        frozen = freeze_queue(
            client,
            known_entries=captured_entries,
            held_numbers=set(overlap_holds),
        )
    integrations: list[dict[str, Any]] = []
    if frozen.batch is not None and client.config.integration.mode == "all":
        integration = client.create_integration_pull_request(
            batch=frozen.batch,
            entries=frozen.queue,
        )
        record_integration_conflict_repair(client, integration, frozen.queue)
        integrations.append(integration)
        deferred_drain: dict[str, Any] = {
            "batch_id": frozen.batch["batch_id"],
            "batch_ids": [frozen.batch["batch_id"]],
            "merged": [],
            "waiting": [],
            "integration_required": frozen.overlap_groups,
            "next_batch": [],
        }
        drained = (
            combine_drain_results(drained, deferred_drain)
            if drained
            else deferred_drain
        )
    else:
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
            integration = client.create_integration_pull_request(
                batch=frozen.batch,
                entries=overlap_entries,
            )
            record_integration_conflict_repair(client, integration, overlap_entries)
            integrations.append(integration)
    created_clean = [
        int(value["number"])
        for value in integrations
        if value.get("number") and not value.get("conflict")
    ]
    newly_promoted_integrations: list[int] = []
    if created_clean:
        # GITHUB_TOKEN-created pull requests do not emit a dependable recursive
        # pull_request/workflow_run chain. Dispatch and own their exact-head CI
        # under this coordinator before any source batch is allowed to drain.
        integration_checks.extend(own_integration_checks(created_clean))
        newly_promoted_integrations = promote_integrations(
            client,
            known_checks_by_number={
                int(value["pull_request"]): dict(value.get("checks") or {})
                for value in integration_checks
                if int(value["pull_request"]) in created_clean
            },
        )
        promoted_integrations = list(
            dict.fromkeys(promoted_integrations + newly_promoted_integrations)
        )

    if not drained:
        # Establish durable ownership for every overlapping source before any
        # independent member of the frozen batch is allowed to merge. A setup
        # or integration-CI failure leaves the batch intact and cannot strand
        # an already-merged partial batch before exact-main CI.
        drained = command_drain(
            client,
            json_output=False,
            emit=False,
            initial_frozen=frozen,
        )
    if newly_promoted_integrations and not drained.get("merged"):
        integration_frozen: FreezeResult | None = None
        if client.config.integration.mode == "all":
            queued_numbers = set(client.queued_numbers())
            integration_frozen = freeze_queue(
                client,
                held_numbers=queued_numbers - set(newly_promoted_integrations),
            )
        integration_drain = command_drain(
            client,
            json_output=False,
            emit=False,
            initial_frozen=integration_frozen,
        )
        drained = combine_drain_results(drained, integration_drain)
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
    release: dict[str, Any] | None = release_before_batch
    should_follow = bool(drained.get("merged"))
    if follow and not should_follow and not release_completed_before_merge:
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
        "integration_checks": integration_checks,
        "reconciled_merges": reconciled_merges,
        "release": release,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def command_control(client: GitHub, *, state: str, reason: str | None) -> None:
    client.set_pipeline_control(state, reason)
    print(f"DeployBot pipeline is {state}")


def command_unpause(
    client: GitHub,
    *,
    main_sha: str,
    control_id: str,
) -> None:
    control = client.pipeline_control()
    if control.get("state") != "paused":
        raise QueueError("DeployBot pipeline is no longer paused; refresh status")
    if str(control.get("control_id") or "") != control_id:
        raise QueueError("DeployBot pause record changed; refresh status")
    if str(control.get("main_sha") or "") != main_sha:
        raise QueueError("DeployBot pause belongs to a different main; refresh status")
    current_sha = client.base_sha()
    if current_sha != main_sha:
        raise QueueError(
            f"DeployBot main advanced from {main_sha} to {current_sha}; refresh status"
        )
    resume_control_id = client.set_pipeline_control(
        "running", None, resumes_control_id=control_id
    )
    refreshed = client.pipeline_control()
    if refreshed.get("state") != "running" or (
        refreshed.get("resumes_control_id") != control_id
    ) or refreshed.get("control_id") != resume_control_id:
        raise QueueError("DeployBot pause changed during unpause; refresh status")
    refreshed_main = client.base_sha()
    if refreshed_main != main_sha:
        latest_control = client.pipeline_control()
        if latest_control.get("state") == "running" and (
            latest_control.get("resumes_control_id") == control_id
        ):
            client.set_pipeline_control(
                "paused",
                f"main advanced during unpause from {main_sha} to {refreshed_main}",
                main_sha=refreshed_main,
                requires_control_id=resume_control_id,
            )
        raise QueueError(
            f"DeployBot main advanced from {main_sha} to {refreshed_main} "
            "during unpause; pipeline remains paused"
        )
    # This is the compare-and-set boundary. A main advance after this final
    # read occurs after the matching pause was successfully resumed and may be
    # the expected repair merge. The release-admission fence then blocks every
    # later batch until that newer exact main passes CI, deploy, and health
    # verification; binding `running` to the failed SHA forever would instead
    # re-pause the repair merge and strand takeover workers.
    print(f"DeployBot pipeline is running for recovered main {main_sha}")


def command_claim_release_repair(
    client: GitHub,
    *,
    provider: str,
    thread_id: str,
    thread_url: str | None,
    main_sha: str | None,
) -> dict[str, Any]:
    result = client.claim_release_repair(
        provider=provider,
        thread_id=thread_id,
        thread_url=thread_url,
        main_sha=main_sha,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def delivery_metrics(client: GitHub, *, limit: int) -> dict[str, Any]:
    pulls = client.recent_merged_pull_requests(limit)
    merged_times = [
        value
        for value in (parse_time(str(pull.get("merged_at") or "")) for pull in pulls)
        if value is not None
    ]
    earliest_merge = min(merged_times, default=None)
    deploy_runs = sorted(
        client.successful_workflow_runs(
            client.config.pipeline.deploy_workflows,
            limit=max(100, limit),
            since=earliest_merge,
        ),
        key=lambda item: str(item.get("updated_at") or ""),
    )
    comments_by_number = client.comments_for_pull_requests(
        int(pull["number"]) for pull in pulls
    )
    repair_logins = coordinator_logins(client) | set(client.trusted_logins)

    def sample(pull: dict[str, Any]) -> dict[str, Any]:
        number = int(pull["number"])
        comments = comments_by_number.get(number, [])
        intent = latest_intent(comments, client.trusted_logins)
        marker = queue_marker_for_client(client, comments)
        repair = latest_payload(comments, REPAIR_MARKER, repair_logins)
        repaired = bool(repair) and not repair_marker_is_transitional(repair)
        merged_at = str(pull.get("merged_at") or "") or None
        merge_sha = str(pull.get("merge_commit_sha") or "")
        live_at: str | None = None
        if merge_sha:
            merged_time = parse_time(merged_at)
            for run in deploy_runs:
                created_time = parse_time(str(run.get("created_at") or ""))
                deployed_sha = str(run.get("head_sha") or "")
                if merged_time and created_time:
                    if created_time < merged_time:
                        continue
                if deployed_sha and client.is_ancestor(merge_sha, deployed_sha):
                    live_at = str(run.get("updated_at") or "") or None
                    break
        requested_at = str((intent or {}).get("requested_at") or "") or None
        queued_at = str((marker or {}).get("queued_at") or "") or None
        return {
            "pull_request": number,
            "requested_at": requested_at,
            "queued_at": queued_at,
            "merged_at": merged_at,
            "live_at": live_at,
            "repaired": repaired,
            "request_to_queue_seconds": seconds_between(requested_at, queued_at),
            "queue_to_merge_seconds": seconds_between(queued_at, merged_at),
            "merge_to_live_seconds": seconds_between(merged_at, live_at),
            "request_to_live_seconds": seconds_between(requested_at, live_at),
        }

    if not pulls:
        samples: list[dict[str, Any]] = []
    else:
        with ThreadPoolExecutor(
            max_workers=min(client.config.pipeline.promotion_workers, len(pulls))
        ) as executor:
            samples = list(executor.map(sample, pulls))
    # Compare each stage against the operator's own speed targets so the
    # historical view answers "are PRs going out the door quickly enough?",
    # mirroring how `status` applies these same targets to live work.
    ready_target = client.config.pipeline.ready_to_merge_target_minutes * 60
    live_target = client.config.pipeline.merge_to_live_target_minutes * 60
    targets = {
        "queue_to_merge_seconds": ready_target,
        "merge_to_live_seconds": live_target,
        "request_to_live_seconds": ready_target + live_target,
    }
    summary = summarize_metrics(samples, targets=targets)
    repaired_merges = sum(1 for value in samples if value.get("repaired"))
    summary["reliability"] = {
        "merged_samples": len(samples),
        "first_pass_merges": len(samples) - repaired_merges,
        "repaired_merges": repaired_merges,
        "first_pass_rate": (
            (len(samples) - repaired_merges) / len(samples) if samples else None
        ),
    }
    return summary


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
    thread_update.add_argument(
        "--phase",
        required=True,
        choices=sorted(THREAD_PHASES - {"deployed"}),
    )
    thread_update.add_argument("--title")
    thread_update.add_argument("--branch")
    thread_update.add_argument("--pr", type=int, dest="pull_request")
    thread_update.add_argument("--url")
    thread_acknowledge = thread_commands.add_parser(
        "acknowledge", help="mark a delivered native-thread notification complete"
    )
    thread_acknowledge.add_argument("--provider", required=True)
    thread_acknowledge.add_argument("--thread-id", required=True)
    thread_acknowledge.add_argument("--notification-id", required=True)
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
    unpause = subparsers.add_parser(
        "unpause", help="resume the exact revalidated failed release"
    )
    unpause.add_argument("--sha", required=True, dest="main_sha")
    unpause.add_argument("--control-id", required=True)
    claim_repair = subparsers.add_parser(
        "claim-release-repair",
        help="atomically claim ownership of the current failed release",
    )
    claim_repair.add_argument("--provider", required=True)
    claim_repair.add_argument("--thread-id", required=True)
    claim_repair.add_argument("--thread-url")
    claim_repair.add_argument("--sha", dest="main_sha")
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
            if arguments.thread_command == "acknowledge":
                command_thread_acknowledge(
                    client,
                    provider=arguments.provider,
                    thread_id=arguments.thread_id,
                    notification_id=arguments.notification_id,
                )
            else:
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
            command_unpause(
                client,
                main_sha=arguments.main_sha,
                control_id=arguments.control_id,
            )
        elif arguments.command == "claim-release-repair":
            command_claim_release_repair(
                client,
                provider=arguments.provider,
                thread_id=arguments.thread_id,
                thread_url=arguments.thread_url,
                main_sha=arguments.main_sha,
            )
        elif arguments.command == "metrics":
            if arguments.limit < 1:
                raise QueueError("--limit must be positive")
            value = delivery_metrics(client, limit=arguments.limit)
            if arguments.json_output:
                print(json.dumps(value, indent=2, sort_keys=True))
            else:
                print(f"delivery samples: {value['sample_count']}")
                reliability = value.get("reliability") or {}
                if reliability.get("merged_samples"):
                    rate = reliability.get("first_pass_rate")
                    rate_text = f"{rate * 100:.0f}%" if rate is not None else "n/a"
                    print(
                        "first-pass merges: "
                        f"{reliability['first_pass_merges']}/"
                        f"{reliability['merged_samples']} ({rate_text})"
                    )
                for name, summary in value.items():
                    if not name.endswith("_seconds"):
                        continue
                    line = (
                        f"{name}: p50={summary['p50']}s "
                        f"p95={summary['p95']}s max={summary['max']}s"
                    )
                    if summary.get("target_seconds") is not None:
                        within = summary.get("within_target_rate")
                        within_text = (
                            f"{within * 100:.0f}%" if within is not None else "n/a"
                        )
                        line += (
                            f" target={summary['target_seconds']}s "
                            f"within={within_text}"
                        )
                    print(line)
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
