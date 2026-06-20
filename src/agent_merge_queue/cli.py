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
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from . import __version__
from .config import ConfigError, QueueConfig, initialize_config, load_config
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
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
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
        found.append(
            (str(comment.get("created_at") or ""), comment_id, index, value)
        )
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


def queue_state_body(
    state: str,
    head_sha: str,
    *,
    queued_at: str | None,
    reason: str | None = None,
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
    if reason:
        marker["reason"] = reason
    return (
        f"<!-- {STATE_MARKER_PREFIX} {json.dumps(marker, sort_keys=True)} -->\n"
        f"Recorded merge queue state `{state}` on `{head_sha}`."
    )


def structured_dependencies(body: str, directive: str) -> list[int]:
    pattern = re.compile(
        rf"^{re.escape(directive)}:\s*(.*?)\s*$", re.I | re.MULTILINE
    )
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
        "dependencies": {
            str(entry.number): entry.dependencies for entry in entries
        },
        "heads": {str(entry.number): entry.head_sha for entry in entries},
        "pull_requests": [entry.number for entry in entries],
        "schema": 1,
        "source_paths": {
            str(entry.number): entry.source_paths for entry in entries
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
        arguments.extend(
            ["--repo", self.repository, "--json", "number"]
        )
        return int(self._json(*arguments)["number"])

    def ensure_labels(self) -> None:
        labels = (
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
        )
        for name, color, description in labels:
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

    def _paged_api(self, endpoint: str) -> list[dict[str, Any]]:
        data = self._json("api", "--paginate", "--slurp", endpoint)
        pages = data if isinstance(data, list) else []
        values: list[dict[str, Any]] = []
        for page in pages:
            if not isinstance(page, list):
                raise QueueError(f"unexpected GitHub response for {endpoint}")
            values.extend(item for item in page if isinstance(item, dict))
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

        comments = self.comments(number)
        reviews = self.reviews(number)
        needs_threads = any(
            provider.kind == "bot" and provider.require_resolved_threads
            for provider in self.config.review_providers
        )
        threads = self.review_threads(number) if needs_threads else []
        marker = latest_marker(
            comments,
            self.trusted_logins,
        )
        head_sha = str(pull["headRefOid"])
        checks = check_states(pull.get("statusCheckRollup") or [])
        changed_files = self.files(number)
        source_paths: list[str] = []
        generated_paths: list[str] = []
        for value in changed_files:
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

        body = str(pull.get("body") or "")
        entry = QueueEntry(
            number=int(pull["number"]),
            title=str(pull["title"]),
            url=str(pull["url"]),
            head_sha=head_sha,
            queued_head_sha=str(marker.get("head_sha")) if marker else None,
            queued_at=str(marker.get("queued_at")) if marker else None,
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
        )
        entry.classify(
            self.config,
            require_marker=require_marker,
            allow_blocked_label=allow_blocked_label,
        )
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
            key=lambda entry: (entry.queued_at or "9999", entry.number),
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
        return {
            str(label.get("name") or "") for label in data.get("labels") or []
        }

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

    def merge(self, number: int, head_sha: str) -> str:
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
        if (
            self.config.queue_label not in labels
            or self.config.blocked_label in labels
        ):
            raise QueueError(f"PR #{number} queue authorization was revoked")
        marker = latest_marker(self.comments(number), self.trusted_logins)
        if (
            not marker
            or marker.get("state") != "queued"
            or marker.get("head_sha") != head_sha
        ):
            raise QueueError(f"PR #{number} durable queue authorization was revoked")

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
            raise QueueError(str(result.get("message") or f"PR #{number} was not merged"))
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
            f"{verdict.provider}: {verdict.state}"
            for verdict in entry.review_verdicts
        )
        review = review or "checks only"
        detail = "; ".join(entry.reasons or []) or "all merge gates passed"
        print(
            f"{position}. #{entry.number} {entry.state} "
            f"({review}) - {detail}"
        )
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


def command_enqueue(client: GitHub, selector: str | None) -> None:
    number = client.resolve_pr(selector)
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

    comments = client.comments(number)
    previous = latest_marker(
        comments,
        client.trusted_logins,
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


def freeze_queue(client: GitHub) -> FreezeResult:
    all_entries = client.queue()
    entries, blocked_entries = split_blocked_entries(
        all_entries, client.config.blocked_label
    )
    if not entries:
        return FreezeResult(None, [], blocked_entries, [], [])

    comments = {entry.number: client.comments(entry.number) for entry in entries}
    latest = {
        number: latest_batch_marker(
            values,
            client.coordinator_logins,
        )
        for number, values in comments.items()
    }
    completed = {
        batch_id
        for values in comments.values()
        for batch_id in completed_batch_ids(values, client.coordinator_logins)
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
            client.coordinator_logins,
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
    previous = latest_marker(client.comments(number), client.trusted_logins)
    if (
        not previous
        or previous.get("state") != "queued"
        or previous.get("head_sha") != entry.head_sha
    ):
        raise QueueError(f"PR #{number} does not have current queue authorization")
    labels = client.labels(number)
    if client.config.queue_label not in labels:
        raise QueueError(f"PR #{number} is not in the merge queue")
    if client.config.blocked_label not in labels:
        client.add_label(number, client.config.blocked_label)
    client.comment(
        number,
        queue_state_body(
            "blocked",
            entry.head_sha,
            queued_at=str(previous.get("queued_at") or "") or None,
            reason=reason,
        ),
    )
    print(f"blocked PR #{number}: {reason}")


def command_unblock(client: GitHub, selector: str | None) -> None:
    number = client.resolve_pr(selector)
    entry = client.snapshot(number, require_marker=False, allow_blocked_label=True)
    previous = latest_marker(client.comments(number), client.trusted_logins)
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
    previous = latest_marker(client.comments(number), client.trusted_logins)
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
            reason=reason,
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
) -> str:
    number = client.resolve_pr(selector)
    batch = latest_batch_marker(
        client.comments(number),
        client.coordinator_logins,
        batch_id=batch_id,
    )
    if batch is None:
        raise QueueError(f"PR #{number} has no trusted marker for batch {batch_id}")
    frozen = {int(value) for value in batch.get("pull_requests") or []}
    if number not in frozen:
        raise QueueError(f"PR #{number} is not a member of batch {batch_id}")
    entries = client.queue()
    frozen_entries = [value for value in entries if value.number in frozen]
    entry = next((value for value in frozen_entries if value.number == number), None)
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

    peers = batch_overlap_peers(
        batch, number, {value.number for value in frozen_entries}
    )
    if peers:
        peers = ", ".join(
            f"#{value}" for value in peers
        )
        raise QueueError(
            f"PR #{number} overlaps {peers}; create one cumulative integration PR"
        )

    expected_dependencies = sorted(
        int(value)
        for value in (batch.get("dependencies") or {}).get(str(number), [])
    )
    if entry.dependencies != expected_dependencies:
        raise QueueError(f"PR #{number} dependencies changed after batch freeze")
    missing_dependencies = [
        value for value in expected_dependencies if not client.dependency_is_merged(value)
    ]
    if missing_dependencies:
        raise QueueError(
            "unmerged dependencies: "
            + ", ".join(f"#{value}" for value in missing_dependencies)
        )

    fresh = client.snapshot(number)
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

    merge_sha = client.merge(number, fresh.head_sha)
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
        number
        for group in frozen.overlap_groups
        for number in group["pull_requests"]
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
                    client, str(number), batch_id, emit=False
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
    unmerged = [
        entry for entry in frozen.queue if entry.number not in merged_numbers
    ]
    complete_batch(client, frozen.batch, unmerged)
    return result, unmerged


def command_drain(client: GitHub, *, json_output: bool) -> dict[str, Any]:
    batch_ids: list[str] = []
    merged: list[dict[str, Any]] = []
    waiting_by_number: dict[int, dict[str, Any]] = {}
    integration_by_members: dict[tuple[int, ...], dict[str, Any]] = {}
    next_batch: list[int] = []

    while True:
        frozen = freeze_queue(client)
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
            print(
                "next batch: "
                + ", ".join(f"#{number}" for number in next_batch)
            )
        if not batch_ids:
            print("merge queue is empty")
    return result


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
    inspect = subparsers.add_parser("inspect", help="evaluate one PR without queueing it")
    inspect.add_argument("pr", nargs="?", help="PR number, URL, or branch")
    inspect.add_argument("--json", action="store_true", dest="json_output")
    plan = subparsers.add_parser("plan", help="show the current ordered queue")
    plan.add_argument("--json", action="store_true", dest="json_output")
    freeze = subparsers.add_parser("freeze", help="persist one exact queue pass")
    freeze.add_argument("--json", action="store_true", dest="json_output")
    drain = subparsers.add_parser(
        "drain", help="freeze and merge every independent ready queue item"
    )
    drain.add_argument("--json", action="store_true", dest="json_output")

    for name in ("enqueue", "unblock"):
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
        config = load_config(arguments.config)
        client = GitHub(config, arguments.repository)
        if arguments.command == "ensure-labels":
            client.ensure_labels()
            print("merge queue labels are ready")
        elif arguments.command == "inspect":
            command_inspect(client, arguments.pr, json_output=arguments.json_output)
        elif arguments.command == "plan":
            print_plan(client.queue(), json_output=arguments.json_output)
        elif arguments.command == "freeze":
            command_freeze(client, json_output=arguments.json_output)
        elif arguments.command == "drain":
            command_drain(client, json_output=arguments.json_output)
        elif arguments.command == "enqueue":
            command_enqueue(client, arguments.pr)
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
