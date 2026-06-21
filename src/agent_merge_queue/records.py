"""Strict GitHub-comment records used by DeployBot's delivery controller."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable


THREAD_PREFIX = "deploybot-thread:v1"
INTENT_PREFIX = "deploybot-intent:v1"
REPAIR_PREFIX = "deploybot-repair:v1"
CONTROL_PREFIX = "deploybot-control:v1"
INTEGRATION_PREFIX = "deploybot-integration:v1"

THREAD_PHASES = {
    "working",
    "pr-draft",
    "pr-review",
    "ready",
    "deploy-requested",
    "queued",
    "merged",
    "blocked",
    "completed",
    "abandoned",
}


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _marker(prefix: str, prose: str) -> re.Pattern[str]:
    return re.compile(
        rf"\A<!--\s*{re.escape(prefix)}\s+(\{{.*\}})\s*-->\n"
        rf"{re.escape(prose)}\s*\Z",
        re.DOTALL,
    )


THREAD_MARKER = _marker(THREAD_PREFIX, "Recorded DeployBot thread metadata.")
INTENT_MARKER = _marker(INTENT_PREFIX, "Recorded DeployBot deploy intent.")
REPAIR_MARKER = _marker(REPAIR_PREFIX, "Recorded DeployBot repair handoff.")
CONTROL_MARKER = _marker(CONTROL_PREFIX, "Recorded DeployBot pipeline control.")
INTEGRATION_MARKER = _marker(
    INTEGRATION_PREFIX, "Recorded DeployBot integration pull request."
)


def marker_body(prefix: str, payload: dict[str, Any], prose: str) -> str:
    return f"<!-- {prefix} {json.dumps(payload, sort_keys=True)} -->\n{prose}"


def _payload(body: str, pattern: re.Pattern[str]) -> dict[str, Any] | None:
    match = pattern.search(body)
    if not match:
        return None
    try:
        value = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) and value.get("schema") == 1 else None


def comment_login(comment: dict[str, Any]) -> str:
    return str((comment.get("user") or {}).get("login") or "").lower()


def _comment_key(comment: dict[str, Any], index: int) -> tuple[str, int, int]:
    try:
        comment_id = int(comment.get("id") or 0)
    except (TypeError, ValueError):
        comment_id = 0
    return str(comment.get("created_at") or ""), comment_id, index


def latest_payload(
    comments: Iterable[dict[str, Any]],
    pattern: re.Pattern[str],
    trusted_logins: Iterable[str],
) -> dict[str, Any] | None:
    trusted = {value.lower() for value in trusted_logins}
    found: list[tuple[tuple[str, int, int], dict[str, Any]]] = []
    for index, comment in enumerate(comments):
        if comment_login(comment) not in trusted:
            continue
        value = _payload(str(comment.get("body") or ""), pattern)
        if value is not None:
            found.append((_comment_key(comment, index), value))
    return max(found, key=lambda item: item[0])[1] if found else None


@dataclass(frozen=True)
class ThreadRecord:
    provider: str
    thread_id: str
    phase: str
    updated_at: str
    title: str | None = None
    branch: str | None = None
    pull_request: int | None = None
    url: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def thread_record_body(record: ThreadRecord) -> str:
    if record.phase not in THREAD_PHASES:
        raise ValueError(f"unsupported thread phase: {record.phase}")
    if not record.provider.strip() or not record.thread_id.strip():
        raise ValueError("provider and thread_id are required")
    payload = {"schema": 1, **record.as_dict()}
    return marker_body(THREAD_PREFIX, payload, "Recorded DeployBot thread metadata.")


def latest_thread_records(
    comments: Iterable[dict[str, Any]],
    trusted_logins: Iterable[str],
    *,
    active_hours: int,
    now: datetime | None = None,
) -> list[ThreadRecord]:
    trusted = {value.lower() for value in trusted_logins}
    latest: dict[tuple[str, str], tuple[tuple[str, int, int], ThreadRecord]] = {}
    current = now or datetime.now(timezone.utc)
    for index, comment in enumerate(comments):
        if comment_login(comment) not in trusted:
            continue
        value = _payload(str(comment.get("body") or ""), THREAD_MARKER)
        if value is None or value.get("phase") not in THREAD_PHASES:
            continue
        try:
            record = ThreadRecord(
                provider=str(value["provider"]),
                thread_id=str(value["thread_id"]),
                phase=str(value["phase"]),
                updated_at=str(value["updated_at"]),
                title=str(value["title"]) if value.get("title") else None,
                branch=str(value["branch"]) if value.get("branch") else None,
                pull_request=(
                    int(value["pull_request"])
                    if value.get("pull_request") is not None
                    else None
                ),
                url=str(value["url"]) if value.get("url") else None,
            )
        except (KeyError, TypeError, ValueError):
            continue
        timestamp = parse_time(record.updated_at)
        if timestamp is None:
            continue
        key = (record.provider.lower(), record.thread_id)
        candidate = (_comment_key(comment, index), record)
        if key not in latest or candidate[0] > latest[key][0]:
            latest[key] = candidate
    terminal = {"completed", "abandoned"}
    result = []
    for _, record in latest.values():
        timestamp = parse_time(record.updated_at)
        age_hours = (
            (current - timestamp).total_seconds() / 3600 if timestamp else 999999
        )
        if record.phase not in terminal and age_hours <= active_hours:
            result.append(record)
    return sorted(
        result, key=lambda value: (value.updated_at, value.provider), reverse=True
    )


def intent_body(
    *,
    intent_id: str,
    state: str,
    requested_at: str,
    requested_head: str,
    provider: str | None = None,
    thread_id: str | None = None,
    thread_url: str | None = None,
    parent_intent_id: str | None = None,
) -> str:
    if state not in {"requested", "cancelled"}:
        raise ValueError(f"unsupported intent state: {state}")
    payload: dict[str, Any] = {
        "intent_id": intent_id,
        "requested_at": requested_at,
        "requested_head": requested_head,
        "schema": 1,
        "state": state,
    }
    for key, value in (
        ("provider", provider),
        ("thread_id", thread_id),
        ("thread_url", thread_url),
        ("parent_intent_id", parent_intent_id),
    ):
        if value:
            payload[key] = value
    return marker_body(INTENT_PREFIX, payload, "Recorded DeployBot deploy intent.")


def latest_intent(
    comments: Iterable[dict[str, Any]], trusted_logins: Iterable[str]
) -> dict[str, Any] | None:
    value = latest_payload(comments, INTENT_MARKER, trusted_logins)
    if value is None or value.get("state") not in {"requested", "cancelled"}:
        return None
    return value


def repair_body(payload: dict[str, Any]) -> str:
    value = {"schema": 1, **payload}
    return marker_body(REPAIR_PREFIX, value, "Recorded DeployBot repair handoff.")


def control_body(*, state: str, reason: str | None = None) -> str:
    if state not in {"running", "paused"}:
        raise ValueError(f"unsupported pipeline control state: {state}")
    payload = {"recorded_at": utc_now(), "schema": 1, "state": state}
    if reason:
        payload["reason"] = reason
    return marker_body(CONTROL_PREFIX, payload, "Recorded DeployBot pipeline control.")


def integration_body(payload: dict[str, Any]) -> str:
    value = {"schema": 1, **payload}
    return marker_body(
        INTEGRATION_PREFIX,
        value,
        "Recorded DeployBot integration pull request.",
    )
