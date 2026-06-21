"""Normalize review services into one fail-closed verdict model."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any, Iterable

from .config import ReviewProviderConfig


@dataclass(frozen=True)
class ReviewVerdict:
    provider: str
    state: str
    reasons: tuple[str, ...] = ()
    score: int | None = None

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["reasons"] = list(self.reasons)
        return value


def normalize_login(login: str | None) -> str:
    return (login or "").lower().removesuffix("[bot]")


def _check_verdict(
    policy: ReviewProviderConfig, checks: dict[str, str]
) -> ReviewVerdict:
    status = checks.get(str(policy.check_name))
    if status == "passed":
        return ReviewVerdict(policy.name, "passed")
    if status == "failed":
        return ReviewVerdict(policy.name, "blocked", (f"{policy.check_name} failed",))
    return ReviewVerdict(
        policy.name, "waiting", (f"{policy.check_name} is not complete",)
    )


def _approval_verdict(
    policy: ReviewProviderConfig,
    head_sha: str,
    reviews: Iterable[dict[str, Any]],
) -> ReviewVerdict:
    latest: dict[str, tuple[str, int, str]] = {}
    for index, review in enumerate(reviews):
        if str(review.get("commit_id") or "").lower() != head_sha.lower():
            continue
        login = normalize_login(str((review.get("user") or {}).get("login") or ""))
        if not login:
            continue
        timestamp = str(review.get("submitted_at") or "")
        candidate = (timestamp, index, str(review.get("state") or "").upper())
        if login not in latest or candidate[:2] > latest[login][:2]:
            latest[login] = candidate
    approved = {login for login, value in latest.items() if value[2] == "APPROVED"}
    allowed = {normalize_login(value) for value in policy.allowed_reviewers}
    approved.intersection_update(allowed)
    if len(approved) >= policy.minimum_approvals:
        return ReviewVerdict(policy.name, "passed")
    return ReviewVerdict(
        policy.name,
        "waiting",
        (f"{len(approved)}/{policy.minimum_approvals} exact-head approvals complete",),
    )


def _latest_score(
    policy: ReviewProviderConfig,
    head_sha: str,
    comments: Iterable[dict[str, Any]],
) -> int | None:
    if policy.minimum_score is None:
        return None
    if not policy.score_pattern:
        return None
    pattern = re.compile(policy.score_pattern, re.I)
    found: list[tuple[str, int]] = []
    expected_login = normalize_login(policy.login)
    for comment in comments:
        login = normalize_login(str((comment.get("user") or {}).get("login") or ""))
        body = str(comment.get("body") or "")
        match = pattern.search(body)
        if login != expected_login or not match or head_sha.lower() not in body.lower():
            continue
        try:
            score = int(match.group(1))
        except (IndexError, TypeError, ValueError):
            continue
        found.append((str(comment.get("created_at") or ""), score))
    return max(found, key=lambda item: item[0])[1] if found else None


def _bot_verdict(
    policy: ReviewProviderConfig,
    head_sha: str,
    checks: dict[str, str],
    comments: Iterable[dict[str, Any]],
    reviews: Iterable[dict[str, Any]],
    threads: Iterable[dict[str, Any]],
) -> ReviewVerdict:
    waiting: list[str] = []
    blocked: list[str] = []
    expected_login = normalize_login(policy.login)
    if policy.check_name:
        check = _check_verdict(policy, checks)
        if check.state == "waiting":
            waiting.extend(check.reasons)
        elif check.state == "blocked":
            blocked.extend(check.reasons)
    if policy.require_formal_review and not any(
        normalize_login(str((review.get("user") or {}).get("login") or ""))
        == expected_login
        and str(review.get("commit_id") or "").lower() == head_sha.lower()
        for review in reviews
    ):
        waiting.append(f"{policy.name} has not reviewed the current head")

    score = _latest_score(policy, head_sha, comments)
    if policy.minimum_score is not None:
        if score is None:
            waiting.append(f"{policy.name} score is missing for the current head")
        elif score < policy.minimum_score:
            blocked.append(
                f"{policy.name} score {score} is below {policy.minimum_score}"
            )

    if policy.require_resolved_threads:
        unresolved = 0
        for thread in threads:
            comments_value = thread.get("comments") or {}
            nodes = comments_value.get("nodes") or []
            root = nodes[0] if nodes else {}
            author = root.get("author") or {}
            if (
                normalize_login(str(author.get("login") or "")) == expected_login
                and not thread.get("isResolved")
                and not thread.get("isOutdated")
            ):
                unresolved += 1
        if unresolved:
            blocked.append(f"{unresolved} unresolved {policy.name} thread(s)")

    if blocked:
        return ReviewVerdict(policy.name, "blocked", tuple(blocked + waiting), score)
    if waiting:
        return ReviewVerdict(policy.name, "waiting", tuple(waiting), score)
    return ReviewVerdict(policy.name, "passed", score=score)


def evaluate_reviews(
    policies: Iterable[ReviewProviderConfig],
    *,
    head_sha: str,
    checks: dict[str, str],
    comments: Iterable[dict[str, Any]],
    reviews: Iterable[dict[str, Any]],
    threads: Iterable[dict[str, Any]],
) -> tuple[ReviewVerdict, ...]:
    comments = list(comments)
    reviews = list(reviews)
    threads = list(threads)
    verdicts: list[ReviewVerdict] = []
    for policy in policies:
        if policy.kind == "check":
            verdicts.append(_check_verdict(policy, checks))
        elif policy.kind == "github-approvals":
            verdicts.append(_approval_verdict(policy, head_sha, reviews))
        elif policy.kind == "bot":
            verdicts.append(
                _bot_verdict(
                    policy,
                    head_sha,
                    checks,
                    comments,
                    reviews,
                    threads,
                )
            )
        else:  # pragma: no cover - config validation owns this boundary.
            raise ValueError(f"unsupported review provider: {policy.kind}")
    return tuple(verdicts)
