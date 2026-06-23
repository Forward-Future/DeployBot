"""Delivery status, release following, notifications, and timing metrics."""

from __future__ import annotations

import json
import math
import os
import time
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .config import PipelineConfig
from .records import parse_time


def release_admitted(state: str, admit_gate: str) -> bool:
    """Return whether a healthy release state permits another merge."""
    admitted = {"verified"}
    if admit_gate == "ci-passed":
        admitted.update({"awaiting-deploy", "deploying"})
    elif admit_gate == "merged":
        admitted.update({"testing", "awaiting-deploy", "deploying"})
    return state in admitted


def workflow_run(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": run.get("id"),
        "name": run.get("name"),
        "head_sha": run.get("head_sha"),
        "status": run.get("status"),
        "conclusion": run.get("conclusion"),
        "event": run.get("event"),
        "created_at": run.get("created_at"),
        "updated_at": run.get("updated_at"),
        "url": run.get("html_url"),
    }


def latest_run(
    runs: Iterable[dict[str, Any]], names: Iterable[str], head_sha: str
) -> dict[str, Any] | None:
    allowed = set(names)
    matching = [
        run
        for run in runs
        if str(run.get("name") or "") in allowed
        and str(run.get("head_sha") or "") == head_sha
    ]
    return max(
        matching,
        key=lambda run: (str(run.get("created_at") or ""), int(run.get("id") or 0)),
        default=None,
    )


def release_state(
    *, main_sha: str, runs: list[dict[str, Any]], config: PipelineConfig
) -> dict[str, Any]:
    ci = latest_run(runs, config.ci_workflows, main_sha)
    substantive_ci = latest_run(
        [
            run
            for run in runs
            if not (
                str(run.get("status") or "") == "completed"
                and str(run.get("conclusion") or "") == "cancelled"
            )
        ],
        config.ci_workflows,
        main_sha,
    )
    # CI triggered by a merge and CI explicitly dispatched by DeployBot can
    # race on the same commit. Workflow concurrency may cancel whichever run
    # GitHub created last, but that duplicate cancellation cannot invalidate a
    # completed substantive run for the identical source tree. Preserve the
    # newest non-cancelled result so real later failures and active reruns stay
    # authoritative.
    if (
        substantive_ci is not None
        and str((ci or {}).get("status") or "") == "completed"
        and str((ci or {}).get("conclusion") or "") == "cancelled"
    ):
        ci = substantive_ci
    # A workflow_run deployment commonly starts for pull-request CI and then
    # skips itself because the upstream run was not exact main. GitHub reports
    # the downstream run against the default-branch SHA, so it can otherwise
    # hide a real deployment (or look like a deployment failure) for that same
    # revision. A skipped run did not attempt a release and is not evidence.
    ci_fence = parse_time(
        str((ci or {}).get("updated_at") or (ci or {}).get("created_at") or "")
    )
    eligible_deploys = [
        run
        for run in runs
        if not (
            str(run.get("status") or "") == "completed"
            and str(run.get("conclusion") or "") == "skipped"
        )
        and (
            ci_fence is None
            or (
                (created_at := parse_time(str(run.get("created_at") or "")))
                is not None
                and created_at >= ci_fence
            )
        )
    ]
    deploy = latest_run(
        eligible_deploys,
        config.deploy_workflows,
        main_sha,
    )
    successful_deploy = latest_run(
        [
            run
            for run in eligible_deploys
            if str(run.get("status") or "") == "completed"
            and str(run.get("conclusion") or "") == "success"
        ],
        config.deploy_workflows,
        main_sha,
    )
    # Successful exact-main release evidence is durable. A later duplicate
    # dispatch may be cancelled by workflow concurrency, but it cannot make an
    # already verified revision become undeployed.
    if (
        successful_deploy is not None
        and str((deploy or {}).get("status") or "") == "completed"
        and str((deploy or {}).get("conclusion") or "") == "cancelled"
    ):
        deploy = successful_deploy
    active_ci = [
        workflow_run(run)
        for run in runs
        if str(run.get("name") or "") in set(config.ci_workflows)
        and str(run.get("status") or "") != "completed"
    ]
    active_deploys = [
        workflow_run(run)
        for run in runs
        if str(run.get("name") or "") in set(config.deploy_workflows)
        and str(run.get("status") or "") != "completed"
    ]
    ci_status = str((ci or {}).get("status") or "")
    ci_conclusion = str((ci or {}).get("conclusion") or "")
    deploy_status = str((deploy or {}).get("status") or "")
    deploy_conclusion = str((deploy or {}).get("conclusion") or "")
    if ci is None or ci_status != "completed":
        state = "testing"
    elif ci_conclusion != "success":
        state = "ci-failed"
    elif deploy is None:
        state = "awaiting-deploy"
    elif deploy_status != "completed":
        state = "deploying"
    elif deploy_conclusion != "success":
        state = "deploy-failed"
    else:
        state = "verified"
    return {
        "state": state,
        "main_sha": main_sha,
        "latest_ci": workflow_run(ci) if ci else None,
        "latest_deploy": workflow_run(deploy) if deploy else None,
        "active_ci": active_ci,
        "active_deployments": active_deploys,
    }


def http_verifications(config: PipelineConfig) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for verification in config.verifications:
        request = Request(
            verification.url,
            headers={"User-Agent": "DeployBot/verification"},
        )
        status: int | None = None
        error: str | None = None
        try:
            with urlopen(request, timeout=10) as response:  # noqa: S310
                status = int(response.status)
        except HTTPError as exc:
            status = int(exc.code)
        except (OSError, URLError) as exc:
            error = str(exc)
        results.append(
            {
                "name": verification.name,
                "url": verification.url,
                "expected_status": verification.expected_status,
                "status": status,
                "passed": status == verification.expected_status,
                "error": error,
            }
        )
    return results


def notify(config: PipelineConfig, event: str, payload: dict[str, Any]) -> bool:
    env_name = config.webhook_url_env
    if not env_name:
        return False
    url = os.environ.get(env_name)
    if not url:
        return False
    body = json.dumps({"event": event, **payload}, sort_keys=True).encode()
    request = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "DeployBot"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=10):  # noqa: S310
            return True
    except (HTTPError, OSError, URLError):
        # GitHub records remain the durable source of truth. Notifications are
        # a convenience and must never block or roll back a safe queue action.
        return False


def seconds_between(start: str | None, end: str | None) -> float | None:
    left = parse_time(start)
    right = parse_time(end)
    if left is None or right is None or right < left:
        return None
    return (right - left).total_seconds()


def percentile(values: Iterable[float], fraction: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def summarize_metrics(samples: list[dict[str, Any]]) -> dict[str, Any]:
    stages = (
        "request_to_queue_seconds",
        "queue_to_merge_seconds",
        "merge_to_live_seconds",
        "request_to_live_seconds",
    )
    summary: dict[str, Any] = {"sample_count": len(samples), "samples": samples}
    for stage in stages:
        values = [
            float(sample[stage]) for sample in samples if sample.get(stage) is not None
        ]
        summary[stage] = {
            "count": len(values),
            "p50": percentile(values, 0.50),
            "p95": percentile(values, 0.95),
            "max": max(values) if values else None,
        }
    return summary


def follow_release(
    client: Any,
    *,
    timeout_seconds: int,
    poll_seconds: int,
    admit_gate: str = "verified",
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    observed_sha = ""
    last_verifications: list[dict[str, Any]] = []
    dispatched_deployments: list[dict[str, Any]] = []
    dispatched_for: set[tuple[str, int]] = set()
    failed_ci_key: tuple[str, int] | None = None
    failed_ci_deadline = 0.0
    while True:
        main_sha = client.base_sha()
        runs = client.workflow_runs()
        value = release_state(
            main_sha=main_sha, runs=runs, config=client.config.pipeline
        )
        observed_sha = main_sha
        clock: float | None = None
        if value["state"] == "ci-failed":
            ci = value.get("latest_ci") or {}
            key = (main_sha, int(ci.get("id") or 0))
            clock = time.monotonic()
            if key != failed_ci_key:
                failed_ci_key = key
                failed_ci_deadline = (
                    clock + client.config.pipeline.ci_failure_grace_seconds
                )
            if (
                not client.config.pipeline.ci_failure_grace_seconds
                or clock >= failed_ci_deadline
            ):
                return {
                    **value,
                    "dispatched_deployments": dispatched_deployments,
                    "verifications": [],
                }
        elif value["state"] == "deploy-failed":
            return {
                **value,
                "dispatched_deployments": dispatched_deployments,
                "verifications": [],
            }
        if value["state"] == "awaiting-deploy":
            ci = value.get("latest_ci") or {}
            ci_id = int(ci.get("id") or 0)
            key = (main_sha, ci_id)
            # Workflows launched with github.token do not reliably emit the
            # workflow_run handoff that repositories usually use for deploys.
            # Dispatch the configured deployment explicitly, carrying the
            # exact successful CI identity into the protected workflow.
            if ci.get("event") == "workflow_dispatch" and key not in dispatched_for:
                dispatched_deployments.extend(
                    client.dispatch_deploy_workflows(ci_run=ci)
                )
                dispatched_for.add(key)
        if value["state"] != "verified" and release_admitted(
            str(value["state"]), admit_gate
        ):
            # The configured admission milestone has passed. Hand control back
            # so independent ready work can merge; later release events (or the
            # scheduled fallback) continue from durable GitHub state.
            return {
                **value,
                "dispatched_deployments": dispatched_deployments,
                "verifications": [],
            }
        if value["state"] == "verified":
            checks = http_verifications(client.config.pipeline)
            last_verifications = checks
            if all(item["passed"] for item in checks):
                return {
                    **value,
                    "dispatched_deployments": dispatched_deployments,
                    "verifications": checks,
                }
        if (clock if clock is not None else time.monotonic()) >= deadline:
            state = (
                "verify-failed"
                if value["state"] == "verified" and last_verifications
                else "timed-out"
            )
            return {
                **value,
                "state": state,
                "dispatched_deployments": dispatched_deployments,
                "observed_sha": observed_sha,
                "verifications": last_verifications,
            }
        time.sleep(poll_seconds)
