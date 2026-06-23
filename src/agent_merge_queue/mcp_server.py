"""MCP tools shared by Codex, Claude Code, Cursor, and other clients."""

from __future__ import annotations

import subprocess
import sys

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as error:  # pragma: no cover - exercised by packaging smoke tests.
    raise SystemExit(
        "The MCP extra is not installed. Run: pip install 'deploybot-merge-queue[mcp]'"
    ) from error


mcp = FastMCP("DeployBot")


def _run(
    command: str,
    *arguments: str,
    repository: str | None = None,
    config: str | None = None,
    allow_nonzero: bool = False,
) -> str:
    argv = [sys.executable, "-m", "agent_merge_queue.cli"]
    if config:
        argv.extend(("--config", config))
    if repository:
        argv.extend(("--repository", repository))
    argv.append(command)
    argv.extend(arguments)
    completed = subprocess.run(argv, text=True, capture_output=True, check=False)
    if completed.returncode and not allow_nonzero:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    return completed.stdout.strip()


@mcp.tool()
def queue_plan(repository: str | None = None, config: str | None = None) -> str:
    """Read the ordered queue, blockers, and source-overlap groups."""
    return _run("plan", "--json", repository=repository, config=config)


@mcp.tool()
def pipeline_status(repository: str | None = None, config: str | None = None) -> str:
    """Read active threads, PR stages, queue, CI, and deployment status."""
    return _run("status", "--json", repository=repository, config=config)


@mcp.tool()
def diagnose(repository: str | None = None, config: str | None = None) -> str:
    """Run read-only installation, policy, and GitHub health checks."""
    return _run(
        "doctor",
        "--json",
        repository=repository,
        config=config,
        allow_nonzero=True,
    )


@mcp.tool()
def inspect_pull_request(
    pull_request: str,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Evaluate one exact PR head without granting merge authorization."""
    return _run("inspect", pull_request, "--json", repository=repository, config=config)


@mcp.tool()
def enqueue_pull_request(
    pull_request: str,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Queue one exact reviewed PR head after the user authorizes deploy."""
    return _run("enqueue", pull_request, repository=repository, config=config)


@mcp.tool()
def request_deployment(
    pull_request: str,
    provider: str | None = None,
    thread_id: str | None = None,
    thread_url: str | None = None,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Persist intent and route receipts to the recorded PR-opening thread."""
    arguments = [pull_request]
    for flag, value in (
        ("--provider", provider),
        ("--thread-id", thread_id),
        ("--thread-url", thread_url),
    ):
        if value:
            arguments.extend((flag, value))
    return _run("request", *arguments, repository=repository, config=config)


@mcp.tool()
def cancel_deployment_request(
    pull_request: str,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Cancel one durable deploy request before it merges."""
    return _run("cancel-request", pull_request, repository=repository, config=config)


@mcp.tool()
def refresh_deployment_request(
    pull_request: str,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Bind existing user intent to a freshly reviewed replacement head."""
    return _run("refresh-request", pull_request, repository=repository, config=config)


@mcp.tool()
def promote_deployment_requests(
    repository: str | None = None, config: str | None = None
) -> str:
    """Promote every exact-head-ready deploy request into the merge queue."""
    return _run("promote", repository=repository, config=config)


@mcp.tool()
def freeze_queue(repository: str | None = None, config: str | None = None) -> str:
    """Freeze the current queue membership and exact head SHAs."""
    return _run("freeze", "--json", repository=repository, config=config)


@mcp.tool()
def drain_queue(repository: str | None = None, config: str | None = None) -> str:
    """Merge every independent ready PR in the frozen batch."""
    return _run("drain", "--json", repository=repository, config=config)


@mcp.tool()
def react_to_delivery_event(
    follow: bool = False,
    dispatch_ci: bool = False,
    timeout_seconds: int = 1800,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Run the event-driven promotion, batching, merge, and optional release flow."""
    arguments = ["--timeout", str(timeout_seconds)]
    if follow:
        arguments.append("--follow")
    if dispatch_ci:
        arguments.append("--dispatch-ci")
    return _run("react", *arguments, repository=repository, config=config)


@mcp.tool()
def create_integration_pull_request(
    include_all: bool = False,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Scaffold a cumulative PR for overlaps or full-batch validation."""
    arguments = ["--all"] if include_all else []
    return _run("integrate", *arguments, repository=repository, config=config)


@mcp.tool()
def follow_release(
    timeout_seconds: int = 1800,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Advance the newest exact-main release through verified deployment."""
    return _run(
        "follow",
        "--timeout",
        str(timeout_seconds),
        "--json",
        repository=repository,
        config=config,
    )


@mcp.tool()
def claim_release_repair(
    provider: str,
    thread_id: str,
    thread_url: str | None = None,
    main_sha: str | None = None,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Elect one native thread to repair the current failed main release."""
    arguments = ["--provider", provider, "--thread-id", thread_id]
    if thread_url:
        arguments.extend(("--thread-url", thread_url))
    if main_sha:
        arguments.extend(("--sha", main_sha))
    return _run(
        "claim-release-repair",
        *arguments,
        repository=repository,
        config=config,
    )


@mcp.tool()
def delivery_metrics(
    limit: int = 25,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Read p50/p95 delivery timing for recent merged pull requests."""
    return _run(
        "metrics", "--limit", str(limit), "--json", repository=repository, config=config
    )


@mcp.tool()
def update_agent_thread(
    provider: str,
    thread_id: str,
    phase: str,
    pull_request: int | None = None,
    title: str | None = None,
    branch: str | None = None,
    url: str | None = None,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Publish state; opening PR phases immutably bind the native owner."""
    arguments = [
        "update",
        "--provider",
        provider,
        "--thread-id",
        thread_id,
        "--phase",
        phase,
    ]
    for flag, value in (
        ("--pr", str(pull_request) if pull_request is not None else None),
        ("--title", title),
        ("--branch", branch),
        ("--url", url),
    ):
        if value:
            arguments.extend((flag, value))
    return _run("thread", *arguments, repository=repository, config=config)


@mcp.tool()
def acknowledge_thread_deployment(
    provider: str,
    thread_id: str,
    notification_id: str,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Acknowledge only after the deployed message reaches the native thread."""
    return _run(
        "thread",
        "acknowledge",
        "--provider",
        provider,
        "--thread-id",
        thread_id,
        "--notification-id",
        notification_id,
        repository=repository,
        config=config,
    )


@mcp.tool()
def block_pull_request(
    pull_request: str,
    reason: str,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Block a queued PR with a concrete reason while other work continues."""
    return _run(
        "block",
        pull_request,
        "--reason",
        reason,
        repository=repository,
        config=config,
    )


@mcp.tool()
def unblock_pull_request(
    pull_request: str,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Clear a resolved queue blocker."""
    return _run("unblock", pull_request, repository=repository, config=config)


@mcp.tool()
def resume_pull_request(
    pull_request: str,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Atomically verify, unblock, requeue, and wake a repaired pull request."""
    return _run("resume", pull_request, repository=repository, config=config)


@mcp.tool()
def dequeue_pull_request(
    pull_request: str,
    reason: str,
    repository: str | None = None,
    config: str | None = None,
) -> str:
    """Revoke merge authorization for one queued PR."""
    return _run(
        "dequeue",
        pull_request,
        "--reason",
        reason,
        repository=repository,
        config=config,
    )


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
