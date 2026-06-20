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
) -> str:
    argv = [sys.executable, "-m", "agent_merge_queue.cli"]
    if config:
        argv.extend(("--config", config))
    if repository:
        argv.extend(("--repository", repository))
    argv.append(command)
    argv.extend(arguments)
    completed = subprocess.run(argv, text=True, capture_output=True, check=False)
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    return completed.stdout.strip()


@mcp.tool()
def queue_plan(repository: str | None = None, config: str | None = None) -> str:
    """Read the ordered queue, blockers, and source-overlap groups."""
    return _run("plan", "--json", repository=repository, config=config)


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
def freeze_queue(repository: str | None = None, config: str | None = None) -> str:
    """Freeze the current queue membership and exact head SHAs."""
    return _run("freeze", "--json", repository=repository, config=config)


@mcp.tool()
def drain_queue(repository: str | None = None, config: str | None = None) -> str:
    """Merge every independent ready PR in the frozen batch."""
    return _run("drain", "--json", repository=repository, config=config)


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
