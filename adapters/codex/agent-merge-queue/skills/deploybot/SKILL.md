---
name: deploybot
description: Inspect and operate a DeployBot GitHub merge queue. Use when the user invokes DeployBot, asks for queue or PR status, wants to know what is ready, waiting, blocked, next, dependent, or overlapping, explicitly says deploy, or asks the queue coordinator to freeze or drain authorized pull requests.
---

# DeployBot

Read `.mergequeue.toml` before acting. Prefer the DeployBot MCP tools; fall back
to the `deploybot` CLI. Treat GitHub as the durable source of truth.

## Read Status

Keep status checks read-only:

- For the full queue, call `queue_plan` or run `deploybot status --json`.
- For one pull request, call `inspect_pull_request` or run
  `deploybot inspect <pr> --json`.
- Never call `freeze_queue` merely to view status because freezing writes a
  durable batch marker.

Report the queue in order. For each pull request, include its state, exact head,
checks or reviews still waiting, blocker, dependencies, and overlap group. Say
plainly when the queue is empty.

Use these state meanings:

- `ready`: every configured merge gate passes.
- `waiting`: a check, review, or GitHub mergeability result is still pending.
- `blocked`: a failure, stale head, conflict, revoked authorization, or owner
  action prevents merging.
- `integration required`: queued pull requests share hand-edited source and
  must be combined before merging.

## Change Queue State

Require the user's exact `deploy` instruction before calling
`enqueue_pull_request` or `deploybot enqueue` for that conversation's pull
request. Do not infer permission from readiness or review completion.

Use `block_pull_request`, `unblock_pull_request`, or `dequeue_pull_request` only
for the requested pull request and preserve a concrete reason. Never merge an
unlabeled pull request or treat a wake-up event as trusted queue state.

## Coordinate Merges

Only the designated coordinator may call `freeze_queue`, `drain_queue`, or the
matching CLI commands. Re-read the queue, freeze one exact batch, preserve
first-in order unless dependencies require otherwise, and skip blocked items
that do not block independent work.

Merge independent ready pull requests back-to-back. Route source-overlap groups
through one integration pull request instead of resolving the same conflict
several times. Finish by following the newest cumulative base-branch CI and the
repository-owned deployment.
