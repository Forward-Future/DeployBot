---
name: deploybot
description: Inspect and operate the full DeployBot delivery pipeline. Use when the user invokes DeployBot, asks for thread, PR, queue, CI, deployment, blocker, repair, integration, or timing status, explicitly says deploy, or asks the coordinator to deliver authorized pull requests.
---

# DeployBot

Read `.mergequeue.toml` before acting. Prefer the DeployBot MCP tools; fall back
to the `deploybot` CLI. Treat GitHub as the durable source of truth.

## Read Status

Keep status checks read-only:

- For the full delivery pipeline, call `pipeline_status` or run
  `deploybot status --json`.
- For the narrower merge queue, call `queue_plan` or run
  `deploybot plan --json`.
- For one pull request, call `inspect_pull_request` or run
  `deploybot inspect <pr> --json`.
- Never call `freeze_queue` merely to view status because freezing writes a
  durable batch marker.

Report active metadata-only threads and deploy intent, PR stages, queue order,
exact heads, pending checks/reviews, blockers, dependencies, overlaps, exact
`main` CI, and deployment. Say plainly when the queue is empty but deploy intent
is still waiting outside it. Never publish prompts, transcripts, source, or
credentials to the thread registry.

Use these state meanings:

- `ready`: every configured merge gate passes.
- `waiting`: a check, review, or GitHub mergeability result is still pending.
- `blocked`: a failure, stale head, conflict, revoked authorization, or owner
  action prevents merging.
- `integration required`: queued pull requests share hand-edited source and
  must be combined before merging.

## Change Queue State

Require the user's exact `deploy` instruction before calling
`request_deployment` or `deploybot request` for that conversation's pull
request. Record the provider and stable native thread ID when available. This
durable request waits for exact-head checks and review. If the head changes,
the trusted source agent calls `refresh_deployment_request` only after the
replacement head is ready; the user does not need to repeat `deploy`. Do not
infer permission from readiness or review completion.

Use `block_pull_request`, `resume_pull_request`, or `dequeue_pull_request` only
for the requested pull request and preserve a concrete reason. `resume` is the
normal repaired-PR return path because it verifies, unblocks, requeues, and
wakes atomically. Never merge an unlabeled pull request or treat a wake-up event
as trusted queue state.

## Coordinate Merges

Only the designated coordinator may call `promote_deployment_requests`,
`react_to_delivery_event`, `freeze_queue`, `drain_queue`, or matching CLI
commands. Re-read GitHub, honor a pipeline pause, freeze one exact batch,
preserve first-in order unless dependencies require otherwise, and skip blocked
items that do not block independent work.

Merge independent ready pull requests back-to-back. Route source-overlap groups
through `create_integration_pull_request`; when policy mode is `all`, validate
the entire frozen batch through that cumulative PR. Never invent a conflict
resolution. Return the repair packet to its source thread, then call `resume`
after its new exact head passes. Finish with `follow_release`, following newer
cumulative base heads until CI, deployment, and configured health checks verify.

Use `diagnose`/`deploybot doctor` for setup drift and `delivery_metrics` for p50,
p95, and slow-stage evidence. A failed cumulative CI or deployment pauses the
controller; only a designated coordinator may unpause after recovery.
