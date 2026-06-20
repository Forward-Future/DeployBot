---
name: manage-merge-queue
description: Prepare, review, enqueue, and land GitHub pull requests through a provider-neutral agent merge queue. Use when a PR becomes ready, configured review feedback must be addressed, the user says deploy, or queued changes need safe conflict-aware ordering.
---

# Manage Merge Queue

Read `.mergequeue.toml` before acting. Treat its checks and review providers as
the repository's policy; never assume a particular review vendor.

## Prepare The Pull Request

1. Keep the PR draft while source changes.
2. Run the repository's required local tests, commit, and push only this task.
3. Mark the final head ready and treat it as immutable.
4. Wait for every configured exact-head check and review provider.
5. Read all actionable review findings and fix valid issues. Return the PR to
   draft before any replacement push, then repeat the final review once.

Do not merge merely because review is complete.

## Enqueue On Deploy

Treat the user's exact `deploy` instruction as authority for this thread's PR
only. Call `enqueue_pull_request` through the DeployBot MCP server, or run
`deploybot enqueue <pr-number>`. The command fails closed on stale heads,
missing checks, unresolved findings, conflicts, or missing review evidence.

The queue label wakes the GitHub coordinator. Do not create a polling timer and
do not merge the PR independently.

## Coordinate A Burst

Use `queue_plan`, then `freeze_queue` or `drain_queue`. Preserve first-in order
unless explicit dependencies require another order. Merge independent ready
PRs back-to-back without rebasing merely because the base branch moved.

Skip blocked or waiting PRs so they do not stop independent work. If the plan
returns `integration_required`, combine that overlap group on one integration
branch, resolve source once, run tests once, and obtain one final review. Never
hand-merge generated output.

Finish by following the newest cumulative base-branch CI and deployment owned
by the repository. Record exact heads, review verdicts, merged commits, waiting
items, and integration groups.
