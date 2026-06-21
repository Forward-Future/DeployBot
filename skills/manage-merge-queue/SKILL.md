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

## Record Deploy Intent

Treat the user's exact `deploy` instruction as authority for this thread's PR
only. Call `request_deployment` through the DeployBot MCP server, or run
`deploybot request <pr-number> --provider <client> --thread-id <stable-id>`.
This records intent immediately. If review fixes change the head, call
`refresh_deployment_request` only after its fresh checks and review pass; the
event worker then promotes that exact head without another user instruction.

The intent label wakes the GitHub coordinator. Review, check, ready, and push
events retry promotion without a polling timer. Do not merge independently.

## Coordinate A Burst

Use `pipeline_status`, then `react_to_delivery_event` or the narrower queue
tools. Preserve first-in order unless dependencies require another order. Merge
independent ready PRs back-to-back without rebasing merely because `main` moved.

Skip blocked or waiting PRs so they do not stop independent work. A blocker
creates a structured repair handoff to the source thread. After the repair has
fresh checks and review, call `resume_pull_request` once. If policy requests an
integration PR, let DeployBot scaffold it, resolve source once, run tests once,
and obtain one final review. Never hand-merge generated output.

Finish with `follow_release`, tracking newer cumulative base heads through CI,
deployment, and configured health checks. A failure pauses further merges until
the coordinator verifies recovery and unpauses. Record exact heads, review
verdicts, merged commits, waiting items, repair packets, integration groups, and
delivery timing.
