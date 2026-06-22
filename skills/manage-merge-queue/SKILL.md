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
6. Immediately after opening the PR, call `update_agent_thread` in `pr-draft`,
   `pr-review`, or `ready` phase with its number so this opening thread becomes
   the immutable repair and deployment-receipt destination.
7. Before finishing the PR-opening response, call `pipeline_status` and verify
   that the exact PR is in `pull_request_thread_owners`, not
   `unbound_pull_requests`. Bind and recheck it if missing.

Do not merge merely because review is complete.

## Record Deploy Intent

Treat the user's exact `deploy` instruction as authority for this thread's PR
only. Call `request_deployment` through the DeployBot MCP server, or run
`deploybot request <pr-number>`. DeployBot uses the recorded PR-opening thread;
a coordinator must never substitute its own thread ID.
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

A genuine repair remains merge-ineligible, but DeployBot may temporarily hold
overlapping ready work for the configured bounded repair window so concurrent
merges do not repeatedly invalidate the replacement head.

Finish with `follow_release`, tracking newer cumulative base heads through CI,
deployment, and configured health checks. A failure pauses further merges until
the coordinator verifies recovery and unpauses. Before creating that recovery,
call `claim_release_repair`; only the returned `owned` thread may use the
deterministic repair branch. Respect the configured maximum batch size and keep
new merges closed while an earlier exact-main release is unfinished. Record
exact heads, review
verdicts, merged commits, waiting items, repair packets, integration groups, and
delivery timing.

Immediately before asking the user to `unpause` or take another repair action,
call `pipeline_status` again. Never show a stale pause prompt when durable state
is already `running` or the release has advanced. The original `deploy`
instruction authorizes the coordinator to unpause the matching failed release
after the elected repair head passes fresh checks and review, provided the pause
reason still matches and no rollback or gate waiver is needed. In that case,
run `deploybot unpause --sha <failed-main-sha> --control-id <control-id>` and
continue without asking the user to repeat authorization.
