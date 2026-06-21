---
name: manage-merge-queue
description: Prepare, review, enqueue, and land GitHub pull requests through a provider-neutral agent merge queue. Use when a PR becomes ready, configured review feedback must be addressed, the user says deploy, or queued changes need safe conflict-aware ordering.
---

# Manage Merge Queue

Read `.mergequeue.toml` and use the `deploybot` MCP tools. Keep changing PRs
draft and make the final ready head immutable. Address valid feedback from the
configured providers; do not assume a specific review service.

Only the user's exact `deploy` instruction authorizes `request_deployment` for
this thread's PR. Include the stable Claude thread ID. If review fixes change
the head, call `refresh_deployment_request` after fresh exact-head gates. Never
poll or merge an unlabeled PR.

Use `pipeline_status` and `react_to_delivery_event` for bursts. Skip blockers,
honor dependencies, route overlap or cumulative validation through
`create_integration_pull_request`, return repair packets to the source thread,
and use `resume_pull_request` after fresh review. Finish with `follow_release`;
a failed CI or deployment pauses the pipeline until verified recovery.

When `follow_release` returns `thread_notifications`, send each supplied
message to its native source thread. The source thread calls
`acknowledge_thread_deployment` with the matching `notification_id` and reports
completion to the user. Leave failed notifications `pending` so they remain
retryable.

Before a requesting source thread stops running, attach a native follow-up
monitor that checks `pipeline_status` and wakes it to report and acknowledge its
matching pending notification.
