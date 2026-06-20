---
name: manage-merge-queue
description: Prepare, review, enqueue, and land GitHub pull requests through a provider-neutral agent merge queue. Use when a PR becomes ready, configured review feedback must be addressed, the user says deploy, or queued changes need safe conflict-aware ordering.
---

# Manage Merge Queue

Read `.mergequeue.toml` and use the `deploybot` MCP tools. Keep changing
PRs draft and make the final ready head immutable. Address valid feedback from
the configured providers; do not assume a specific review service.

Only the user's exact `deploy` instruction authorizes enqueueing this thread's
PR. The queue label wakes GitHub immediately. Never poll or merge an unlabeled
PR. Drain independent ready work in order, skip blockers, honor dependencies,
and route overlapping source through one integration PR.
