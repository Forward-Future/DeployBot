---
name: manage-merge-queue
description: Prepare, review, enqueue, and land GitHub pull requests through a provider-neutral agent merge queue. Use when a PR becomes ready, configured review feedback must be addressed, the user says deploy, or queued changes need safe conflict-aware ordering.
---

# Manage Merge Queue

Read `.mergequeue.toml` and use the DeployBot MCP tools. Keep changing
PRs draft, make the final ready head immutable, and address every valid finding
from the configured review providers.

Only the user's exact `deploy` instruction authorizes `enqueue_pull_request`
for this thread's PR. Enqueueing wakes the GitHub coordinator; never poll and
never absorb an unlabeled PR.

Use `queue_plan` before a burst. Merge independent ready PRs back-to-back, skip
blocked work, honor explicit dependencies, and send overlapping source through
one integration PR. Follow the newest cumulative base-branch release.
