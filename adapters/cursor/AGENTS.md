# DeployBot

Read `.mergequeue.toml` and use the `deploybot` MCP tools. Keep changing
pull requests draft and make the final ready head immutable. Address valid
feedback from the configured review providers.

For read-only status, use `pipeline_status` for the full delivery path,
`queue_plan` for the merge queue, and `inspect_pull_request` for one PR. Report
thread intent, PR stages, order, blockers, overlaps, CI, and deployment; never
freeze the queue just to inspect it.

Only the user's exact `deploy` instruction authorizes `request_deployment` for
the current thread. Include the stable Cursor thread ID, never prompt contents.
Never poll, merge an unlabeled PR, or absorb unrelated work. Let the event worker
promote fresh exact heads, use one integration PR for overlaps or cumulative
validation, return repair packets to the source thread, atomically resume after
fresh review, and follow cumulative `main` through verified deployment.
