# DeployBot

Read `.mergequeue.toml` and use the `deploybot` MCP tools. Keep changing
pull requests draft and make the final ready head immutable. Address valid
feedback from the configured review providers.

For read-only status, use `queue_plan` for the full queue and
`inspect_pull_request` for one pull request. Report order, readiness, blockers,
dependencies, and overlap groups; never freeze the queue just to inspect it.

Only the user's exact `deploy` instruction authorizes enqueueing the current
thread's pull request. Never poll, merge an unlabeled pull request, or absorb
unrelated work. Merge independent ready work in order, skip blockers, honor
dependencies, and route overlapping source through one integration pull
request.
