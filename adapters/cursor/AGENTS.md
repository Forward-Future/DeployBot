# DeployBot

Read `.mergequeue.toml` and use the `deploybot` MCP tools. Keep changing
pull requests draft and make the final ready head immutable. Address valid
feedback from the configured review providers.

Only the user's exact `deploy` instruction authorizes enqueueing the current
thread's pull request. Never poll, merge an unlabeled pull request, or absorb
unrelated work. Merge independent ready work in order, skip blockers, honor
dependencies, and route overlapping source through one integration pull
request.
