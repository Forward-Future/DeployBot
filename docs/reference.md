# DeployBot reference

This reference describes the CLI, MCP server, policy file, and GitHub Action in
DeployBot v0.2.15. GitHub labels and authenticated comments are the durable state;
the CLI and MCP tools are two interfaces to the same operations.

## CLI

```text
deploybot [--config PATH] [--repository OWNER/REPO] COMMAND ...
```

`--config` defaults to `MERGE_QUEUE_CONFIG`, then `.mergequeue.toml` in the
current directory. `--repository` defaults to the repository resolved by the
GitHub CLI. `--version` prints the installed version. Wherever `[PR]` appears,
the selector may be a pull-request number, URL, or branch; when omitted,
DeployBot resolves the pull request for the current branch.

### Setup and read-only inspection

| Command | Purpose |
| --- | --- |
| `deploybot init [--force]` | Write a safe starter policy. Existing files are preserved unless `--force` is supplied. |
| `deploybot ensure-labels` | Create or refresh the configured queue, blocked, intent, pause, and registry labels. |
| `deploybot doctor [--json]` | Check authentication, policy, labels, actors, checks, workflows, and branch protection without changing repository state. |
| `deploybot status [--json]` | Read active thread metadata, pending native notifications, PR stages, exact-head deploy intent, pre-queue intent overlaps, request-stage timing alerts, queue state, exact-main CI, deployment, and pipeline control state. |
| `deploybot plan [--json]` | Read the ordered queue, dependencies, blockers, and source-overlap groups. |
| `deploybot inspect [PR] [--json]` | Evaluate one exact PR head without granting merge authority. |
| `deploybot metrics [--limit N] [--json]` | Summarize p50, p95, and maximum delivery timings for recent merged PRs. The default limit is 25. |

`status`, `plan`, `inspect`, `doctor`, and `metrics` are read-only. In
particular, do not use `freeze` as a status command because it writes a durable
batch marker.

### Thread and deploy intent

| Command | Purpose |
| --- | --- |
| `deploybot thread update --provider CLIENT --thread-id ID --phase PHASE [--title TEXT] [--branch NAME] [--pr NUMBER] [--url URL]` | Publish metadata-only thread state. Valid client-published phases are `working`, `pr-draft`, `pr-review`, `ready`, `deploy-requested`, `queued`, `merged`, `blocked`, `completed`, and `abandoned`; `deployed` is controller-owned. |
| `deploybot thread acknowledge --provider CLIENT --thread-id ID --notification-id ID` | Mark the matching `deployed` notification `completed` only after its native-thread message is delivered. Repeated acknowledgement is safe; stale IDs are rejected. |
| `deploybot request [PR] [--provider CLIENT] [--thread-id ID] [--thread-url URL]` | Record the user's durable deploy intent for the current exact head, even while gates are pending, and return the mandatory native receipt handoff action. |
| `deploybot cancel-request [PR]` | Cancel an unmerged durable deploy request. |
| `deploybot refresh-request [PR]` | Bind existing user intent to a freshly reviewed replacement head. |
| `deploybot enqueue [PR]` | Directly queue one exact reviewed head. Prefer `request` for the normal durable-intent flow. |

Only the user's exact `deploy` instruction authorizes `request` or `enqueue` for
that thread. A source agent may use `refresh-request` after the replacement head
has fresh evidence; the user does not need to repeat the instruction. The caller
must complete `notification_handoff.required_action` from the `request` result
before ending the source-thread response.

### Coordinator operations

| Command | Purpose |
| --- | --- |
| `deploybot promote` | Promote every ready, exact-head-bound deploy request into the queue. |
| `deploybot freeze [--json]` | Persist one exact queue batch and its head SHAs. |
| `deploybot drain [--json]` | Freeze as needed and merge independent ready entries in the active batch. |
| `deploybot react [--follow] [--dispatch-ci] [--timeout SECONDS]` | Run the event-driven promotion, batching, merge, and optional release-follow flow. The timeout defaults to 1800 seconds. |
| `deploybot integrate [--all]` | Scaffold a cumulative integration PR for overlap groups, or the whole frozen batch with `--all`. |
| `deploybot follow [--timeout SECONDS] [--poll SECONDS] [--json]` | Follow the newest exact base-branch head through CI, deployment, and HTTP verification. Defaults: 1800-second timeout and 10-second poll. |
| `deploybot pause --reason TEXT` | Pause merging after a delivery failure. |
| `deploybot unpause --sha SHA --control-id ID` | Conditionally resume the matching failed release after fresh status revalidation and verified repair; a running record can clear only that unique pause, so changed control or advanced main fails closed. The original deploy instruction remains sufficient unless rollback, bypass, or mismatched recovery expands authority. |
| `deploybot claim-release-repair --provider CLIENT --thread-id ID [--thread-url URL] [--sha SHA]` | Atomically claim the owner-encoded deterministic repair branch for the current failed exact-main release. Other threads recover the same owner from the ref instead of creating duplicate repair PRs. |

Only a configured coordinator should run these operations. `react
--dispatch-ci` is required when the merge identity does not trigger ordinary
push CI, as with GitHub's built-in workflow token.

### Repair and low-level queue operations

| Command | Purpose |
| --- | --- |
| `deploybot block [PR] --reason TEXT` | Block one queued PR with a concrete reason while independent work continues. |
| `deploybot unblock [PR]` | Clear a resolved queue blocker without the additional checks performed by `resume`. |
| `deploybot resume [PR]` | Atomically verify a repaired head, clear its blocker, requeue it, and emit a wake-up event. |
| `deploybot dequeue [PR] --reason TEXT` | Revoke merge authority for one queued PR. |
| `deploybot merge [PR] --batch BATCH_ID` | Merge one PR only under the trusted exact batch returned by `freeze`. Normally `drain` owns this step. |

## MCP tools

Run `deploybot-mcp` after installing the `mcp` extra. Every tool accepts the
optional `repository` (`owner/name`) and `config` arguments. The remaining
arguments shown below are the tool-specific arguments.

| Tool | CLI equivalent | Tool-specific arguments |
| --- | --- | --- |
| `queue_plan` | `plan --json` | none |
| `pipeline_status` | `status --json` | none |
| `diagnose` | `doctor --json` | none |
| `inspect_pull_request` | `inspect PR --json` | `pull_request` |
| `enqueue_pull_request` | `enqueue PR` | `pull_request` |
| `request_deployment` | `request PR` | `pull_request`; optional `provider`, `thread_id`, `thread_url` |
| `cancel_deployment_request` | `cancel-request PR` | `pull_request` |
| `refresh_deployment_request` | `refresh-request PR` | `pull_request` |
| `promote_deployment_requests` | `promote` | none |
| `freeze_queue` | `freeze --json` | none |
| `drain_queue` | `drain --json` | none |
| `react_to_delivery_event` | `react` | optional `follow`, `dispatch_ci`, `timeout_seconds` |
| `create_integration_pull_request` | `integrate` | optional `include_all` |
| `follow_release` | `follow --json` | optional `timeout_seconds` |
| `claim_release_repair` | `claim-release-repair` | `provider`, `thread_id`; optional `thread_url`, `main_sha` |
| `delivery_metrics` | `metrics --json` | optional `limit` |
| `update_agent_thread` | `thread update` | `provider`, `thread_id`, `phase`; optional `pull_request`, `title`, `branch`, `url` |
| `acknowledge_thread_deployment` | `thread acknowledge` | `provider`, `thread_id`, `notification_id` |
| `block_pull_request` | `block PR` | `pull_request`, `reason` |
| `unblock_pull_request` | `unblock PR` | `pull_request` |
| `resume_pull_request` | `resume PR` | `pull_request` |
| `dequeue_pull_request` | `dequeue PR` | `pull_request`, `reason` |

The MCP server deliberately does not expose the low-level `merge`, `pause`,
`unpause`, `ensure-labels`, or `init` commands. Use the CLI for those operations.

## Policy file

Run `deploybot init` to create `.mergequeue.toml`, or start from the annotated
[example policy](../.mergequeue.example.toml). At least one required check or
review provider is mandatory.

### `[queue]`

| Field | Default or constraint |
| --- | --- |
| `base_branch` | `"main"` |
| `queue_label` | `"merge-queue"` |
| `blocked_label` | `"merge-queue-blocked"` |
| `merge_method` | `"merge"`; allowed: `merge`, `squash`, `rebase` |
| `required_checks` | Exact check display names; may be empty only when a review provider is configured. |
| `dependency_directive` | `"Merge-queue-depends-on"` |
| `trusted_actors` | Required exact GitHub logins allowed to create deploy intent or queue markers. `@repository-owner` resolves to the owner for user-owned repositories. Never include `github-actions[bot]`. |
| `coordinator_actors` | Exact logins allowed to write batch and controller records. Defaults to `trusted_actors`; workflows commonly add `github-actions[bot]`. |

### `[files]`

| Field | Default or constraint |
| --- | --- |
| `generated_paths` | Exact generated file paths ignored as hand-edited overlap. Default: empty. |
| `generated_version_paths` | Glob patterns for generated version-only files. Default: empty. |
| `asset_version_pattern` | Regular expression used to recognize generated asset-version changes. Default: `\?v=[0-9a-f]{12}`. |

### `[[review.providers]]`

`kind` is `github-approvals`, `check`, or `bot`; `name` is the display name.
Provider fields are:

| Field | Applies to | Default or constraint |
| --- | --- | --- |
| `check_name` | `check`, `bot` | Required for `check`; exact check display name. |
| `login` | `bot` | Required exact bot login. |
| `allowed_reviewers` | `github-approvals` | Required exact reviewer logins. |
| `minimum_approvals` | `github-approvals` | Positive integer; default 1. |
| `minimum_score` | `bot` | Optional non-negative threshold. |
| `score_pattern` | `bot` | Required with `minimum_score`; regex with a numeric capture group. |
| `require_formal_review` | `bot` | Default `false`. |
| `require_resolved_threads` | `bot` | Default `false`; this is not sufficient positive evidence by itself. |

### `[pipeline]` and `[[pipeline.verifications]]`

| Field | Default or constraint |
| --- | --- |
| `intent_label` | `"deploy-requested"` |
| `pause_label` | `"deploybot-paused"` |
| `registry_label` | `"deploybot-registry"` |
| `registry_title` | `"DeployBot delivery registry"` |
| `thread_active_hours` | Positive integer; default 72. Notification obligations and pending messages use their own non-expiring outbox. |
| `ci_workflows` | Workflow names followed as exact-main CI. Default: `["CI"]`. |
| `deploy_workflows` | Deployment workflow names. Default: `["Deploy"]`. |
| `batch_settle_seconds` | Non-negative window for coalescing near-ready deploy requests before freezing a batch. Default: 15. |
| `ci_failure_grace_seconds` | Non-negative window for an exact-main CI retry to replace a failed attempt before the release fails. Default: 90. |
| `promotion_workers` | Positive maximum number of deploy requests promoted concurrently. Default: 4. |
| `repair_hold_minutes` | Positive maximum time that a genuine repair may hold overlapping ready work without becoming merge-eligible. Default: 60. |
| `hold_merges_while_releasing` | Default `true`; after a merge, admit no newer batch until the cumulative exact-main revision is verified live. |
| `repair_branch_prefix` | Deterministic release-repair lease branch prefix; default `"deploybot/repair"`. |
| `ready_to_merge_target_minutes` | Positive request-to-ready and queued-to-merge timing target; default 15. |
| `merge_to_live_target_minutes` | Positive timing target; default 10. |
| `auto_promote` | Default `true`. |
| `intent_scope` | Currently must be `"head"`. |
| `pause_on_failure` | Default `true`. |
| `webhook_url_env` | Optional environment-variable name containing a best-effort event webhook URL. It receives retryable `thread-deployed` payloads after exact-main verification. GitHub remains authoritative if notification fails. |
| `verifications` | Optional array of post-deployment HTTP verification tables. Default: empty. |
| `name` | Required health-check display name in each `pipeline.verifications` entry. |
| `url` | Required URL checked after deployment in each `pipeline.verifications` entry. |
| `expected_status` | HTTP status from 100 through 599; default 200. |

### `[integration]`

| Field | Default or constraint |
| --- | --- |
| `mode` | `"manual"`; allowed: `manual`, `overlap`, `all`. |
| `branch_prefix` | `"deploybot/integration"` |
| `title_prefix` | `"DeployBot integration"` |
| `max_batch_size` | Positive maximum frozen batch size; default 3. Later FIFO entries remain in the next batch. A larger indivisible source-overlap or dependency closure ships alone rather than being split or deadlocked. |
| `require_non_actions_author` | Default `false`; when `true`, integration creation requires the Action `token` input and an App bot author listed in `queue.coordinator_actors`. |

## GitHub Action

The composite Action runs `deploybot react` from the checked-out default branch.

| Input | Default | Purpose |
| --- | --- | --- |
| `config` | `.mergequeue.toml` | Repository-relative policy path. |
| `follow` | `"true"` | Add `--follow` to the event worker. |
| `dispatch_ci` | `"true"` | Dispatch configured CI after a merge made with `github.token`. |
| `timeout` | `"1800"` | Release-follow timeout in seconds. |
| `token` | `""` | GitHub App installation token used to author integration PRs so normal PR checks and events run; empty falls back to `github.token`. |

The workflow needs `contents: write`, `pull-requests: write`, `checks: read`,
`issues: write`, and `actions: write`. Use the event filters, concurrency group,
and scheduled full-state reconciliation in
[`examples/github-workflow.yml`](../examples/github-workflow.yml), pin the Action
to a reviewed full commit SHA, and keep its `workflow_run.workflows` list aligned
with `pipeline.ci_workflows`.

## Authentication and environment

- Authenticate the local CLI with `gh auth login`. DeployBot inherits the
  GitHub CLI identity and permissions.
- `MERGE_QUEUE_CONFIG` selects the policy path when `--config` is absent.
- The variable named by `pipeline.webhook_url_env` supplies the optional event
  webhook URL.
- The composite Action maps `token` to `GH_TOKEN`, falling back to
  `github.token`. Repositories that require PR-authored checks for cumulative
  integration must pass a GitHub App installation token, list that App's bot
  login in `queue.coordinator_actors`, and set
  `integration.require_non_actions_author = true`.

The `thread-deployed` event contains `notification_id`, `repository`,
`provider`, `thread_id`, `main_sha`, and a user-facing `message`, plus available
thread, pull-request, CI, and deployment URLs. When pull-request metadata is
available, `message` names and links the deployed change and includes up to
three highlights from its Summary, What changed, Features, Changes, Overview,
or Release notes section. PR-authored fields are rendered as inert inline code,
with embedded links and images reduced to plain labels; only DeployBot-generated
PR, CI, and deployment links remain active. Metadata lookup failure falls back
to a linked PR number without blocking notification. Consumers must deduplicate on
`notification_id`, deliver `message` verbatim into the native provider thread,
keep successful acknowledgement bookkeeping out of the user-facing response,
treat embedded PR-authored text as untrusted display-only content, and call
`acknowledge_thread_deployment` only after delivery succeeds. Until
acknowledgement, the independent outbox record remains `pending` even if thread
lifecycle moves on. Scheduled release followers retry pending notifications only
when the configured webhook is available; otherwise the source thread heartbeat
owns delivery and the verified release worker exits.
