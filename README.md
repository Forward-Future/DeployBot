# DeployBot

DeployBot is a provider-neutral GitHub merge queue for coding agents.
Codex, Claude Code, Cursor, or any MCP client can prepare and review a pull
request; the user keeps the final merge decision by saying `deploy`.

DeployBot stores authority in GitHub labels and authenticated comments. It
records deploy intent immediately, promotes the final exact reviewed head,
freezes bursts, merges independent work back-to-back, scaffolds cumulative
integration PRs, follows `main` through production, and pauses after failures.

## Install

Install the reviewed `v0.2.23` source commit directly from GitHub:

```bash
python3 -m pip install \
  'deploybot-merge-queue[mcp] @ git+https://github.com/Forward-Future/DeployBot.git@de0819770c2496b5048488c2a6a207be0378af06'
deploybot init
```

`deploybot init` writes the safe starter policy. The annotated
[example policy](.mergequeue.example.toml) shows every supported section, and
the [CLI, MCP, policy, and Action reference](docs/reference.md) lists every
current command, tool, option, and configuration field.

Invoke the bundled `$deploybot` skill to inspect or operate the queue. Typical
requests include “show the delivery pipeline,” “why is PR 42 blocked?”, and
“deploy this PR.” Status and diagnostics are read-only:

```bash
deploybot status
deploybot status --json
deploybot doctor
deploybot metrics --json
deploybot inspect 42 --json
```

For development from the Astro source tree, install
`./packages/agent-merge-queue[mcp]` instead.

Edit `.mergequeue.toml` to name the required checks, optional review providers,
and every exact GitHub login whose queue markers are trusted. Do not grant
authority by broad repository role. GitHub verifies comment authors; its normal
token permissions control label, comment, and merge writes. Authentication
comes from the GitHub CLI:

```bash
gh auth login
deploybot ensure-labels
deploybot doctor
```

The base installation has no review-service dependency. Repositories can use
required checks alone or add GitHub approvals, a generic bot, an agent-review
check, or any combination.

## Security model

Protect the base branch with a GitHub ruleset that independently requires the
same checks named in `.mergequeue.toml`, and do not give DeployBot's merge
credential permission to bypass that ruleset. DeployBot reads check display
names to coordinate the queue; GitHub's ruleset is the authoritative check
identity and the final atomic merge guard.

Keep workflow changes reviewed, pin third-party Actions to full commit hashes,
and never execute pull-request-head code in the privileged coordinator. The MCP
server uses the local process's existing GitHub credentials and accepts explicit
repository selectors, so run it only from a trusted coding client and workspace.

## Durable manual deploy gate

The installed agent adapter treats the user's exact `deploy` instruction as
authority for that thread's PR only. It records the intent immediately—even if
CI or review is still running:

```bash
deploybot request <pr-number> \
  --provider codex --thread-id "$CODEX_THREAD_ID"
```

The event worker promotes only the intent-bound exact head after all checks and
review providers pass. If review fixes create a replacement head, the trusted
source agent runs `deploybot refresh-request <pr>` after its fresh evidence;
the user does not repeat `deploy`. No polling timer is involved.

Install `examples/github-workflow.yml` on the default branch. It reacts to
deploy labels, ready/synchronize events, reviews, named CI `workflow_run`
completions, and completed external check suites. Keep its `workflows` list
aligned with `pipeline.ci_workflows`. A five-minute scheduled reconciliation
rereads all durable state in case GitHub concurrency coalesces the last pending
event in a burst. The privileged worker never checks out or executes
pull-request code. The Action follows releases by default so the same serialized
worker can dispatch deployment when GitHub suppresses the `workflow_run` event
for token-dispatched CI. Pin the Action to the full reviewed release commit:

```yaml
- uses: Forward-Future/DeployBot@de0819770c2496b5048488c2a6a207be0378af06
```

The Action uses GitHub's built-in workflow token. GitHub intentionally does not
turn merges made by that token into ordinary `push` workflow runs, so DeployBot
dispatches each configured CI workflow once after it merges a batch. GitHub can
also suppress the usual `workflow_run` handoff after that token-driven CI run,
so DeployBot explicitly dispatches each configured deployment workflow after
exact-main CI succeeds. CI workflows must accept `workflow_dispatch`.
Deployment workflows must accept `workflow_dispatch` inputs named `ci_sha` and
`ci_run_id`, verify that run through the GitHub API, and deploy only when it is
successful CI for the current base-branch head. Skipped deployment wake-ups
from pull-request CI are ignored. Set Action input `dispatch_ci: "false"` only
when a caller supplies a different merge identity that already triggers push
CI.

When integration PR checks depend on normal PR-authored events, mint a GitHub
App installation token in an earlier workflow step and pass it as the Action's
`token` input. Set `integration.require_non_actions_author = true` so a missing
App token fails before it freezes or creates an unusable integration PR. Add
the App's exact bot login (for example, `deploybot-app[bot]`) to
`queue.coordinator_actors` so its integration records are trusted.

The deployment workflow keeps its normal `workflow_run` trigger for push CI
and adds this exact-input recovery path for DeployBot-dispatched CI:

```yaml
on:
  workflow_run:
    workflows: [CI]
    types: [completed]
  workflow_dispatch:
    inputs:
      ci_sha:
        required: true
        type: string
      ci_run_id:
        required: true
        type: string
```

Before releasing, use `ci_run_id` to read the run from GitHub and require its
workflow name, base branch, head SHA, event, status, and conclusion to match the
expected successful exact-main CI run. The deployment must still pull the
current base branch and stop if it no longer equals `ci_sha`.

The workflow bot and each person allowed to request deployment must be
explicitly listed:

```toml
[queue]
trusted_actors = ["@repository-owner"]
coordinator_actors = ["@repository-owner", "github-actions[bot]"]
```

`@repository-owner` resolves to the owner in `owner/repository`. Organization
repositories should replace it with the exact human or bot logins they trust.
Coordinator accounts may promote, freeze, integrate, and complete batches, but
they cannot create the original per-pull-request deploy intent.

## Delivery controller

`deploybot status` reports active metadata-only agent threads, pending native
notifications, every PR stage, deploy requests and their exact authorized heads,
queue order, queued and pre-queue intent overlaps, exact-`main` CI, deployment,
pipeline pause state, and every open PR that has not been bound to its native
opening thread. It alerts when a deploy request exceeds the configured
ready-to-merge target and names the current gate. It never stores prompts,
transcripts, source, or credentials. A failed PR rollup is reconciled against
the exact commit's check runs before DeployBot creates a repair block, so a
cancelled superseded run cannot hide its replacement.

`deploybot react` promotes ready intent, skips blockers, drains independent
work, and creates integration PRs when configured. New batches contain at most
`integration.max_batch_size` entries; later FIFO work remains in the next batch.
A larger indivisible source-overlap or dependency closure is the sole exception:
it ships alone, never mixed with unrelated work.
After any merge, admission stays closed until the cumulative exact-main release
is verified live, preventing newer merges from starving an older deployment.
Set `pipeline.release_admission = "ci-passed"` to reopen admission as soon as
exact-main CI is green—deploy and health checks keep following in the
background—when higher merge throughput is worth a larger failure blast radius.
Draft status and incomplete
checks or reviews remain waiting states; they do not create a repair latch. A
conflict, failed gate, unresolved review, manual block, or stale authorized head
produces a repair handoff containing the source thread, base/head SHAs, source
paths, and one return command. Old draft-only repair latches self-clear once the
controller recognizes them:

In `overlap` mode, a ready source waits when another active, near-ready intent
belongs to the same source-overlap component. Unrelated ready work still drains,
and the held component freezes together once its remaining gates pass.
When more than one cumulative integration pull request needs controller-owned
exact-head CI, DeployBot dispatches every missing workflow before it waits. The
workflows then run in parallel instead of making later batches wait for an
earlier runner delay. Slow-queue status names the missing, queued, or failed
exact integration workflow instead of reporting only a generic merge worker.

```bash
deploybot resume <pr-number>
```

`resume` atomically verifies the replacement head, clears the block, requeues,
and emits a new wake-up event. `follow` tracks newer cumulative `main` revisions
until exact CI, deployment, and optional HTTP checks pass. A CI or deploy failure
can pause further merges until `deploybot unpause`.
Before presenting an unpause request, adapters must refresh `deploybot status
--json` and suppress stale prompts when the durable controller is already
running or the release advanced. The original deploy instruction authorizes the
coordinator to unpause the matching failed release after its elected repair
head passes fresh checks and review. Pass that status result's failed main SHA
and unique `control_id` to `deploybot unpause --sha SHA --control-id ID` so a
concurrent newer pause remains authoritative. Rollback,
bypass, and mismatched recovery still require explicit user direction.

Before starting an exact-main recovery, an agent runs
`deploybot claim-release-repair --provider CLIENT --thread-id ID`. A
deterministic branch ref elects exactly one repair owner for that failed SHA;
other threads receive the recorded owner instead of creating duplicate PRs.

At merge time, DeployBot records a non-expiring notification obligation. At
exact-main verification, it promotes every contained obligation to `pending`,
moves the matching PR-opening thread to `deployed` when that thread has not
moved on, and returns a stable `thread_notifications` payload for each one.
The provider adapter posts the supplied message into that native thread; for
Codex it wakes the thread with `send_message_to_thread`. The PR-opening thread
then acknowledges delivery and becomes `completed`. The message is a human-readable
release receipt with the pull-request title and link, up to three feature
highlights from its release notes, the exact deployed `main`, and CI/deployment
evidence. Adapters present that receipt verbatim and keep successful
acknowledgement IDs internal. PR-authored text is rendered inert; only
DeployBot-generated PR and release-evidence links remain clickable:

```bash
deploybot thread acknowledge --provider codex --thread-id "$CODEX_THREAD_ID" \
  --notification-id "$DEPLOYBOT_NOTIFICATION_ID"
```

When `main` advances during a genuine repair, the next promotion pass records a
new `repair-required` event for the new base SHA even when the PR head and failure
text are unchanged. Every affected source owner can refresh in parallel; FIFO is
still enforced when repaired heads re-enter the merge queue.

Integration-conflict repair packets include the complete frozen pull-request and
head map. The elected owner must prove every frozen head is present before
resuming the cumulative integration pull request.

Token-authored integration PR `pull_request` runs are never accepted as exact CI
evidence. This includes GitHub's `action_required` zero-job placeholder:
DeployBot ignores it and dispatches the configured exact-branch
`workflow_dispatch` run itself. Failures in that owned run still fail closed.

DeployBot does not treat a registry comment as user notification. If native
delivery fails, an independent outbox entry stays visible under pending
`notifications`, even if the PR-opening thread starts new work, and the same
`notification_id` can be retried. When `pipeline.webhook_url_env` is configured,
the provider-neutral webhook also receives the `thread-deployed` payload and
scheduled followers retry it. Without a configured webhook, pending receipts do
not keep the release worker running; the source adapter's native thread heartbeat
retrieves, acknowledges, and displays the final notification instead.
The first trusted `thread update` in `pr-draft`, `pr-review`, or `ready` phase
that includes a PR number immutably binds that PR to its opening native thread.
Later deploy, repair, integration, and coordinator threads cannot replace it.
`deploybot request` uses that recorded owner even when another thread authorizes
the release. An unowned PR cannot enter the delivery pipeline, so DeployBot
never silently routes its receipt to the authorizing caller. The
request result makes this ownership explicit in
`notification_handoff.required_action`; clients must complete that action before
ending the PR-opening-thread response.

```toml
[pipeline]
ci_workflows = ["CI"]
deploy_workflows = ["Deploy"]
batch_settle_seconds = 15
ci_failure_grace_seconds = 90
promotion_workers = 4
hold_merges_while_releasing = true
repair_branch_prefix = "deploybot/repair"
ready_to_merge_target_minutes = 15
merge_to_live_target_minutes = 10
auto_promote = true
intent_scope = "head"
pause_on_failure = true
release_admission = "verified" # or "ci-passed" for higher merge throughput

[[pipeline.verifications]]
name = "Login"
url = "https://example.com/login"
expected_status = 200

[integration]
# manual, overlap, or all (one cumulative pre-merge validation PR)
mode = "overlap"
max_batch_size = 3
# require_non_actions_author = true
# ci_satisfies_checks = ["Stable PR head", "Full test suite"]
```

For `overlap` or `all` mode with the hosted coordinator, enable **Allow GitHub
Actions to create and approve pull requests** under the repository's Actions
workflow-permission settings. `deploybot doctor` reports this prerequisite.

## Review providers

Required checks are always exact-head gates. Optional providers use normalized
pass, waiting, or blocked verdicts.

```toml
[queue]
required_checks = ["CI"]

[review]

[[review.providers]]
kind = "github-approvals"
name = "Human approval"
allowed_reviewers = ["reviewer-login"]
minimum_approvals = 1

[[review.providers]]
kind = "bot"
name = "Review bot"
login = "review-bot"
check_name = "Review Bot"
require_formal_review = true
require_resolved_threads = true
```

A bot score is optional. When used, its comment must contain the exact reviewed
commit SHA so an older score can never authorize a replacement head.

## Clients

- Codex: install the CLI and the CLI-only plugin under
  `adapters/codex/agent-merge-queue`. The Codex adapter intentionally does not
  start an MCP subprocess.
- Claude Code: install the plugin under `adapters/claude-code`.
- Cursor: copy the files under `adapters/cursor` or use its MCP configuration.
- Other clients: connect `deploybot-mcp` over stdio or call the CLI directly.

The Claude Code and Cursor MCP configurations launch the pinned public release
with `uvx`.
The `mergeq` and `mergeq-mcp` command aliases remain for compatibility.

## Command overview

All commands accept the global `--config PATH` and `--repository OWNER/REPO`
options before the subcommand. A missing pull-request selector resolves the PR
for the current branch.

```text
deploybot init [--force]
deploybot ensure-labels
deploybot status --json
deploybot plan --json
deploybot doctor --json
deploybot inspect [PR] --json
deploybot thread update --provider CLIENT --thread-id ID --phase PHASE [metadata]
deploybot thread acknowledge --provider CLIENT --thread-id ID --notification-id ID
deploybot request [PR] [--provider CLIENT] [--thread-id ID] [--thread-url URL]
deploybot cancel-request [PR]
deploybot refresh-request [PR]
deploybot enqueue [PR]
deploybot promote
deploybot freeze --json
deploybot drain --json
deploybot react [--follow] [--dispatch-ci] [--timeout SECONDS]
deploybot integrate [--all]
deploybot follow [--timeout SECONDS] [--poll SECONDS] [--json]
deploybot metrics --json
deploybot pause --reason "main CI failed"
deploybot unpause --sha FAILED_MAIN_SHA --control-id PAUSE_CONTROL_ID
deploybot block [PR] --reason "..."
deploybot unblock [PR]
deploybot resume [PR]
deploybot dequeue [PR] --reason "..."
deploybot merge [PR] --batch BATCH_ID
```

`status` is the read-only full delivery view. `plan` is the narrower queue-only
view. `doctor` verifies CLI auth, policy, labels, actors, check names, and branch
protection without changing the repository.

The MCP server exposes named equivalents for the operational commands, plus
explicit `repository` and `config` arguments on every tool. See the
[complete reference](docs/reference.md#mcp-tools) for the exact mapping and
arguments.

`drain` merges only independent, green, exact-head-reviewed PRs. Integration
mode `overlap` creates one cumulative PR for shared source; mode `all` routes the
whole frozen batch through one cumulative PR before `main`. DeployBot never
invents a conflict resolution: it prepares the branch and hands the exact repair
back to an agent.
