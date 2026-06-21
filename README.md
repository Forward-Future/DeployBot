# DeployBot

DeployBot is a provider-neutral GitHub merge queue for coding agents.
Codex, Claude Code, Cursor, or any MCP client can prepare and review a pull
request; the user keeps the final merge decision by saying `deploy`.

DeployBot stores authority in GitHub labels and authenticated comments. It
records deploy intent immediately, promotes the final exact reviewed head,
freezes bursts, merges independent work back-to-back, scaffolds cumulative
integration PRs, follows `main` through production, and pauses after failures.

## Install

Install the reviewed `v0.2.8` source commit directly from GitHub:

```bash
python3 -m pip install \
  'deploybot-merge-queue[mcp] @ git+https://github.com/Forward-Future/DeployBot.git@13d7293b181581d2e4d59d8a605df76f7feb88a6'
deploybot init
```

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
aligned with `pipeline.ci_workflows`. The privileged worker never checks out or
executes pull-request code. Pin the Action to the full reviewed release commit:

```yaml
- uses: Forward-Future/DeployBot@13d7293b181581d2e4d59d8a605df76f7feb88a6
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

`deploybot status` reports active metadata-only agent threads, every PR stage,
deploy requests, queue order, overlaps, exact-`main` CI, deployment, and pipeline
pause state. It never stores prompts, transcripts, source, or credentials.

`deploybot react` promotes ready intent, skips blockers, drains independent
work, and creates integration PRs when configured. A conflict produces a repair
handoff containing the source thread, base/head SHAs, source paths, and one
return command:

```bash
deploybot resume <pr-number>
```

`resume` atomically verifies the replacement head, clears the block, requeues,
and emits a new wake-up event. `follow` tracks newer cumulative `main` revisions
until exact CI, deployment, and optional HTTP checks pass. A CI or deploy failure
can pause further merges until `deploybot unpause`.

```toml
[pipeline]
ci_workflows = ["CI"]
deploy_workflows = ["Deploy"]
ready_to_merge_target_minutes = 15
merge_to_live_target_minutes = 10
auto_promote = true
intent_scope = "head"
pause_on_failure = true

[[pipeline.verifications]]
name = "Login"
url = "https://example.com/login"
expected_status = 200

[integration]
# manual, overlap, or all (one cumulative pre-merge validation PR)
mode = "overlap"
```

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

- Codex: install the plugin under `adapters/codex/agent-merge-queue`.
- Claude Code: install the plugin under `adapters/claude-code`.
- Cursor: copy the files under `adapters/cursor` or use its MCP configuration.
- Other clients: connect `deploybot-mcp` over stdio or call the CLI directly.

The bundled MCP configurations launch the pinned public release with `uvx`.
The `mergeq` and `mergeq-mcp` command aliases remain for compatibility.

## Commands

```text
deploybot status --json
deploybot plan --json
deploybot doctor --json
deploybot request <pr> --provider codex --thread-id THREAD
deploybot refresh-request <pr>
deploybot promote
deploybot react --follow
deploybot resume <pr>
deploybot integrate --all
deploybot follow --json
deploybot metrics --json
deploybot pause --reason "main CI failed"
deploybot unpause
deploybot thread update --provider codex --thread-id THREAD --phase ready --pr 42
deploybot inspect <pr> --json
deploybot enqueue <pr>
deploybot freeze --json
deploybot drain --json
deploybot block <pr> --reason "..."
deploybot unblock <pr>
deploybot dequeue <pr> --reason "..."
deploybot merge <pr> --batch <batch-id>
```

`status` is the read-only full delivery view. `plan` is the narrower queue-only
view. `doctor` verifies CLI auth, policy, labels, actors, check names, and branch
protection without changing the repository.

`drain` merges only independent, green, exact-head-reviewed PRs. Integration
mode `overlap` creates one cumulative PR for shared source; mode `all` routes the
whole frozen batch through one cumulative PR before `main`. DeployBot never
invents a conflict resolution: it prepares the branch and hands the exact repair
back to an agent.
