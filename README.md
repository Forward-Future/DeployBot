# DeployBot

DeployBot is a provider-neutral GitHub merge queue for coding agents.
Codex, Claude Code, Cursor, or any MCP client can prepare and review a pull
request; the user keeps the final merge decision by saying `deploy`.

The queue stores authority in GitHub labels and authenticated comments. It pins
the exact reviewed head, freezes bursts, merges independent work back-to-back,
skips blockers, and reports overlapping source that needs one integration PR.

## Install

Install the reviewed `v0.1.0` source commit directly from GitHub:

```bash
python3 -m pip install \
  'deploybot-merge-queue[mcp] @ git+https://github.com/Forward-Future/DeployBot.git@0110f877423f60be04003b22561e4bfd95909491'
deploybot init
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

## Manual deploy gate

The installed agent adapter treats the user's exact `deploy` instruction as
authority for that thread's PR only. It calls:

```bash
deploybot enqueue <pr-number>
```

No timer is involved. Adding the `merge-queue` label can wake a GitHub Actions
coordinator immediately:

```yaml
name: DeployBot
on:
  pull_request_target:
    types: [labeled]

permissions:
  contents: write
  pull-requests: write
  checks: read

concurrency:
  group: deploybot-${{ github.repository }}
  cancel-in-progress: false

jobs:
  drain:
    if: github.event.label.name == 'merge-queue'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683
      # v0.1.0; keep the full commit so privileged workflows are immutable.
      - uses: Forward-Future/DeployBot@0110f877423f60be04003b22561e4bfd95909491
```

Keep this workflow on the default branch. Never check out or execute code from
the pull-request head in a privileged `pull_request_target` workflow.

The workflow bot and each person allowed to enqueue must be explicitly listed:

```toml
[queue]
trusted_actors = ["@repository-owner"]
coordinator_actors = ["@repository-owner", "github-actions[bot]"]
```

`@repository-owner` resolves to the owner in `owner/repository`. Organization
repositories should replace it with the exact human or bot logins they trust.
Coordinator accounts may freeze and complete a batch, but they cannot create
the per-pull-request deploy authorization.

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
deploybot plan --json
deploybot inspect <pr> --json
deploybot enqueue <pr>
deploybot freeze --json
deploybot drain --json
deploybot block <pr> --reason "..."
deploybot unblock <pr>
deploybot dequeue <pr> --reason "..."
deploybot merge <pr> --batch <batch-id>
```

`drain` merges only independent, green, exact-head-reviewed PRs. Overlapping
source is returned as `integration_required` for an agent to resolve once. It
waits briefly while GitHub recomputes mergeability after a merge, then records
the pass complete so a later independent batch cannot be stranded behind a
waiting item.
