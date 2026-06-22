# Agent notes

Operational memory for AI agents working in this repository.

## Repository access and merging

- The git credential in agent sessions has **push and admin access** to the
  GitHub remote. Verify with `gh api repos/<owner>/<repo> --jq .permissions`
  rather than assuming a lack of access.
- The bundled `gh` CLI is used read-only (so the PR "merge" button and other
  `gh` write commands are unavailable), but `git push` works for feature
  branches **and for `main`**. Do not refuse a merge by claiming you cannot
  merge — check first.
- Only merge or deploy when the user explicitly instructs it. When they do:
  fetch and integrate the latest `main`, resolve conflicts, re-run the tests
  and linter, then merge the feature branch into `main` and `git push origin
  main`. GitHub records the corresponding PR as merged once its head commit is
  reachable from `main`.

## Development

- Install for tests: `python3 -m pip install -e '.[dev]'`.
- Run the suite: `python3 -m unittest discover -s tests`.
- Lint: `python3 -m ruff check .`.
