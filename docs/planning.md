# Planning

[mission](mission.md) → [requirements](requirements.md) → **plan** (GitHub)

The plan lives on GitHub: milestones, issues, and the
[`chattrs dev` board](https://github.com/users/bolu-atx/projects/2).

## Shape

- **Milestone = phase.** Named for the outcome. Description states the exit test.
  Work only the earliest open milestone.
- **Issue = story.** One shippable behavior. Title states the outcome.
- **Relationships** record real order only: `blocked-by` / `blocking`.
  Sub-issues only for genuine decomposition.
- Every issue and PR cites requirement IDs: `Meets: MSG-1`.
  No requirement fits? Propose one in an issue first.

## Issue body

```md
## What & why
One paragraph: what changes for the user, and why.

## How
Meets: <IDs>. Steps in order. Name blockers.

## Verification performed
Commands run and results. Do not claim what was not run.

## Agents supplemental
Prompt and verifier used.
```

## Labels

One subject label: `feature`, `bugfix`, `docs`, `chore`.

## Pull requests

Every PR links its issue. GitHub does the bookkeeping. Do not close
issues or move cards by hand.

- **Partial PR:** first line `Part of #N`. Links the PR, keeps the issue open.
- **Last PR of a story:** first line `Closes #N`. Merge closes the issue.
- **Same milestone as the issue** on every PR, so milestone progress counts it.
- **Add to the board** (`gh pr create -p "chattrs dev"`). Merged PRs and
  closed issues move to Done automatically.
- **Milestone closes** when its last issue closes and the exit test in its
  description passes. The PR that closes the last issue quotes the exit
  test result. Then close the milestone.
- **Dogfood after a milestone closes:**
  1. Build and install from a clean worktree at `origin/main`:
     `cargo install --path . --locked`.
  2. Re-run `chattrs hooks install`. It is safe to repeat. Restart the
     local broker if one runs.
  3. Check `chattrs --version`, and run one `send` and `watch` smoke test
     against an isolated broker on a scratch port and database. Never run
     smoke tests against the real swarm broker.
  4. Report the installed commit to the owner.
- **Blocked issue:** do not open a PR until its blockers are closed.

- Branch: `feat/…`, `fix/…`, `docs/…`, `refactor/…`, `chore/…`.
- Done = mergeable: checks green, no conflicts.

## Phases

### First run

Outcome: a new user goes from `brew install chattrs` to a working swarm
using only the README. Chattrs still never hosts or supervises an agent.

Requirements: SETUP-1, SETUP-2, SETUP-3, SETUP-4, OPS-1, SEC-5.

Exit test: a clean macOS account (or a temp HOME) and bobox go from nothing
to a working swarm using only the README, with `chattrs doctor` passing on
both. The test uses an isolated broker port and database.

### Hooks and agent status

Outcome: workers get a lead's messages between tool calls without polling,
and `chattrs agents` shows what each worker is doing. No viewer is needed.

Issues: #63 (agent state, MON-4), #64, #65 (hook adapters, HOOK-1).

Exit test: a Claude Code worker and a Codex worker, each with
`chattrs hooks install`, get a lead's message between tool calls and before
they stop, with no Monitor needed for Codex. `chattrs agents` shows each
worker's state change through working, idle, and waiting. `chattrs hooks
remove` restores each config file exactly. No viewer (tray, TUI, or GUI) is
needed for any step.
