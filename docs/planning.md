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

- Every PR closes an issue: `Closes #N`.
- Branch: `feat/…`, `fix/…`, `docs/…`, `refactor/…`, `chore/…`.
- Done = mergeable: checks green, no conflicts.
