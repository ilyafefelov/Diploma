# Tracker Flow

Status: confirmed locally, external publish flow not yet automated in this workspace

## Current Confirmation

- Git remote points at GitHub: `https://github.com/ilyafefelov/Diploma`
- The repository does not currently expose a workspace-local issue-tracker label mapping for the skill workflows.
- GitHub CLI (`gh`) is not available in the current PowerShell environment, so AI issue publishing cannot be confirmed end-to-end from this session.

## Canonical Workflow For This Repo Right Now

Until GitHub issue automation is explicitly configured, the canonical tracker flow for planning skills in this repository is:

1. Write the approved PRD into `docs/technical/`.
2. Write independently-grabbable backlog slices into `docs/technical/issues/`.
3. Keep the issue files dependency-ordered and demo-oriented.
4. Commit planning artifacts in focused commits so weekly reports and demos can reference stable hashes.

## Skill Workflow Mapping

- `/to-prd` output maps to a local PRD markdown artifact under `docs/technical/`.
- `/to-issues` output maps to local issue-style markdown artifacts under `docs/technical/issues/`.
- `/triage` state labels are tracked narratively inside these markdown artifacts until GitHub label automation is available.

## What Is Needed For External Tracker Automation Later

- Install and authenticate GitHub CLI (`gh`) in the active environment.
- Confirm the repo should use GitHub Issues as the canonical external tracker.
- Define the label mapping for skill states, especially:
  - `enhancement`
  - `bug`
  - `needs-triage`
  - `needs-info`
  - `ready-for-agent`
  - `ready-for-human`
  - `wontfix`

## Why This Is Acceptable For Now

The current local markdown tracker flow is good enough for:

- dependency-aware backlog planning
- weekly report references
- thesis/demo artifact linking
- focused implementation against approved slices

It is not a substitute for a fully automated external issue tracker, but it removes ambiguity about where approved planning artifacts live today.