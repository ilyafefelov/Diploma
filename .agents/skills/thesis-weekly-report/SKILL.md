---
name: thesis-weekly-report
description: Drafts and updates thesis progress artifacts for this diploma repo, including weekly reports, presentation scripts, and literature review chapter drafts aligned to the syllabus. Use when working on `docs/thesis/weekly-reports`, `docs/thesis/chapters`, weekly progress reports, literature review drafts, or supervisor-ready reporting materials.
---

# Thesis Weekly Report

## Quick start

1. Read the relevant `docs/syllabus/` files first.
2. Decide which artifact is being produced:
   - weekly progress report
   - literature review chapter draft
   - presentation script or demo-support material
3. Write into canonical repo paths:
   - `docs/thesis/weekly-reports/weekN/`
   - `docs/thesis/chapters/`
4. Keep verified implementation separate from planned research direction.

## Workflows

### Weekly report

- Treat `docs/syllabus/` as the source of truth.
- Include completed work, risks, next-week plan, and artifact links.
- Prefer Ukrainian unless the user asks for English.
- Keep every claim tied to verified repo state.
- Link code, docs, screenshots, and demo materials where available.

### Literature review draft

- Write analytically, not as a flat list of sources.
- Structure by themes that justify the chosen architecture.
- For each theme, cover: existing approaches, limitations, gap, and project implication.
- Tie each source to a concrete design choice in repo language.
- Mark future architecture as planned if it is not implemented yet.

### Thesis chapter updates

- Put reusable chapter text in `docs/thesis/chapters/`.
- Put week-specific evidence in `docs/thesis/weekly-reports/weekN/`.
- Keep chapter drafts reusable for the explanatory note.
- Avoid duplicating long analytical text across weekly artifacts.

### Validation checklist

- Syllabus-aligned
- Ukrainian by default
- No overclaims
- Artifact links included
- Terminology consistent with `CONTEXT.md`
- Clear separation between MVP baseline and target DFL architecture

## Advanced features

See [REFERENCE.md](REFERENCE.md) and [EXAMPLES.md](EXAMPLES.md).