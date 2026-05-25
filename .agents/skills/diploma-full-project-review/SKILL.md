---
name: diploma-full-project-review
description: Runs a commission-style full project review for the Diploma repo across market rules, Dagster assets, experiments, thesis docs, dashboard/API, and external academic/industry benchmarks. Use when the user asks for a full project review, thesis audit, academic committee critique, ADR/PRD/data-flow/experiment atlas, dashboard review, or Daxter/Dagster end-to-end review in `D:\School\GoIT\Courses\Diploma`.
---

# Diploma Full Project Review

## Quick start

1. Work from `D:\School\GoIT\Courses\Diploma`.
2. Read the local project instructions first: `AGENTS.md` when present, then `CONTEXT.md`, `README.md`, and `docs/syllabus/` for academic workflow questions.
3. Treat the current product boundary as DAM delivery-day operator recommendation preview and offline/read-model evidence. Preserve `market_execution_enabled=false`.
4. Use a review stance: findings first, severity ordered, exact file/line evidence, then recommendations.
5. Create or update a dated report packet under `docs/technical/deep-research-reports/<YYYY-MM-DD>-full-project-review/`.

## Mandatory boundaries

- V13 is an acquisition/source-readiness gate, not a modeling slice.
- Do not describe current outputs as market-submittable bids, live trading, deployed Decision Transformer control, promoted LAVA, or full differentiable DFL.
- DT/LAVA remain blocked until required source families are ready and each tenant/source has at least 20 prior/train non-tail-risk material safe-switch examples.
- EU/Poland rows may be governed exogenous/context evidence; they are not Ukrainian training targets without explicit governance closure.
- Follow the Safe Refactor Protocol before any code edits. For a review-only task, prefer docs/report artifacts and do not change behavior.

## Review workflow

1. **Establish sources**
   - Inspect dirty worktree status without reverting user changes.
   - Load prior review packets and local materialized research artifacts.
   - If the user provides a Google Docs thesis link, read it with the Google Drive connector.
   - Browse for current market-rule, competitor, and academic comparator sources.

2. **Audit market and safety logic**
   - Check `configs/market_rules_ua.yaml`, active gatekeeper schemas, and any legacy duplicate contracts.
   - Verify date-aware price-cap handling and no-market-execution boundaries.

3. **Audit Dagster and pipelines**
   - Use Dagster commands from [REFERENCE.md](REFERENCE.md).
   - Report assets/check counts, failures, and pipeline scope.

4. **Audit experiments**
   - Map each thesis claim to a run slug, artifact path, comparator, coverage, metric, and claim boundary.
   - Separate headline evidence from negative evidence, shadow evidence, smoke tests, and blocked gates.

5. **Audit thesis/docs**
   - Cross-check local `docs/thesis/` and online thesis text against artifact paths.
   - Flag stale verification counts, unsupported numeric claims, and ambiguous wording.

6. **Audit dashboard/API**
   - Start local services only when needed.
   - Use Browser/Playwright screenshots and console checks for `/operator` and `/defense`.
   - Look for layout overlap, stale wording, console errors, and API/schema typing issues.

7. **Deliver packet**
   - Include `README.md`, `review.md`, `adr.md`, `prd.md`, `data-flow.md`, `experiments-atlas.md`, `dashboard-review.md`, `external-benchmark.md`, `plain-language-review.md`, `fix-plan.md`, `source-matrix.md`, and optional `infographics.md`.
   - Keep diagrams deterministic with Mermaid/SVG when they are simple data-flow visuals. Use image generation only for raster visuals that benefit from a generated bitmap.

## Advanced features

See [REFERENCE.md](REFERENCE.md) for repo commands, artifact paths, and evidence checklists. See [EXAMPLES.md](EXAMPLES.md) for output shapes.

