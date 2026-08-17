# Full Project Review — 2026-08-17

This packet is the release review for the Diploma repository at the pre-merge
state of `codex/security-dependency-release-2026-08-17`. It separates verified
runtime evidence from research aspirations and keeps the project boundary at
operator preview / offline evidence with `market_execution_enabled=false`.

## Verdict

Release-ready after the review branch passes hosted CI and review comments are
resolved. The local release gates are green: 1,079 Python tests, Ruff, mypy over
283 files, Dagster definition loading, 261 dashboard tests, dashboard lint and
typecheck, static generation, Compose validation, and zero npm audit findings
in both Node workspaces.

The principal pre-review risks were 99 open dependency alerts, no pull-request
CI workflow, stale tests coupled to untracked local evidence, and missing local
Nuxt icon bundling. This branch addresses those risks. Remaining limitations
are recorded rather than promoted into claims: the clean checkout has no V13
acquisition packet, DT/LAVA materialization inputs are absent, the public bundle
has one chunk above 500 kB, and Windows needs a compiler or a compatible wheel
for a fresh `diffcp` build.

## Packet index

- [review.md](review.md) — findings, evidence, and disposition.
- [adr.md](adr.md) — release decisions and rejected alternatives.
- [prd.md](prd.md) — product boundary and acceptance criteria.
- [data-flow.md](data-flow.md) — source-to-read-model flow.
- [experiments-atlas.md](experiments-atlas.md) — implemented experiment lanes.
- [dashboard-review.md](dashboard-review.md) — route and frontend assessment.
- [external-benchmark.md](external-benchmark.md) — primary-source comparison.
- [plain-language-review.md](plain-language-review.md) — non-technical summary.
- [fix-plan.md](fix-plan.md) — completed and deferred work.
- [source-matrix.md](source-matrix.md) — claim-to-source matrix.
