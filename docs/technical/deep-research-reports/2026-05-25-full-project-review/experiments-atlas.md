# Experiments Atlas

This atlas maps experiment setup, pipeline architecture, results, and thesis-safe deliverables.

## Headline Result

| Lane | Setup | Pipeline | Result | Thesis role |
|---|---|---|---|---|
| V2+ schedule/value learner | 5 tenants, 90 tenant-anchors, official global panel NBEATSx sources | Dagster gold offline comparison against frozen strict LP/oracle and V2 | Mean regret `174.77` UAH, strict mean `310.58` UAH, V2 mean `206.37` UAH, 4/4 rolling robustness | Headline offline research challenger |

Artifact:

- `data/research_runs/week3_official_global_panel_schedule_value_v2_plus_comparison/dfl_schedule_value_learner_v2_plus_comparison.md`

## Non-Promoted and Negative Evidence

| Lane | Setup | Result | Status | Safe wording |
|---|---|---|---|---|
| DFL V2 | Schedule/value learner predecessor | Evidence exists but gate not headline | Superseded by V2+ | "Frozen V2 comparator" |
| TFT quantile 365 | Official TFT/NBEATSx forecast-screen branch | TFT evidence computed but not promoted over V2+ | Negative/complementary | "TFT is a forecast adapter candidate, not the headline scheduler" |
| Poland lag24 features | 24 lagged exogenous columns, 17 pass, 7 null-blocked | Latest-holdout signal positive but rolling status `positive_not_promoted`; full 90-row comparison does not beat Ukrainian-only V2+ mean | Promising, not promoted | "Point-in-time external context research only" |
| DT research shadow | HF Decision Transformer importable, candidate-index/schedule-family target | 97,431 research-shadow rows, 7,300 sequences, 360 eval sequences; mean regret `507.90` vs strict `431.70` | Not promotable | "Offline sequence-policy shadow, not controller" |
| Regret-aware V2+ selector | Weighted value-gap ranker with explicit V2+ abstention | Selector mean regret `174.77` UAH equals V2+; `0 / 90` non-V2+ switches; `90 / 90` abstentions | Conservative negative evidence | "Correct objective trained, but current features do not justify replacing V2+" |
| LAVA NPZ smoke | 8-instance neighbor/margin smoke | Contract and hashes pass; promotion false; V13 blocked | CI/prototype only | "NPZ smoke contract validation" |
| V13 acquisition | Ukrainian source-readiness preflight | explicit DAM receipts missing; safe-switch examples below required count; ready rows 0 | Blocked | "Source acquisition gate, not modeling slice" |

## Current Validation Snapshot

Fresh command evidence is recorded in `verification-refresh-2026-05-25.md`.

| Command | Status | Notes |
|---|---|---|
| `uv run ruff check .` | Pass | Style/lint passes |
| `uv run mypy .` | Pass | 247 source files passed |
| `uv run pytest -p no:cacheprovider tests` | Pass | 945 tests passed in 616.85s |
| `uv run dg check defs` | Pass | Dagster component YAML and definitions loaded |
| `uv run dg list defs --json` | Pass | 335 assets, 63 checks, 2 jobs, 2 schedules |
| `docker compose config --quiet` | Pass | Compose syntax/config ok |
| `dashboard npm run typecheck` | Pass | Nuxt/TS checks pass |
| `dashboard npm exec -- vitest run` | Pass | 66 dashboard tests pass |
| `/operator` Browser/Playwright dock check | Pass | Desktop/mobile evidence shows no dock occlusion, no horizontal overflow, and no console/page errors |

## Deliverable Map

Use these as thesis/demo anchors:

- Headline result: V2+ packet and Chapter 4 discussion.
- Governance result: V13 acquisition packet.
- Academic MVP result: credentialless academic MVP validation.
- UI result: operator and defense dashboard screenshots.
- Research roadmap: DT shadow, LAVA smoke, TFT, and Poland branches.

Do not use DT/LAVA/Poland/TFT as replacement headline until their own promotion gates pass.
