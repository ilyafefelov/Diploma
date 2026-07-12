# Final Evidence Index

Date: 2026-06-16

Purpose: provide a compact, GitHub-safe index of the evidence needed for final
repository review and thesis defense. This file intentionally links to tracked
documents and curated summaries, not ignored local `data/research_runs` paths.

Global boundary:

- Product surface: DAM/IDM hourly operator recommendation preview/read model.
- Market execution: disabled.
- Full DFL controller: not claimed.
- Deployed Decision Transformer controller: not claimed.
- V13: source-readiness/acquisition gate, not a modeling slice.

## Headline Claims

| Claim | Current status | Tracked evidence | Boundary |
| --- | --- | --- | --- |
| V2+ is the headline/default schedule-value evidence | Confirmed | [DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS.md](DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS.md), [regret ladder](../thesis/chapters/assets/compact-fig-4-1-regret-ladder.png) | offline/read-model evidence only |
| Historical `dt_v2_plus` random forest is lower than V2+ on an exact-mirror packet | In-packet diagnostic only | [corrected model-lineage aggregate](final-evidence/dt_v2_plus_canonical_aggregate.md), [post-defense erratum](../thesis/ERRATA_MODEL_LINEAGE_2026-07-12.md) | not DT, not OOS, not promoted |
| HF value-aligned shadow has the strongest live shadow/demo signal | Manual shadow/demo evidence | [HF promotion proof](final-evidence/hf_value_aligned_shadow_promotion_proof.md), [HF robustness summary](final-evidence/hf_safe_switch_robustness_summary.md) | manual preview only |
| HF value-aligned shadow covers DAM/IDM latest/today/tomorrow/day+2 readiness | Confirmed readiness | [HF readiness summary](final-evidence/hf_value_aligned_forecast_readiness_summary.md), [HF readiness matrix](../thesis/chapters/assets/compact-fig-4-8-hf-readiness-matrix.png) | source-backed preview rows only |
| HF demo packet gives commission-ready positive and abstention cases | Confirmed demo packet | [HF demo packet](final-evidence/hf_live_safe_switch_value_aligned_shadow_demo_packet.md), [defense runbook](FINAL_DEFENSE_RUNBOOK.md) | no market order payload |
| V13 remains an acquisition/source-readiness gate | Blocked/precondition only | [CURRENT_GOAL_BOUNDARY_V13.md](CURRENT_GOAL_BOUNDARY_V13.md), [DFL_UA_CONTEXT_ACQUISITION_V13.md](DFL_UA_CONTEXT_ACQUISITION_V13.md) | not a modeling/training success |
| API and dashboard are operator-preview surfaces | Confirmed | [API_ENDPOINTS.md](API_ENDPOINTS.md), [dashboard README](../../dashboard/README.md), [OPERATOR_DAM_TIMING_AND_BID_BOUNDARY.md](OPERATOR_DAM_TIMING_AND_BID_BOUNDARY.md) | no `ProposedBid` |

## Curated Evidence Packets

| Packet | Why it matters |
| --- | --- |
| [final-evidence/dt_v2_plus_canonical_aggregate.md](final-evidence/dt_v2_plus_canonical_aggregate.md) | Gives the random-forest identity, exact-mirror limitation, single switch date, and vector-parse trust check for the historical `dt_v2_plus` artifact. |
| [final-evidence/hf_safe_switch_robustness_summary.md](final-evidence/hf_safe_switch_robustness_summary.md) | Shows the HF shadow robustness gate and frozen regret signal. |
| [final-evidence/hf_value_aligned_shadow_promotion_proof.md](final-evidence/hf_value_aligned_shadow_promotion_proof.md) | Shows the value-aligned candidate-library gate and non-execution flags. |
| [final-evidence/hf_value_aligned_forecast_readiness_summary.md](final-evidence/hf_value_aligned_forecast_readiness_summary.md) | Shows 8/8 DAM/IDM readiness cases for latest/today/tomorrow/day+2. |
| [final-evidence/hf_live_safe_switch_value_aligned_shadow_demo_packet.md](final-evidence/hf_live_safe_switch_value_aligned_shadow_demo_packet.md) | Gives the short operator demo packet for commission review. |
| [FINAL_METRICS_ATLAS.md](FINAL_METRICS_ATLAS.md) | Maps final README/defense numbers to source artifacts and claim boundaries. |
| [FINAL_UNIVERSITY_RUBRIC_MATRIX.md](FINAL_UNIVERSITY_RUBRIC_MATRIX.md) | Maps the university 50-point software/experimental rubric to demo steps, evidence, verification, and risk controls. |
| [FINAL_REVIEW_CHECKLIST.md](FINAL_REVIEW_CHECKLIST.md) | Gives the final GitHub, dashboard, API, evidence, and verification checklist. |
| [BUSINESS_VALUE_NOTE.md](BUSINESS_VALUE_NOTE.md) | Frames the business value without claiming live market execution or ROI. |
| [final-demo-assets/README.md](final-demo-assets/README.md) | Defines the tracked screenshots used for the GitHub-facing demo package. |

## Review Checklist

- README links resolve to tracked files.
- Generated `outputs/`, `output/`, `analysis_outputs/`, and local runtime caches
  are not tracked.
- API docs match the decorators in `api/main.py`.
- Dashboard docs do not use stale `0 / 90` claims as the current result.
- All market-facing language says preview/read model, not trading or execution.
- Any local-only artifact references are explicitly labeled as local evidence,
  not GitHub links.
