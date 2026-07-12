# Final Metrics Atlas

Date: 2026-06-16

Purpose: map every final README/defense number to a source, status, and claim
boundary. This is the first file to open when a reviewer asks, "Where did this
number come from?"

Global boundary: all rows are offline/read-model or manual shadow/demo evidence.
They do not permit market submission, inverter dispatch, or a default
production strategy switch. `market_execution_enabled=false` remains fixed.

## Headline Metrics

| Metric | Value | Source artifact | Status | Boundary |
| --- | ---: | --- | --- | --- |
| Strict LP/oracle comparator mean regret | 310.58 UAH | `docs/thesis/chapters/04-results-and-discussion.md`, `docs/technical/OFFICIAL_GLOBAL_PANEL_NBEATSX.md` | comparator | evaluator only, not UI default |
| Strict LP/oracle comparator median regret | 198.39 UAH | `docs/thesis/chapters/04-results-and-discussion.md` | comparator | offline scoring only |
| V2 forecast selector mean regret | 206.37 UAH | `docs/thesis/chapters/04-results-and-discussion.md`, `docs/technical/OFFICIAL_GLOBAL_PANEL_NBEATSX.md` | historical baseline | not production execution |
| V2 forecast selector median regret | 96.02 UAH | `docs/thesis/chapters/04-results-and-discussion.md` | historical baseline | offline/read-model evidence |
| Schedule/Value Learner V2+ mean regret | 174.77 UAH | `docs/technical/DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS.md`, `docs/thesis/chapters/04-results-and-discussion.md` | headline/default evidence | no market payload |
| Schedule/Value Learner V2+ median regret | 67.30 UAH | `docs/thesis/chapters/04-results-and-discussion.md` | headline/default evidence | no market payload |
| V2+ rolling robustness | 4 / 4 windows | `docs/thesis/chapters/04-results-and-discussion.md` | headline robustness | offline/read-model evidence |
| Random-forest V2+ safe-switch mean regret | 168.16 UAH | `docs/technical/final-evidence/dt_v2_plus_canonical_aggregate.md` | exact-mirror in-packet diagnostic | historical artifact id `dt_v2_plus`; not DT or OOS |
| Random-forest V2+ safe-switch median regret | 61.71 UAH | `docs/thesis/ERRATA_MODEL_LINEAGE_2026-07-12.md` | exact-mirror in-packet diagnostic | four switches occur on one date |
| Random-forest non-V2+ switches | 4 / 90 rows | `docs/technical/final-evidence/dt_v2_plus_canonical_aggregate.md` | in-packet diagnostic | V2+ remains fallback; one distinct switch date |
| Random-forest abstentions | 86 / 90 rows | `docs/technical/final-evidence/dt_v2_plus_canonical_aggregate.md` | in-packet diagnostic | abstain-to-V2+ behavior |
| Temporal DT primary suite | 0 / 36 beneficial; 33 ties; 3 harmful | `docs/technical/final-evidence/dt_temporal_v2_plus_experiment.md` | post-defense, zero-overlap temporal research shadow | two sources, three windows, two objectives, three seeds |
| Decision-aware temporal DT | 18 / 18 ties with V2+ | `runs/dt_temporal_v2_plus/temporal_suite_rows.csv` | DFL-style candidate ranking, not full differentiable DFL | no non-V2+ switches; not promoted |
| Cross-entropy temporal DT | 15 / 18 ties; 3 harmful | `runs/dt_temporal_v2_plus/temporal_suite_rows.csv` | candidate-index imitation | harmful deltas +10.12, +12.28, +21.09 UAH |
| Random-forest recovered opportunities | 3 / 15 filtered opportunities | `docs/thesis/ERRATA_MODEL_LINEAGE_2026-07-12.md` | historical diagnostic | denominator requires the stored selector filter |
| RF temporal suite latest-window delta | 0.00 UAH | `docs/technical/final-evidence/rf_safe_switch_temporal_replay.md` | post-defense negative evidence | both sources and thresholds 0/5/10/20/50 fully abstain |
| RF temporal suite beneficial protocols | 0 / 14 rows | `docs/technical/final-evidence/rf_safe_switch_temporal_replay.md` | post-defense negative evidence | zero content overlap throughout |
| RF temporal suite harmful protocols | 3 / 14 rows | `docs/technical/final-evidence/rf_safe_switch_temporal_replay.md` | post-defense negative evidence | primary-seed harm +65.18 to +123.08 UAH; direction stable across three seeds |
| HF value-aligned frozen mean regret signal | 158.71 UAH | `docs/technical/final-evidence/hf_safe_switch_robustness_summary.md`, `docs/technical/final-evidence/hf_value_aligned_shadow_promotion_proof.md` | manual shadow/demo evidence | candidate-library only |
| HF value-aligned non-fallback days | 20 / 32 source-backed DAM days | `docs/technical/final-evidence/hf_value_aligned_shadow_promotion_proof.md` | manual shadow/demo evidence | no production promotion |
| HF readiness matrix | 8 / 8 DAM/IDM cases | `docs/technical/final-evidence/hf_value_aligned_forecast_readiness_summary.md` | demo readiness | preview context only |
| HF demo packet | positive + abstention cases | `docs/technical/final-evidence/hf_live_safe_switch_value_aligned_shadow_demo_packet.md` | commission demo evidence | manual preview only |

## Figure And README Assets

| Asset | Path | Purpose |
| --- | --- | --- |
| Operator screenshot | `docs/technical/final-demo-assets/operator-preview-desktop.png` | First README visual for the product surface |
| Optional defense screenshot | `docs/technical/final-demo-assets/defense-dashboard-desktop.png` | Defense evidence screen for demo rehearsal |
| Regret ladder | `docs/thesis/chapters/assets/compact-fig-4-1-regret-ladder.png` | Shows strict, V2, and V2+ result ordering |
| Architecture comparison | `docs/thesis/chapters/assets/compact-fig-4-3-architecture-comparison.png` | Shows progression from baseline to guarded research paths |
| HF readiness matrix | `docs/thesis/chapters/assets/compact-fig-4-8-hf-readiness-matrix.png` | Shows DAM/IDM latest/today/tomorrow/day+2 readiness |

## Reviewer Inspection Order

1. `README.md` for the product story and quickstart.
2. `docs/technical/FINAL_DEFENSE_RUNBOOK.md` for the live demo.
3. `docs/technical/FINAL_EVIDENCE_INDEX.md` for the curated source map.
4. `docs/technical/FINAL_METRICS_ATLAS.md` for numeric traceability.
5. `docs/technical/API_ENDPOINTS.md` for API contracts and boundaries.
6. `/operator`, then `/defense`, then `http://127.0.0.1:8000/docs`.

## Non-Claimed Items

- No live trading.
- No market-submittable DAM/IDM bids.
- No `ProposedBid`.
- No market order payload.
- No deployed Decision Transformer/LAVA controller.
- No full differentiable DFL controller.
- No V13 modeling success.
- No claim that HF replaces V2+ in production.

