# DFL Point-In-Time Context Repair + Candidate-Value DFL V5

## Status

This slice repairs the context gap exposed by Plateau-Breaker V4 before trying
another Decision Transformer or larger selector. It keeps Ukrainian-only
Schedule/Value Learner V2+ as the frozen thesis comparator:

- calibrated V2+ mean regret: `174.77` UAH;
- raw V2+ mean regret: `193.36` UAH;
- rolling robustness: `4 / 4`;
- claim scope: Offline Strategy Promotion only;
- `market_execution_enabled=false`.

V5 is additive. It does not route Poland/ENTSO-E/European market-coupling
features into training, does not switch dashboard/API defaults, and does not
claim live market execution.

## Why This Exists

V4 showed that the plateau is not mainly caused by a missing tiny DT variant.
The strongest current learner falls back to V2+ because the available candidate
schedules and prior features do not prove a reliable improvement. The V4
data-quality audit marked these concrete context gaps:

- weather/load context;
- calendar, holiday, outage, or event context;
- publication-time availability.

V5 therefore repairs point-in-time context first, then reruns a candidate-value
gate against the unchanged V2+ baseline.

## New Assets

The new assets are:

- `dfl_point_in_time_context_repair_audit_frame`;
- `dfl_point_in_time_context_feature_panel_frame`;
- `dfl_context_enriched_schedule_candidate_library_v5_frame`;
- `dfl_context_enriched_candidate_value_label_panel_v5_frame`;
- `dfl_context_enriched_candidate_value_dfl_v5_frame`;
- `dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame`.

The new checks are:

- `dfl_point_in_time_context_repair_audit_evidence`;
- `dfl_point_in_time_context_feature_panel_evidence`;
- `dfl_context_enriched_candidate_value_dfl_v5_evidence`.

Tracked config:

- `configs/real_data_dfl_point_in_time_context_v5_week3.yaml`.

## Context Audit Contract

The context repair audit emits concrete rows by source model, tenant, anchor,
feature family, and blocker. The allowed blocker labels are:

| Blocker | Meaning |
| --- | --- |
| `missing_weather_load_context` | Weather/load features are absent or not available before the anchor. |
| `missing_calendar_event_context` | Calendar/event features are absent or not timestamped before the anchor. |
| `missing_publication_time` | Publication-time evidence is absent or occurs after the anchor. |
| `context_available_not_used` | Context is available but was not used by the upstream V4/V2+ candidate path. |
| `context_ready` | The feature family is prior-available and can be converted into selector features. |

The audit uses final regret and failure labels only as diagnostics. Those labels
must not become selector features.

## Feature Panel Contract

`dfl_point_in_time_context_feature_panel_frame` produces one prior-only context
row per source model, tenant, and anchor. Selector inputs are explicitly
prefixed with `selector_feature_*`, including:

- context-readiness indicators for weather/load, calendar/event, and
  publication-time evidence;
- blocker counts;
- anchor hour, day-of-week, and weekend flags;
- Ukrainian weather/load values when available.

Realized outcomes remain in `label_*` or `diagnostic_*` columns. The evidence
check blocks feature panels that contain Poland, ENTSO-E, or EU-derived
training inputs.

## Candidate-Value DFL V5

V5 reuses the V4 candidate families but enriches the candidate-level value
scorer with the point-in-time context features. The scorer still works at the
candidate-schedule level, not raw hourly BUY/SELL/HOLD imitation:

```text
V4 candidate schedules
  + prior-only context feature panel
  -> context-enriched schedule/value label panel
  -> candidate-level value scorer
  -> V2+ fallback unless train/prior evidence predicts non-degrading improvement
  -> strict LP/oracle scoring
```

The final holdout actuals affect only realized labels and strict scoring. They
do not fit weights, choose feature columns, choose fallback thresholds, or
generate selector features.

## Gate

V5 can replace V2+ as headline evidence only if it:

- improves mean regret versus frozen V2+;
- does not worsen median regret versus frozen V2+;
- still beats `strict_similar_day` by at least `5%` mean regret;
- preserves rolling robustness before any headline change;
- preserves thesis-grade Ukrainian observed coverage;
- has zero safety violations;
- keeps `market_execution_enabled=false`.

If V5 matches V2+, the correct interpretation is not "DT failed." It means the
current Ukrainian-only context and candidate space is likely exhausted under the
unchanged strict gate, and the next branch should be either stronger Ukrainian
context acquisition/backfill or a teacher-trajectory DT experiment with V2+ and
oracle schedules as teachers.

## Run

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select dfl_official_global_panel_schedule_candidate_library_v3_frame,dfl_official_global_panel_candidate_value_label_panel_v3_frame,dfl_official_global_panel_candidate_value_dfl_v3_frame,dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame,dfl_official_global_panel_v2_v3_plateau_autopsy_frame,dfl_official_global_panel_plateau_data_quality_audit_frame,dfl_official_global_panel_schedule_candidate_library_v4_frame,dfl_point_in_time_context_repair_audit_frame,dfl_point_in_time_context_feature_panel_frame,dfl_context_enriched_schedule_candidate_library_v5_frame,dfl_context_enriched_candidate_value_label_panel_v5_frame,dfl_context_enriched_candidate_value_dfl_v5_frame,dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame -c configs/real_data_dfl_point_in_time_context_v5_week3.yaml
```

Claim boundary:

- no live market execution;
- no dashboard/API default switch;
- no Poland/ENTSO-E/EU feature training;
- `strict_similar_day` remains fallback/control;
- V2+ remains headline evidence unless V5 beats it under the unchanged gate.
