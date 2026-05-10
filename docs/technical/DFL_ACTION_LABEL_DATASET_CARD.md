# DFL Action-Label Dataset Card

Date: 2026-05-08

This card summarizes the current Ukrainian DAM action-label panel for future
DFL experiments. It is a research dataset, not a trained DFL model, not
Decision Transformer control, and not market execution.

## Dataset Identity

| Field | Value |
|---|---|
| Asset | `dfl_action_label_panel_frame` |
| Dagster group | `gold_dfl_training` |
| Readiness check | `dfl_action_label_panel_readiness_evidence` |
| Materialization/check run | `3743f42c-8cc6-4822-a3f0-7730af6af458` |
| Postgres table | `dfl_action_label_vectors` |
| Tenants | 5 canonical Ukrainian tenants |
| Source models | `tft_silver_v0`, `nbeatsx_silver_v0` |
| Strict control | `strict_similar_day` |
| Oracle target | strict LP over realized DAM horizon prices |
| Market/currency | DAM / UAH |
| Claim scope | `dfl_action_label_panel_not_full_dfl` |

## Readiness Result

| Check | Result |
|---|---:|
| Rows | 1,040 |
| Tenants | 5 |
| Source models | 2 |
| Anchors per tenant/model | 104 |
| Train-selection rows | 860 |
| Final-holdout rows | 180 |
| Final-holdout anchors per tenant/model | 18 |
| First anchor | `2026-01-08 23:00` |
| Last anchor | `2026-04-29 23:00` |
| Final-holdout window | `2026-04-12 23:00` to `2026-04-29 23:00` |
| Bad vector-length rows | 0 |
| Safety violations | 0 |
| Claim flags | all `not_full_dfl=true`, all `not_market_execution=true` |

The readiness check passed in Dagster. It requires the expected tenants and
source models, at least 90 anchors per tenant/model, exactly 18 latest
final-holdout anchors, no train/final overlap, matching vector lengths,
one-hot action masks, thesis-grade observed rows, and zero safety violations.

## Label Balance

| Split | Rows | Charge labels | Discharge labels | Hold labels | Label hours |
|---|---:|---:|---:|---:|---:|
| `train_selection` | 860 | 3,046 | 4,540 | 13,054 | 20,640 |
| `final_holdout` | 180 | 642 | 772 | 2,906 | 4,320 |
| total | 1,040 | 3,688 | 5,312 | 15,960 | 24,960 |

Overall label mix: charge 14.8%, discharge 21.3%, hold 63.9%. The hold class
is dominant, but both active action classes are represented in train and final
splits.

## Value And Regret Summary

| Metric | Value |
|---|---:|
| Mean candidate regret, UAH | 1,270.28 |
| Median candidate regret, UAH | 862.89 |
| Mean strict baseline regret, UAH | 734.36 |
| Mean candidate minus strict regret, UAH | 535.91 |
| Mean candidate net value, UAH | 2,303.42 |
| Mean strict baseline net value, UAH | 2,839.33 |
| Mean oracle net value, UAH | 3,573.69 |

The labels reinforce the current finding: raw neural forecast candidates are
useful training inputs, but they still underperform the frozen
`strict_similar_day` control in decision value. This is a reason to train and
gate carefully, not a reason to promote a model.

## Coverage Notes

The coverage audit found 104 eligible daily anchors per tenant and met the
90-anchor target. It also recorded one price/weather source gap per tenant in
the wider feature frame. The persisted action-label rows themselves are all
observed coverage and `thesis_grade`.

European sources remain research-only bridge material. They must not be mixed
into this Ukrainian DAM/UAH panel until currency, timezone, price-cap,
market-rule, licensing/API, and domain-shift normalization questions are
resolved.

## Next Gate

The first supervised action-label baseline has now materialized as
`dfl_action_classifier_baseline_frame` in Dagster run
`91fe584d-73f9-41ca-b3e9-88288136b8b7`. It trained only on
`train_selection` and scored the immutable final holdout:

| Scope | Final-holdout rows | Label hours | Accuracy | Macro F1 | Status |
|---|---:|---:|---:|---:|---|
| all source models | 180 | 4,320 | 0.6495 | 0.5364 | blocked |
| `tft_silver_v0` | 90 | 2,160 | 0.6685 | 0.5589 | blocked |
| `nbeatsx_silver_v0` | 90 | 2,160 | 0.6306 | 0.5141 | blocked |

The status is
`blocked_classification_only_no_strict_lp_value`: this is action-label
classification evidence, not a strict LP/oracle decision-value result. The next
technical gate is to project predicted actions into feasible dispatch vectors
and score them against `strict_similar_day` under the existing promotion gate.
