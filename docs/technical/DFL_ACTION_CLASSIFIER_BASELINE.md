# DFL Action Classifier Baseline

Date: 2026-05-08

This note records the first supervised action-label baseline over the Ukrainian
DAM DFL v2 action-label panel. It is a transparent classification probe for
future DFL work, not full DFL, not Decision Transformer control, not strict LP
promotion evidence, and not market execution.

## Identity

| Field | Value |
|---|---|
| Asset | `dfl_action_classifier_baseline_frame` |
| Upstream dataset | `dfl_action_label_panel_frame` |
| Dagster group | `gold_dfl_training` |
| Baseline name | `dfl_action_classifier_v0` |
| Materialization run | `91fe584d-73f9-41ca-b3e9-88288136b8b7` |
| Tenants | 5 canonical Ukrainian tenants |
| Source models | `tft_silver_v0`, `nbeatsx_silver_v0` |
| Train split | 86 anchors per tenant/model |
| Final holdout | latest 18 anchors per tenant/model |
| Claim scope | `dfl_action_classifier_baseline_not_full_dfl` |
| Promotion status | `blocked_classification_only_no_strict_lp_value` |

## Method

The classifier is intentionally dependency-free and interpretable. It expands
each action-label row into per-hour labels, then learns majority actions from
`train_selection` only. The rule key is source model, horizon step, and a
within-vector forecast price rank bin. Ties fall back conservatively toward
`hold`.

The final holdout is used only for scoring. Mutating final-holdout labels changes
final scoring but not the learned rule set, which preserves the no-leakage
split discipline.

## Materialized Metrics

| Scope | Split | Rows | Label hours | Accuracy | Macro F1 |
|---|---|---:|---:|---:|---:|
| all source models | train_selection | 860 | 20,640 | 0.6925 | 0.6102 |
| all source models | final_holdout | 180 | 4,320 | 0.6495 | 0.5364 |
| `tft_silver_v0` | final_holdout | 90 | 2,160 | 0.6685 | 0.5589 |
| `nbeatsx_silver_v0` | final_holdout | 90 | 2,160 | 0.6306 | 0.5141 |

Final-holdout action balance versus predictions:

| Scope | True charge | True discharge | True hold | Pred charge | Pred discharge | Pred hold |
|---|---:|---:|---:|---:|---:|---:|
| all source models | 642 | 772 | 2,906 | 685 | 573 | 3,062 |
| `tft_silver_v0` | 321 | 386 | 1,453 | 330 | 286 | 1,544 |
| `nbeatsx_silver_v0` | 321 | 386 | 1,453 | 355 | 287 | 1,518 |

The model learned 168 majority rules from the 860 train-selection rows
(20,640 labeled hours). The final-holdout metrics are useful as a baseline for
future action-aware learners, but they do not prove decision value because these
predicted action labels have not yet been projected through the frozen strict LP
and oracle regret protocol.

## Claim Boundary

- `strict_similar_day` remains the frozen Level 1 control comparator.
- This baseline is classification evidence only.
- A candidate still needs strict LP/oracle scoring, zero safety violations,
  thesis-grade observed coverage, and the conservative promotion gate before it
  can be considered for promotion.
- The current status remains blocked: `blocked_classification_only_no_strict_lp_value`.

## Next Step

The next safe technical step is an action-to-dispatch strict LP projection: turn
the classifier's predicted charge/discharge/hold labels into feasible dispatch
vectors, score them on the same final holdout with realized prices, and compare
decision regret against `strict_similar_day`.
