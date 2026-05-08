# DFL Action Classifier Baseline

Date: 2026-05-08

This note records the first supervised action-label baseline over the Ukrainian
DAM DFL v2 action-label panel and the follow-up strict LP projection check. It
is a transparent probe for future DFL work, not full DFL, not Decision
Transformer control, not promotion evidence, and not market execution.

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

## Strict LP Projection Evidence

The follow-up asset `dfl_action_classifier_strict_lp_benchmark_frame` converts
the classifier's predicted charge/discharge/hold labels into feasible dispatch
vectors under the same Level 1 SOC and power constraints, then scores those
dispatch vectors on realized final-holdout prices. The classifier is still fit
only on `train_selection`; final-holdout prices are used for scoring only.

| Field | Value |
|---|---|
| Asset | `dfl_action_classifier_strict_lp_benchmark_frame` |
| Dagster run id | `97cac49e-b3f8-4829-b687-b4b5f3470d07` |
| Strategy kind | `dfl_action_classifier_strict_lp_projection` |
| Projection method | `action_mask_lp_projection` |
| Rows | 360 |
| Tenants | 5 canonical Ukrainian tenants |
| Final holdout | 18 anchors per tenant/model |
| Anchor range | `2026-04-12 23:00` to `2026-04-29 23:00` |
| Claim flags | `not_full_dfl=true`, `not_market_execution=true` |
| Leakage check | `uses_final_holdout_for_training=false` for all candidate rows |

Strict LP/oracle regret comparison:

| Candidate | Rows | Mean value UAH | Mean regret UAH | Median regret UAH |
|---|---:|---:|---:|---:|
| `strict_similar_day` | 180 | 3,532.52 | 314.81 | 202.61 |
| `dfl_action_classifier_v0_tft_silver_v0` | 90 | 2,689.92 | 1,157.40 | 715.66 |
| `dfl_action_classifier_v0_nbeatsx_silver_v0` | 90 | 2,660.50 | 1,186.83 | 1,054.08 |

Result: the action classifier is decisively blocked by the strict LP/oracle
promotion gate. It produces feasible dispatch vectors and preserves the
no-leakage split, but the projected decisions lose badly to the frozen
`strict_similar_day` control. This is evidence that label accuracy alone is not
enough; the next DFL step needs richer in-domain data or value-aware action
learning, not promotion of this baseline.

## Claim Boundary

- `strict_similar_day` remains the frozen Level 1 control comparator.
- The classifier baseline is now classification plus strict projection evidence.
- The projected candidates fail the conservative strict LP/oracle promotion gate.
- The current status remains blocked: feasible research evidence, but not a
  promoted controller.

## Next Step

The next safe technical step is not another small classifier tweak. Expand the
in-domain Ukrainian data/action-label coverage or add a value-aware supervised
baseline that optimizes regret-aware action quality directly, then retest it
through the same strict LP/oracle gate.
