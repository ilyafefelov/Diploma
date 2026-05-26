# DT V2+ Apples-to-Apples Shadow

Date: 2026-05-26

This note documents a Decision Transformer research-shadow packet built directly
from the real V2+ strict-row comparison artifact. It was created to remove an
ambiguity from the earlier Direct DT Shadow result: `627.04` UAH was the V13
teacher-packet fallback row, not the thesis headline V2+ comparator.

The run is research-only. It does not promote DT/LAVA, does not change the
dashboard default, does not emit a market-order payload, and keeps
`market_execution_enabled=false`.

## Command

```powershell
.\.venv\Scripts\python.exe scripts\materialize_dt_v2_plus_apples_to_apples_packet.py `
  --strict-rows-csv data\research_runs\week3_official_global_panel_schedule_value_v2_plus_comparison\dfl_schedule_value_learner_v2_plus_strict_rows.csv `
  --regret-decomposition-pickle .tmp_runtime\v2_plus_export_inputs\regret_decomposition.pkl `
  --output-dir data\research_runs\week3_dt_v2_plus_apples_to_apples_current `
  --run-slug week3_dt_v2_plus_apples_to_apples_current `
  --source-model-name nbeatsx_official_global_panel_horizon_calibrated_v1 `
  --context-length 4 `
  --max-epochs 3 `
  --hidden-dim 48 `
  --num-layers 2 `
  --num-heads 2 `
  --seed 20260526 `
  --model-backbone hf
```

## Artifacts

Output directory:

```text
data/research_runs/week3_dt_v2_plus_apples_to_apples_current/
```

Key files:

| File | Purpose |
|---|---|
| `dt_v2_plus_apples_to_apples_teacher_rows.csv` | Adapted candidate-index teacher rows from the real V2+ strict packet |
| `dt_research_shadow_sequence_summary.json` | Sequence dataset summary |
| `dt_research_shadow_smoke_summary.json` | HF DecisionTransformer training summary |
| `dt_research_shadow_evaluation_summary.json` | DT-vs-control evaluation metrics |
| `dt_research_shadow_selected_schedule_preview.json` | Manual dashboard/API preview rows |
| `dt_v2_plus_apples_to_apples_summary.json` | Consolidated apples-to-apples result |
| `dt_v2_plus_apples_to_apples_summary.md` | Human-readable summary |

## Final-Holdout Controls

The candidate set is the real comparison set from
`dfl_schedule_value_learner_v2_plus_strict_rows.csv`:

| Candidate | Mean regret UAH | Median regret UAH |
|---|---:|---:|
| `schedule_value_learner_v2_plus` | `174.77` | `67.30` |
| `schedule_value_learner_v2_reference` | `206.37` | `96.02` |
| `strict_reference` | `310.58` | `198.39` |
| `raw_reference` | `622.25` | `290.22` |

This is the comparator table that should be used when discussing whether a new
model beats V2+. The thesis headline remains V2+.

## DT Result

| Metric | Value |
|---|---:|
| DT selected mean regret | `460.30` UAH |
| Real V2+ mean regret | `174.77` UAH |
| Strict reference mean regret | `310.58` UAH |
| Raw reference mean regret | `622.25` UAH |
| DT minus real V2+ | `+285.53` UAH |
| DT minus strict reference | `+149.72` UAH |
| `accuracy_secondary` | `1.0` |
| `eval_cross_entropy_loss` | `0.5494` |

The selected-preview packet chose:

| Selected family | Count |
|---|---:|
| `schedule_value_learner_v2_reference` | `65` |
| `raw_reference` | `25` |
| `schedule_value_learner_v2_plus` | `0` |
| `strict_reference` | `0` |

This explains the apparent contradiction: the classification-style DT smoke can
look valid by sequence metrics, but its selected candidates have worse realized
decision value than V2+.

## Why The Prototype DT Does Not Beat V2+

From an ML architecture perspective, this is expected:

- The current DT smoke is a candidate-index sequence classifier trained with
  cross-entropy. That objective does not directly minimize LP/oracle regret.
- The packet is final-holdout comparison data, not a full historical DT training
  corpus. Training rows are mirrored backward only to exercise the tensor and
  training path, so this is not an out-of-sample promotion claim.
- V2+ is not a raw forecast. It is a conservative schedule/value selector with
  prior-only fallback, strict LP/oracle scoring, and failure-mode candidate
  families. Beating it requires a better safe-switch signal, not just a larger
  sequence model.
- The diagnostic labels show opportunity exists: best-available mean regret is
  `153.27` UAH and `52 / 90` anchors have a material better-than-V2+ candidate.
  However, those labels are realized diagnostics, not prior-safe deployable
  training permission.
- The correct next model objective is regret-aware candidate selection with
  point-in-time context and explicit abstention/fallback to V2+, not raw hourly
  BUY/SELL/HOLD imitation and not a DT promotion shortcut.

## API And Dashboard

The packet is exposed only as a manual shadow preview:

```text
GET /dashboard/shadow-recommendation-preview?tenant_id=client_003_dnipro_factory&preview_source=dt_v2_plus_apples_to_apples_shadow
```

Expected boundary fields:

- `preview_source_id=dt_v2_plus_apples_to_apples_shadow`
- `preview_source_label=DT vs real V2+ Shadow`
- `preview_status=apples_to_apples_not_promoted`
- `market_execution_enabled=false`
- `promotion_gate_passed=false`
- `dt_lava_ready=false`

The default `/dashboard/operator-recommendation` endpoint remains V2+.
