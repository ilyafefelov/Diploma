# Direct DT Candidate Shadow

Date: 2026-05-26

This note documents the direct Decision Transformer research-shadow run trained
on V2+/strict/oracle teacher rows with candidate-index and schedule-family
targets. It answers a narrow question: can the repo train and render a DT
without waiting for the V13 DT/LAVA promotion gate?

Answer: yes, as a research-shadow model. It is not V13-permitted/promoted
training, not LAVA promotion, not a deployed controller, and not market
execution.

## Command

```powershell
.\.venv\Scripts\python.exe scripts\materialize_dt_research_shadow_packet.py `
  --teacher-rows-csv data\research_runs\week3_v13_dt_lava_teacher_dataset_safe_switch_only\dfl_v13_dt_lava_teacher_rows.csv `
  --output-dir data\research_runs\week3_dt_direct_candidate_shadow_current `
  --run-slug week3_dt_direct_candidate_shadow_current `
  --context-length 4 `
  --max-epochs 3 `
  --hidden-dim 48 `
  --num-layers 2 `
  --num-heads 2 `
  --seed 20260526 `
  --model-backbone hf
```

The run deliberately passed no `--candidate-library-csv` arguments, so no
adapter rows were added. The source rows came from the existing V13-gated
teacher CSV, but every output remains non-promotable while the V13 receipt gate
is blocked.

## Artifacts

Output directory:

```text
data/research_runs/week3_dt_direct_candidate_shadow_current/
```

Key files:

| File | SHA256 | Purpose |
|---|---|---|
| `dt_research_shadow_sequence_summary.json` | `D78BAEA75FB08A10219B8D6A4B5BD52E165828F3980AB196F573691F7419C48F` | Sequence dataset summary |
| `dt_research_shadow_smoke_summary.json` | `EC84BF5120908E10E7CB18C21ADEC744FBF5502E5A61E7ED9FA514DE28B8DE1A` | HF DecisionTransformer training/evaluation summary |
| `dt_research_shadow_selected_schedule_preview.json` | `F1DC6748978915E2D4BC5FEDB14D9972E37D058F2833EC210EBFA108E79D9360` | Dashboard/API selected-schedule preview rows |

## Result

| Metric | Value |
|---|---:|
| `model_backbone` | `huggingface_decision_transformer_model` |
| Teacher rows available | `3,921` |
| Research-shadow training rows | `3,741` |
| Adapted candidate-library rows | `0` |
| Train sequences | `1,735` |
| Evaluation sequences | `90` |
| Context length | `4` |
| Forecast context coverage | `partial_missing_tft` |
| DT selected mean regret | `627.04` UAH |
| V2+ mean regret in this packet | `627.04` UAH |
| Strict/oracle mean regret | `310.58` UAH |
| DT minus V2+ mean regret | `0.00` UAH |
| DT minus strict mean regret | `316.46` UAH |

Interpretation:

- The direct DT model trained and produced a valid sequence/evaluation packet.
- It selected the conservative frozen V2+ fallback candidate in the Dnipro
  operator preview, so it ties V2+ on this direct packet.
- It remains materially worse than the strict/oracle reference and therefore is
  not a thesis headline replacement.
- `promotable_v13_permitted_training_rows=0`,
  `dt_promotion_gate_passed=false`, and `market_execution_enabled=false`.

## API And Dashboard

The run is exposed as a manual operator-dashboard preview source:

```text
GET /dashboard/shadow-recommendation-preview?tenant_id=client_003_dnipro_factory&preview_source=dt_direct_candidate_shadow
```

The response uses:

- `preview_source_id=dt_direct_candidate_shadow`
- `preview_source_label=Direct DT Shadow`
- `preview_status=direct_candidate_shadow_not_promoted`
- `market_execution_enabled=false`
- `promotion_gate_passed=false`
- `dt_lava_ready=false`
- `source_readiness_gate_passed=false`

The default `/dashboard/operator-recommendation` strategy is unchanged. V2+
remains the default/fallback; Direct DT Shadow is only a manually selected
diagnostic preview.

## Boundary

This run is allowed because it is research-shadow training over existing
candidate-index/schedule-family teacher targets. It does not unlock V13,
does not satisfy source-readiness, does not create `ProposedBid`, does not emit
market order payloads, and does not justify a deployed Decision Transformer
controller claim.
