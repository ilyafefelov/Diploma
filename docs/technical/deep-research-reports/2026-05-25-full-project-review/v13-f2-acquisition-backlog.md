# V13 F2 Acquisition Backlog

Date: 2026-05-25

Backlog item: F2 - Document acquisition backlog.

Source evidence:

- `v13-f1-preflight-evidence.md`
- `assets/v13_acquisition_inputs_preflight_2026-05-25.json`
- `data/research_runs/week3_dfl_ua_context_acquisition_v13/dfl_ua_context_v13_acquisition_summary.json`

## Current V13 State

V13 remains blocked before candidate generation:

- `data_acquisition_needed=true`
- `v13_candidate_generation_ready=false`
- `dt_lava_ready=false`
- `permits_model_training=false`
- `market_execution_enabled=false`
- `full_v13_gate_evaluated=false`

Missing configured inputs:

- `oree_dam_publication_receipts_csv_path`
- `ua_context_safe_switch_examples_csv_path`

## Required Input 1 - OREE DAM Publication Receipts

Config field:

- `oree_dam_publication_receipts_csv_path`

Required columns:

- `timestamp`
- `source_publication_timestamp`

Acceptance:

- CSV path is configured in `configs/real_data_dfl_ua_context_v13_acquisition_week3.yaml`.
- Rows are source-backed and row-level, not dataset-level metadata only.
- `source_publication_timestamp` is an actual source publication timestamp, not local observation time or first-seen polling time.
- Validate before materialization:

```powershell
.\.venv\Scripts\python.exe scripts\validate_oree_dam_publication_receipts.py --input <raw.csv> --output <normalized.csv>
```

Current blocker:

- Preflight status is `missing_config_path`.

## Required Input 2 - V13 Safe-Switch Examples

Config field:

- `ua_context_safe_switch_examples_csv_path`

Required columns:

- `tenant_id`
- `source_model_name`
- `anchor_timestamp`
- `split_name`
- `source_evidence_timestamp`
- `label_v13_material_safe_switch`
- `label_v13_tail_risk_loss`

Acceptance:

- CSV path is configured in `configs/real_data_dfl_ua_context_v13_acquisition_week3.yaml`.
- Rows are incremental source-backed `train_selection` rows.
- Each tenant/source reaches at least `20` prior/train non-tail-risk material safe-switch examples.
- Rows used for the threshold must have:
  - `label_v13_material_safe_switch=true`
  - `label_v13_tail_risk_loss=false`
- Validate before materialization:

```powershell
.\.venv\Scripts\python.exe scripts\validate_ua_context_safe_switch_examples_v13.py --input <raw.csv> --output <normalized.csv>
```

Current blocker:

- Preflight status is `missing_config_path`.
- Historical V13 summary had ready rows `0`, blocked rows `5`, max prior material safe-switch examples `7`, required `20`.

## Required Sequence

1. Acquire source-backed OREE DAM publication receipt rows.
2. Validate receipt rows with `validate_oree_dam_publication_receipts.py`.
3. Acquire source-backed safe-switch examples.
4. Validate safe-switch rows with `validate_ua_context_safe_switch_examples_v13.py`.
5. Set both validated CSV paths in `configs/real_data_dfl_ua_context_v13_acquisition_week3.yaml`.
6. Re-run:

```powershell
.\.venv\Scripts\python.exe scripts\preflight_ua_context_v13_acquisition_inputs.py --config configs\real_data_dfl_ua_context_v13_acquisition_week3.yaml --output .tmp_runtime\v13_acquisition_inputs_preflight.json
```

7. Only after preflight confirms configured inputs, consider materializing the V13 assets.

## Boundaries That Must Not Change

Until both source families are ready:

- Do not set `market_execution_enabled=true`.
- Do not set `dt_lava_ready=true`.
- Do not set `permits_model_training=true`.
- Do not emit `ProposedBid` or market order payloads.
- Do not treat V13 as a modeling result.
- Do not treat EU/Poland rows as Ukrainian training targets.

## F2 Verdict

F2 is complete as a documented backlog. Actual V13 readiness remains incomplete because it depends on external/source-backed receipt and safe-switch data.

