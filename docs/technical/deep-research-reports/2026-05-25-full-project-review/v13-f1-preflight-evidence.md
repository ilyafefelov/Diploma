# V13 F1 Preflight Evidence

Date: 2026-05-25

Backlog item: F1 - Refresh V13 preflight.

Command:

```powershell
.\.venv\Scripts\python.exe scripts\preflight_ua_context_v13_acquisition_inputs.py --config configs\real_data_dfl_ua_context_v13_acquisition_week3.yaml --output .tmp_runtime\v13_acquisition_inputs_preflight.json
```

Exit status: `0`

Attached snapshot:

- `assets/v13_acquisition_inputs_preflight_2026-05-25.json`
- SHA256 `6759403AD4D59C8D0EF24FC8A42F5CFE45125D954AF4B92E652EDB212905BE12`

## Result

```json
{
  "claim_boundary": "v13_source_readiness_only_not_market_execution",
  "data_acquisition_needed": true,
  "dt_lava_ready": false,
  "full_v13_gate_evaluated": false,
  "market_execution_enabled": false,
  "missing_required_inputs": [
    "oree_dam_publication_receipts_csv_path",
    "ua_context_safe_switch_examples_csv_path"
  ],
  "permits_model_training": false,
  "v13_candidate_generation_ready": false
}
```

## Blockers

- DAM publication receipts are not configured.
- Required DAM receipt columns remain `timestamp` and `source_publication_timestamp`.
- Safe-switch examples are not configured.
- Required safe-switch columns remain `tenant_id`, `source_model_name`, `anchor_timestamp`, `split_name`, `source_evidence_timestamp`, `label_v13_material_safe_switch`, and `label_v13_tail_risk_loss`.

## Verdict

F1 is complete. The current state is captured and still correctly blocker-oriented:

- `market_execution_enabled=false`
- `dt_lava_ready=false`
- `permits_model_training=false`
- `v13_candidate_generation_ready=false`

Recommended next item: F2 - document the exact acquisition backlog and validator sequence.

