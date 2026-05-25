# DFL LAVA Schedule-Neighbor Bridge

This slice starts the DT/LAVA branch without jumping straight to a raw
hourly-action Decision Transformer. It builds a teacher-label and feasible
schedule-neighbor layer anchored to the current frozen Ukrainian-only V2+
baseline.

The frozen comparator remains:

| Metric | Frozen Ukrainian-only calibrated V2+ |
|---|---:|
| Mean regret | `174.77` UAH |
| Median regret | `67.30` UAH |
| Rolling robustness | `4 / 4` windows |
| Market execution | `false` |

Poland remains positive but not promoted. Its token-backed feature lane works,
and the latest holdout showed a useful TFT/Poland signal, but rolling evidence
and the simple Poland ranker were not safe enough to replace V2+.

## Why This Layer Exists

The previous Poland candidate-value ranker selected risky schedules too
aggressively:

| Row | Mean regret, UAH | Median regret, UAH | Interpretation |
|---|---:|---:|---|
| Frozen Ukrainian-only V2+ | `174.77` | `67.30` | current thesis comparator |
| Poland ranker, calibrated NBEATSx | `334.02` | `223.15` | negative evidence |
| Poland ranker, calibrated TFT | `445.75` | `358.19` | negative evidence |

That result says the next model should not imitate the same objective. The next
step is to create explicit teacher labels:

- `v2_plus_best`;
- `poland_safe_win`;
- `poland_tail_risk_loss`;
- `selector_overreach`;
- `oracle_only_train_diagnostic`.

These labels separate useful Poland/TFT opportunities from tail-risk rows and
from rows where the selector overreached. They are evidence for a future
schedule-neighbor or DT model, not a market-execution signal.

## Asset Interface

The implementation is additive and keeps existing V2+/Poland assets unchanged.

| Asset | Purpose |
|---|---|
| `dfl_v2_plus_schedule_neighbor_teacher_label_frame` | Classifies current V2+, Poland V2+, Poland veto, and Poland ranker evidence into teacher-label rows. |
| `dfl_lava_schedule_neighbor_candidate_frame` | Builds feasible schedule-neighbor candidates from V2+, strict fallback, Poland/TFT near-miss schedules, and train-only oracle-neighborhood diagnostics. |
| `dfl_lava_candidate_value_scorer_frame` | Trains a conservative prior-only candidate-level delta scorer and falls back to V2+ when prior evidence is weak. |
| `dfl_lava_candidate_value_strict_lp_benchmark_frame` | Strict-scores `strict_similar_day`, frozen V2+, behavior cloning, and the LAVA scorer under the unchanged LP/oracle evaluator. |
| `dfl_lava_tail_risk_diagnostic_frame` | Uses the failed bridge rows to identify perturbation families that create tail-risk regret. |
| `dfl_lava_tail_risk_aware_target_frame` | Converts the diagnostic into schedule-candidate-index targets and blocks risky families before any DT/LAVA training. |
| `dfl_lava_tail_risk_aware_strict_lp_benchmark_frame` | Strict-scores the redesigned target against frozen V2+ without raw hourly action imitation. |
| `dfl_lava_tail_risk_safe_switch_scorer_frame` | Trains a conservative prior-profile safe-switch scorer over approved challenger sources with a family-level tail-risk veto. |
| `dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame` | Strict-scores the safe-switch scorer and uses exact frozen V2+ rows for per-anchor fallback. |

Tracked config:

- [configs/real_data_dfl_lava_schedule_neighbor_week3.yaml](../../configs/real_data_dfl_lava_schedule_neighbor_week3.yaml)
- [configs/real_data_dfl_lava_tail_risk_target_week3.yaml](../../configs/real_data_dfl_lava_tail_risk_target_week3.yaml)

Core implementation:

- `src/smart_arbitrage/dfl/lava_schedule_neighbor_bridge.py`
- `src/smart_arbitrage/dfl/lava_tail_risk_target.py`

## No-Leakage Rules

- Final-holdout actuals can change labels and strict scores, but not prior
  selector features or selected weights.
- Oracle-neighborhood rows are train-only diagnostics and are never eligible
  for final-holdout selection.
- Poland data remains exogenous feature context only; no European rows become
  Ukrainian target rows.
- `market_execution_enabled=false` is fixed in benchmark rows.

## Tiny NPZ Smoke Contract

Future solver-free LAVA-style smoke runs may use a small deterministic NPZ
sourced from the existing schedule-neighbor candidate frame, but the NPZ is
only a research artifact contract. It does not permit candidate promotion,
DT/LAVA training, dashboard defaults, or market execution.

Before citing or running the smoke, write the readiness packet that separates
the three relevant gates:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_dt_lava_prototype_readiness_packet.py `
  --v13-acquisition-summary-json data\research_runs\week3_dfl_ua_context_acquisition_v13_safe_switch_only\dfl_ua_context_v13_acquisition_summary.json `
  --offline-strategy-promotion-registry-json data\research_runs\week3_official_global_panel_365_strategy_promotion\dfl_schedule_value_production_gate_registry.json `
  --lava-npz-smoke-validation-json <lava_npz_margin_smoke_packet_validation.json> `
  --output-dir .tmp_runtime\dt_lava_prototype_readiness
```

If a real `dfl_lava_schedule_neighbor_candidate_frame.pkl` exists, pass it with
`--candidate-frame-pickle`. If a materialization attempt failed because an
upstream Dagster asset was absent, add one `--materialization-blocker <asset>`
argument per missing input. The optional
`--offline-strategy-promotion-registry-json` attachment records the already
passed schedule/value offline strategy gate separately from the DT/LAVA gate.
The `--lava-npz-smoke-validation-json` attachment is required for a passed
prototype CI-smoke gate; omitting it writes a blocker packet with
`lava_npz_smoke_validation_missing`.
The packet writes
`dt_lava_prototype_readiness_summary.json` and
`dt_lava_prototype_readiness_summary.md`. It can report CI-smoke readiness
and upstream offline strategy promotion separately from V13 training
permission. It also emits a `gate_passport` that can honestly mark the
DT/LAVA prototype CI-smoke gate, upstream offline strategy promotion gate, and
no-market-execution safety gate as passed while leaving V13 training permission,
DT/LAVA training promotion, and market execution blocked. The packet always
keeps `market_execution_enabled=false`, `promotion_gate_passed=false`, and
`market_execution_gate_passed=false` until a real DT/LAVA benchmark/promotion
gate and a separate execution contract are implemented and passed.

## Credentialless Academic MVP Packet

SCMO credentials are not required for the diploma MVP. Missing SCMO
username/password/cert/P12 material blocks only market-submission-grade DAM
receipt readiness, not the credentialless operator-preview demo or the
research-only DT/LAVA prototype evidence. Keep the V13
`explicit_dam_publication_receipts` gate blocked for market-submission/source
readiness claims; attach the public OREE/SCMO negative evidence and
credential-gated status as source-governance evidence instead of synthesizing
receipts.

After the operator preview, V13 packet, LAVA NPZ readiness packet, V13-gated
teacher packet, and offline challenger packet exist, first materialize the
credentialless DT research-shadow packet. `source_publication_timestamp is not required for offline research-shadow DT prototype`; it is required only for
market-submission-grade receipt readiness and promotable V13 source-ready DT
training. This DT packet still records `publication_receipt_verified=false`,
`source_publication_timestamp_available=false`, `market_availability_claim=false`,
`research_shadow_not_promotable=true`, and `market_execution_enabled=false`.

```powershell
.\.venv\Scripts\python.exe scripts\materialize_dt_research_shadow_packet.py `
  --teacher-rows-csv data\research_runs\week3_v13_dt_lava_teacher_dataset_safe_switch_only\dfl_v13_dt_lava_teacher_rows.csv `
  --candidate-library-csv data\research_runs\week3_tft_quantile_365_full_negative_evidence\tft_candidate_library_rows.csv `
  --output-dir data\research_runs\week3_dt_research_shadow_current `
  --run-slug week3_dt_research_shadow_current `
  --context-length 3 `
  --max-sequences 7300 `
  --max-epochs 1 `
  --model-backbone auto
```

Then write the thesis-facing credentialless MVP packet:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_credentialless_academic_mvp_readiness_packet.py `
  --tenant-id client_004_kharkiv_hospital `
  --v13-acquisition-summary-json data\research_runs\week3_dfl_ua_context_acquisition_v13_safe_switch_only\dfl_ua_context_v13_acquisition_summary.json `
  --dt-lava-prototype-readiness-json data\research_runs\week3_dt_lava_prototype_readiness_current\dt_lava_prototype_readiness_summary.json `
  --teacher-summary-json data\research_runs\week3_v13_dt_lava_teacher_dataset_safe_switch_only\dfl_v13_dt_lava_teacher_summary.json `
  --teacher-validation-json data\research_runs\week3_v13_dt_lava_teacher_dataset_safe_switch_only\dfl_v13_dt_lava_teacher_validation.json `
  --offline-challenger-summary-json data\research_runs\week3_v13_dt_lava_offline_challenger_gate_safe_switch_only\dfl_v13_dt_lava_offline_challenger_summary.json `
  --offline-challenger-validation-json data\research_runs\week3_v13_dt_lava_offline_challenger_gate_safe_switch_only\dfl_v13_dt_lava_offline_challenger_validation.json `
  --dt-research-shadow-sequence-summary-json data\research_runs\week3_dt_research_shadow_current\dt_research_shadow_sequence_summary.json `
  --dt-research-shadow-smoke-summary-json data\research_runs\week3_dt_research_shadow_current\dt_research_shadow_smoke_summary.json `
  --output-dir data\research_runs\week3_credentialless_academic_mvp_current
```

Then validate the packet as a standalone thesis/demo artifact:

```powershell
.\.venv\Scripts\python.exe scripts\validate_credentialless_academic_mvp_readiness_packet.py `
  --input data\research_runs\week3_credentialless_academic_mvp_current\credentialless_academic_mvp_readiness_summary.json `
  --output data\research_runs\week3_credentialless_academic_mvp_current\credentialless_academic_mvp_readiness_validation.json
```

The materializer also writes the sibling
`credentialless_academic_mvp_readiness_validation.json` artifact required by
the API/dashboard endpoint. The standalone validator command above remains the
explicit revalidation path when the summary was copied, edited, or produced by
an older run.
The MVP validator re-checks the DT/LAVA readiness packet's embedded
`lava_npz_smoke_validation` fields and emits a separate
`lava_npz_smoke_packet_validation` validation gate plus
`gate_passport.lava_npz_smoke_packet_validation_gate`, so a shape-only or
tampered NPZ smoke summary cannot pass the credentialless MVP packet or the
API/dashboard surface.
The `--teacher-validation-json` input is the Phase 2
`dfl_v13_dt_lava_teacher_validation.json` artifact; the MVP packet refuses to
pass the V13-gated teacher-contract gate if that validation is missing, failed,
or enables market execution.
The `--offline-challenger-validation-json` input is the Phase 3
`dfl_v13_dt_lava_offline_challenger_validation.json` artifact; the MVP packet
refuses to pass the offline challenger non-promotion gate if strict controls,
deterministic safety projection, non-promotion/execution boundaries, or
no-market-execution validation fail.

Use `--operator-preview-json <operator-preview.json>` instead of `--tenant-id`
when the operator preview payload has already been exported. The packet passes
only the academic MVP gates: DAM delivery-day recommendation preview,
LAVA NPZ CI-smoke validation, V13-gated teacher-contract shape with `0`
permitted training rows while receipts are blocked, offline challenger
non-promotion evidence, and no-market-execution safety. It still reports
`market_submission_ready=false`, `promotion_gate_passed=false`,
`permits_model_training=false`, and `market_execution_enabled=false`.
The summary now also includes `gate_passport`, which is the compact
defense-facing map of passed and blocked gates: operator preview,
non-submittable DAM bid preview, LAVA NPZ CI smoke, LAVA NPZ packet validation,
V13-gated teacher contract, offline challenger non-promotion, and
no-market-execution safety pass for the credentialless MVP; market-submission
receipts stay `blocked_external_access`, DT/LAVA training promotion stays
`blocked_until_v13_source_readiness`, and market execution remains
`out_of_scope`.
The operator-preview section also carries `bid_preview_summary` with BUY/SELL
row counts, total preview MWh, indicative notional value, and fixed
`market_execution_enabled=false` / `proposed_bid_emitted=false` flags so the
thesis packet proves a DAM delivery-day schedule recommendation exists without
turning it into a market-order payload.
The offline challenger section carries `control_comparison_summary` from the
Phase 3 packet: strict-reference, frozen V2+, and filtered behavior-cloning
controls must be present; validation tenant-anchor coverage is exposed; and
per-source regret summaries show why DT/LAVA remains non-promoted. This is the
credentialless DT/LAVA prototype evidence surface, not training permission and
not deployed control.
The packet also emits `prototype_contract`, which is the machine-readable
academic proof of the DFL/DT prototype boundary: DFL inputs are calibrated
forecast context plus tenant/SOC/context and feasible candidate schedules, DFL
targets are schedule value / regret value against V2+, DT input is the
V13-gated sequence contract, the DT action target is candidate id or schedule
family, and evaluation is strict LP/oracle regret/value against V2+ with
strict-reference and behavior-cloning controls. The paired
`dfl_dt_prototype_contract_gate` may pass for the credentialless MVP while
market-submission receipts, DT/LAVA promotion, and market execution remain
blocked.
It also emits `prototype_evidence_scorecard`, a compact dashboard/thesis object
derived from the operator preview, LAVA validation, teacher packet, and offline
challenger packet. The scorecard is not new evidence; it summarizes the already
validated evidence as bid-preview rows, LAVA NPZ validation, teacher row counts,
`0` permitted model-training rows, strict/V2+/behavior-cloning controls,
deterministic safety projection, and fixed non-execution flags.
The same summary is mirrored into
`gate_passport.prototype_evidence_scorecard_gate`, which must pass before the
API/dashboard treat the credentialless prototype packet as demo-ready.
The packet also emits `prototype_phase_readiness`, a compact Phase 0-4 matrix
for defense/demo use: Phase 0 V13 source readiness stays
`blocked_market_submission_receipts`, Phase 1 LAVA NPZ smoke is
`passed_ci_smoke_not_promotion`, Phase 2 teacher contract is
`passed_contract_training_rows_gated`, Phase 3 offline challenger is
`passed_non_promotion_evidence`, and Phase 4 full schedule-level DFL remains
`future_work_not_started`. This matrix is also checked by the sibling
validation artifact and keeps `market_execution_enabled=false`.
The DT research-shadow path now materializes a separate chronological sequence
dataset and local transformer smoke from the existing candidate/value teacher
rows. This is the fast DT prototype path: `research_shadow_training_rows` may be
positive for academic evaluation, while
`promotable_v13_permitted_training_rows=0`,
`publication_receipt_verified=false`, and `market_availability_claim=false`
keep V13 promotion and market submission blocked. Its smoke packet reports
regret and value metrics for DT, strict LP/oracle, V2+
teacher/comparator/fallback, and behavior-cloning controls; imitation accuracy
is retained only as secondary evidence. The sequence packet also reports
forecast-family coverage for the NBEATSx/TFT state contract. The current V13
safe-switch teacher artifact is NBEATSx-only, but the credentialless DT packet
can now adapt TFT candidate-library rows as research-shadow context with
`--candidate-library-csv`. The refreshed packet reports
`forecast_context_coverage_status=complete_nbeatsx_tft`, while every adapted
row remains non-promotable with `publication_receipt_verified=false` and
`market_execution_enabled=false`.

## Operator Shadow Preview Switch

The operator dashboard keeps `schedule_value_learner_v2_plus` / best valid
gate-passed recommendation as the default preview source. Shadow sources are
manual diagnostics only:

- `DT Shadow`: research-shadow, not promoted, preview only, no market execution.
- `Poland-TFT Shadow`: `positive_not_promoted`, useful challenger evidence but
  not robust enough for the default strategy.
- `DFL diagnostics`: diagnostic only, used to explain candidate-value evidence.
- `V13/DT/LAVA promoted training`: blocked roadmap evidence until source
  readiness and receipt gates pass.

The dashboard switch expands a selected shadow candidate into the same hourly
recommendation read model used by the default charts and final schedule table.
Rows carry timestamp, charge/discharge/hold, quantity, SOC path when available,
candidate id or schedule family, expected value, regret/value versus V2+ and
strict reference, and gate/safety status. This intentionally renders negative
DT evidence too: if DT is worse than V2+, the chart/table still display the DT
schedule and labels it as not promoted.

Hourly auto-refresh is intentionally not enabled for this demo slice. The
backend artifacts are materialized batch evidence, not an hourly live market
feed, so a timer would mostly reload the same files while making the dashboard
look more real-time than the source pipeline supports. The UI instead exposes
`last loaded` plus a manual refresh control for the currently selected preview
source. That refresh is read-model only and cannot change the default strategy,
emit `ProposedBid`, create market order payloads, unblock V13 receipts, promote
DT/LAVA, or set `market_execution_enabled=true`.

The transformer smoke records the backbone decision explicitly. Install the
research DT extra before the HF-backed defense run:

```powershell
uv sync --extra dev --extra dt
```

Then use `--model-backbone hf` when the artifact must prove Hugging Face
`DecisionTransformerModel` import and forward/train/eval execution. The packet
must write `model_backbone=huggingface_decision_transformer_model`,
`model_backbone_selection_reason=hf_requested`, and
`hf_decision_transformer_available=true`. Use `--model-backbone auto` for a
portable smoke path: it selects Hugging Face only when `transformers` is
importable; otherwise it uses the local DT-compatible classifier and writes
`model_backbone_selection_reason=transformers_not_installed`. Both paths remain
non-promotable and non-executable evidence, not DT promotion.
The sequence packet also records the DT substrate as contracts:
`dt_state_feature_contract` proves that forecast, battery/SOC, tenant,
candidate value/regret, and gate context groups are present; and
`dt_reward_target_contract` fixes the return-to-go target as negative regret
delta versus V2+/strict reference while preserving schedule-value metrics. Both
contracts keep `market_execution_enabled=false`.
The smoke run now writes sidecars
`dt_research_shadow_evaluation_summary.json` and
`dt_research_shadow_evaluation_validation.json`. Use them as the formal
evaluation packet for thesis/API evidence: they compare the DT shadow
challenger against strict LP/oracle, V2+ teacher/comparator/fallback, and
behavior-cloning controls with regret/value deltas before imitation accuracy.
The validation sidecar fails closed on missing metrics, missing controls,
promotion, or market execution.
Credentialless academic MVP materialization loads this validation sidecar. When
the smoke summary path is configured, the materializer infers the sibling
`dt_research_shadow_evaluation_validation.json` unless an explicit validation
path is supplied, and the top-level MVP gate depends on that DT shadow
validation passing.
The validator checks the same boundary without requiring FastAPI: academic MVP
and DFL/DT prototype gates pass, market-submission receipt/training/execution
gates are explicitly non-required and unpassed, and every nested
`market_execution_enabled` flag remains false.

For normal verification, write the NPZ, summary, margin metrics, and manifest
as one packet from a persisted candidate-frame pickle:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_lava_npz_margin_smoke_packet.py `
  --candidate-frame-pickle <dfl_lava_schedule_neighbor_candidate_frame.pkl> `
  --output-dir .tmp_runtime\lava_npz_smoke `
  --v13-acquisition-summary-json data\research_runs\week3_dfl_ua_context_acquisition_v13_safe_switch_only\dfl_ua_context_v13_acquisition_summary.json `
  --seed 0 `
  --window-id lava_npz_smoke_window `
  --max-instances 8 `
  --max-neighbors 4
```

The packet writes `candidate_lava_smoke.npz`,
`candidate_lava_smoke_summary.json`, `candidate_lava_margin_metrics.json`,
`dt_lava_research_metrics_aggregate.json`, and
`lava_npz_margin_smoke_manifest.json`, then validates the packet and writes
`lava_npz_margin_smoke_packet_validation.json`. The manifest records the
aggregate and validation paths. When `--v13-acquisition-summary-json` is
provided, it also records the attached
`dfl_ua_context_v13_acquisition_summary.json` path, a compact V13 readiness
summary, and a SHA256 hash for that source-readiness packet. This keeps the
solver-free smoke tied to the current V13 blocker state instead of relying on a
manually typed gate label. The manifest repeats the boundary flags:
`v13_candidate_generation_ready=false`, `dt_lava_ready=false`,
`permits_model_training=false`, `raw_hourly_action_imitation=false`,
`ci_smoke_only=true`, `promotion_gate=false`, and
`market_execution_enabled=false`. The manifest also records SHA256 hashes for
the candidate-frame pickle, NPZ, summary JSON, metrics JSON, and aggregate JSON
so a later thesis packet can prove which exact research artifacts were
evaluated. Do not pass a ready V13 gate status to this packet unless the NPZ
contract itself reports
`v13_candidate_generation_ready=true`; the current smoke rejects that mismatch.
The NPZ contract now validates identity vectors as well as numeric arrays:
`tenant_id_vector`, `source_model_name_vector`, `anchor_timestamp_vector`, and
`selected_candidate_model_name_vector` must all match the feature row count.
The packet metrics also include `baseline_comparison`, computed from the same
source candidate frame, comparing selected smoke candidates against
`strict_control` and `frozen_v2_plus_fallback` rows. The validator now requires
`baseline_comparison_ready=true`: every selected NPZ smoke instance must have
both strict-control and frozen V2+ fallback coverage, with zero missing fallback
anchors. This is diagnostic evidence only: `promotion_gate=false`,
`permits_model_training=false`, and `market_execution_enabled=false` remain
mandatory even when the smoke candidate beats strict or V2+ on the tiny packet.

Revalidate a completed packet before citing it in a thesis evidence bundle or
after moving artifacts between directories:

```powershell
.\.venv\Scripts\python.exe scripts\validate_lava_npz_margin_smoke_packet.py `
  --manifest .tmp_runtime\lava_npz_smoke\lava_npz_margin_smoke_manifest.json `
  --output .tmp_runtime\lava_npz_smoke\lava_npz_margin_smoke_packet_validation.json
```

The packet validator, also run by the packet materializer, recomputes SHA256
hashes, revalidates the NPZ contract, revalidates DT/LAVA metrics, recomputes
the one-metric aggregate, verifies complete strict/V2+ fallback baseline
coverage, and writes a validation summary with attached V13 blocker counts such
as `ready_rows`, `readiness_rows`,
`max_prior_material_safe_switch_examples`, and
`min_safe_examples_required`, plus
`promotion_gate=false`,
`permits_model_training=false`, and `market_execution_enabled=false`. A hash
mismatch is a hard failure, not a warning. A contradictory V13 readiness claim
between the manifest, NPZ contract, or attached V13 summary is also a hard
failure. The validator also checks that the manifest baseline comparison exactly
matches the metrics JSON and does not claim promotion, training permission, or
execution. It emits `baseline_comparison_valid`,
`baseline_comparison_ready`, `baseline_selected_instance_count`,
`strict_fallback_anchor_count`, and `v2_plus_anchor_count` in
`lava_npz_margin_smoke_packet_validation.json`. Manifest summary counters such
as `npz_instance_count`,
`npz_valid_neighbor_count`, `lava_adjacent_pair_count`, and
`aggregate_metric_count` are cross-checked against the validated artifacts, so
the manifest cannot overstate the size of the research smoke evidence.

`.\scripts\verify.ps1` can run the same packet as an optional verification hook
when a real candidate-frame pickle is available:

```powershell
$env:SMART_ARBITRAGE_VERIFY_LAVA_NPZ_CANDIDATE_FRAME_PICKLE = "<dfl_lava_schedule_neighbor_candidate_frame.pkl>"
.\scripts\verify.ps1
```

If that environment variable is unset, the wrapper skips the LAVA NPZ margin
smoke with a status message. If it is set, the wrapper writes the packet under
`.tmp_runtime\verify_lava_npz_margin_smoke`, attaches
`data\research_runs\week3_dfl_ua_context_acquisition_v13_safe_switch_only\dfl_ua_context_v13_acquisition_summary.json`
when that file exists, attaches the passed schedule/value promotion registry to
the DT/LAVA readiness packet when available, and re-runs
`scripts\validate_lava_npz_margin_smoke_packet.py`. A failure in that optional
packet fails verification, but a passing packet is still CI smoke evidence only:
`promotion_gate=false`, `permits_model_training=false`, and
`market_execution_enabled=false`. When the validation JSON exists, the readiness
packet attaches it through `--lava-npz-smoke-validation-json` and exposes a
separate `lava_npz_smoke_packet_validation_gate` in the gate passport.

For the Phase 2 candidate-index / schedule-family teacher handoff, build a
V13-gated teacher contract from the same candidate-frame pickle and the current
V13 readiness CSV:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_v13_dt_lava_teacher_contract_from_candidate_frame.py `
  --candidate-frame-pickle .tmp_runtime\dt_lava_prototype\dfl_lava_schedule_neighbor_candidate_frame.pkl `
  --readiness-csv .tmp_runtime\v13\dfl_ua_context_v13_readiness_rows_safe_switch_only.csv `
  --output-pickle .tmp_runtime\dt_lava_teacher\dfl_v13_gated_dt_lava_teacher_contract_frame_safe_switch_only.pkl `
  --summary-json .tmp_runtime\dt_lava_teacher\dfl_v13_gated_dt_lava_teacher_contract_summary_safe_switch_only.json
```

Then export the thesis-facing packet:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_v13_dt_lava_teacher_packet.py `
  --teacher-contract-pickle .tmp_runtime\dt_lava_teacher\dfl_v13_gated_dt_lava_teacher_contract_frame_safe_switch_only.pkl `
  --output-root data\research_runs `
  --run-slug week3_v13_dt_lava_teacher_dataset_safe_switch_only `
  --materialization-command "local materialize_v13_dt_lava_teacher_contract_from_candidate_frame.py from V13-tracked LAVA candidate rows plus safe-switch-only V13 readiness; DAM receipts still missing" `
  --asset-check-status blocked_v13_explicit_dam_publication_receipts
```

The current safe-switch-only export filters 70,945 candidate rows to 3,921
V13-tracked rows and contains 3,741 `train_selection` rows, but has 0 permitted
model-training rows because explicit DAM publication receipts still block the
V13 training permission gate. It now reports the safe-switch floor separately:
`safe_switch_covered_tenant_source_count=5`,
`safe_switch_required_tenant_source_count=5`,
`safe_switch_min_observed_prior_material_examples=20`, and
`safe_switch_coverage_gate_passed=true`. This means the safe-switch count
precondition is closed, while explicit DAM publication receipts still block
training permission. The packet passes only the teacher dataset and safe-switch
coverage contract gates:
`dt_action_target_contract=candidate_id_or_schedule_family`,
`v2_plus_role=teacher_comparator_fallback`,
`dt_lava_training_dataset_ready=false`,
`promotion_gate_passed=false`, and `market_execution_enabled=false`.
It also writes `dfl_v13_dt_lava_teacher_validation.json`, which revalidates
the candidate-id / schedule-family teacher contract, training-permission
consistency, blocked promotion/execution status, and no-market-execution
boundary for the packet itself.
The teacher packet now writes the architecture recommendation directly into
`feature_contract.architecture_recommendation`: DFL consumes calibrated
NBEATSx/TFT forecasts plus tenant/SOC/context and feasible candidate schedules;
DFL targets best candidate / schedule value / regret delta versus V2+; DT
consumes V13-passing teacher sequences with forecast, battery, tenant,
candidate/value, return-to-go, and regret fields; DT predicts candidate id or
schedule family; V2+ remains teacher, comparator, and fallback.

The lower-level artifact step remains useful when debugging a single NPZ:

## Phase 3 Offline Challenger Gate Packet

After a V13 teacher packet exists and a V2+-anchored bridge strict frame exists,
export the Phase 3 offline challenger gate packet:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_v13_dt_lava_offline_challenger_packet.py `
  --teacher-summary-json data\research_runs\week3_v13_dt_lava_teacher_dataset_safe_switch_only\dfl_v13_dt_lava_teacher_summary.json `
  --bridge-frame-pickle .tmp_runtime\v2_plus_bridge_export\dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame.pkl `
  --output-root data\research_runs `
  --run-slug week3_v13_dt_lava_offline_challenger_gate_safe_switch_only `
  --asset-check-status blocked_v13_explicit_dam_publication_receipts `
  --infer-deterministic-safety-projection-from-zero-violations
```

The `--infer-deterministic-safety-projection-from-zero-violations` switch is
only for legacy bridge frames that predate the explicit
`deterministic_safety_projection_passed` column. It overlays the same
deterministic rule used by current bridge rows:
`safety_violation_count == 0`.

The 2026-05-25 packet at
`data/research_runs/week3_v13_dt_lava_offline_challenger_gate_safe_switch_only/`
is correctly blocked and writes
`dfl_v13_dt_lava_offline_challenger_validation.json`:

- `safe_switch_coverage_gate_passed=true`;
- `deterministic_safety_projection_passed=true` for `1080` bridge rows;
- `v13_training_permission_gate_passed=false`;
- `teacher_permitted_model_training_rows=0`;
- `bridge_gate_passed=false`;
- `offline_dt_lava_challenger_gate_passed=false`;
- `market_execution_enabled=false`.

This is progress toward Phase 3 infrastructure, not a DT/LAVA promotion. It
turns the offline challenger gate into a repeatable artifact and keeps the
current blockers explicit: missing explicit DAM receipts and no residual/DT
challenger beating V2+ under strict LP/oracle evidence.

```powershell
.\.venv\Scripts\python.exe scripts\materialize_lava_npz_smoke_artifact.py `
  --candidate-frame-pickle <dfl_lava_schedule_neighbor_candidate_frame.pkl> `
  --output-npz .tmp_runtime\lava_npz_smoke\candidate_lava_smoke.npz `
  --summary-json .tmp_runtime\lava_npz_smoke\candidate_lava_smoke_summary.json `
  --max-instances 8 `
  --max-neighbors 4
```

Validate any such artifact before use:

```powershell
.\.venv\Scripts\python.exe scripts\validate_lava_npz_smoke_contract.py `
  --input <candidate-lava-smoke.npz> `
  --output .tmp_runtime\lava_npz_smoke_summary.json
```

Run the CI-fast adjacent-margin diagnostic:

```powershell
.\.venv\Scripts\python.exe scripts\run_lava_npz_margin_smoke.py `
  --input .tmp_runtime\lava_npz_smoke\candidate_lava_smoke.npz `
  --output .tmp_runtime\lava_npz_smoke\candidate_lava_margin_metrics.json `
  --seed 0 `
  --window-id lava_npz_smoke_window
```

The materializer only uses `train_selection` rows where
`eligible_for_final_selection=true`; final-holdout rows and ineligible oracle
diagnostics are not exported into the smoke artifact. The NPZ must include
`feature_matrix`, `cost_vector_matrix`,
`optimal_vertex_matrix`, `adjacent_vertex_tensor`, `adjacent_mask`, and scalar
boundary fields: `claim_scope`, `v13_candidate_generation_ready`,
`dt_lava_ready`, `permits_model_training`, `raw_hourly_action_imitation`, and
`market_execution_enabled`. The validator rejects shape mismatches, empty
neighbor masks, `raw_hourly_action_imitation=true`,
`market_execution_enabled=true`, and premature `permits_model_training=true`
unless V13 and DT/LAVA readiness are both true.

The margin-smoke command computes deterministic hinge-style adjacent-vertex
violations and writes normalized DT/LAVA research metrics JSON. It is a CI
diagnostic, not training, not full DFL, not DT deployment, and not market
execution.

The packet command already writes a one-metric aggregate. To combine several
windows or seeds, aggregate one or more metrics files into a CI evidence
summary:

```powershell
.\.venv\Scripts\python.exe scripts\aggregate_dt_lava_research_metrics.py `
  --input-dir .tmp_runtime\lava_npz_smoke `
  --output .tmp_runtime\lava_npz_smoke\dt_lava_research_metrics_aggregate.json
```

The aggregate validates each metrics JSON first and repeats
`ci_smoke_only=true`, `promotion_gate=false`, `permits_model_training=false`,
and `market_execution_enabled=false`. It summarizes research smoke evidence
only; it is not a 4-window promotion gate and cannot start DT/LAVA training.

## Gate

The LAVA scorer can become a stronger Offline Strategy Promotion challenger
only if it:

- has a V13-ready teacher packet with final-holdout scoring rows;
- passes the teacher-packet safe-switch coverage gate;
- carries an explicit deterministic safety projection pass for every strict
  LP/oracle comparison row;
- beats frozen V2+ mean regret;
- does not worsen median regret versus V2+;
- still beats `strict_similar_day` by at least `5%`;
- preserves rolling robustness before headline replacement;
- emits zero market-execution claims.

If the gate fails, the output is still useful negative evidence: it identifies
whether the blocker is weak teacher labels, no safe Poland/TFT candidates, or
over-conservative fallback.

## First Materialized Result

Dagster run:

- `30742a14-2712-4640-9ec8-1aff155f52d1`

Persisted strategy kind:

- `dfl_lava_candidate_value_strict_lp_benchmark`

The bridge materialized successfully, but the scorer did not beat frozen V2+:

| Row | Tenant-anchor rows | Mean regret, UAH | Median regret, UAH | Status |
|---|---:|---:|---:|---|
| Frozen calibrated V2+ | `90` | `174.77` | `67.30` | headline comparator |
| Frozen raw V2+ | `90` | `193.36` | `68.89` | reference |
| `strict_similar_day` | `90` | `310.58` | `198.39` | control |
| Behavior-cloning reference | `90` | `310.58` | `198.39` | required baseline |
| LAVA candidate-value scorer | `90` | `501.25` | `221.77` | negative evidence |

The scorer mostly selected strict-control and strict/raw-blend schedules, but
also selected a small set of rank-extrema perturbation schedules that created
large tail losses. This confirms the main lesson from the Poland ranker: the
current schedule-neighbor feature space is useful for labels and diagnostics,
but not yet strong enough to replace V2+ safely.

## Tail-Risk Target Redesign

The follow-up target uses that negative bridge result directly. Instead of
training DT/LAVA to imitate raw hourly BUY/SELL/HOLD actions, the new target
asks a safer question:

```text
Which feasible schedule candidate or schedule family should be selected, and
when should the system fall back to frozen V2+?
```

The new diagnostic labels candidate rows as `safe_neighbor_candidate`,
`tail_risk_perturbation_loss`, `neutral_or_weak_neighbor`,
`oracle_only_train_diagnostic`, or `v2_plus_default`. The target then blocks
families with prior tail-risk losses and hard-blocks known risky perturbation
families such as `rank_extrema_perturbation_v2_plus`. It emits
`schedule_candidate_index` supervision for future DT/LAVA work. Final-holdout
realized prices may change the strict score, but they do not change the blocked
family list or target selection rules.

Technical spec:
[DFL_LAVA_TAIL_RISK_TARGET.md](DFL_LAVA_TAIL_RISK_TARGET.md).

First tail-risk target result: the redesigned strict benchmark materialized in
Dagster run `60f19630-3469-4d07-9576-14c62c356011`. It hard-blocked risky
perturbation families and fell back to calibrated V2+ for all tenants, matching
the frozen comparator at `174.77` UAH mean regret and `67.30` UAH median regret.

Safe-switch follow-up result: Dagster run `ac432cb6-93b6-476b-a914-baca350aa14e`
trained the prior-profile safe-switch scorer and strict-scored it against the
same frozen comparator. With `poland_shadow_candidate` as the only approved
switch source and a family-level tail-risk veto, no risk profile was allowed.
All five tenants used `18 / 18` V2+ fallback anchors, so the result again
matched frozen calibrated V2+ at `174.77` UAH mean regret and `67.30` UAH median
regret. This is not a new promoted strategy; it is diagnostic evidence that the
next DT/LAVA target must learn better safe-switch labels before generating a
policy.
That is a safe diagnostic closure, not a promotion over V2+.

Baseline leakage audit: on 2026-05-22 the V2+ tie-breaker was corrected so final
selection no longer uses final-holdout `regret_uah`. Corrected V2+ still
materialized at `174.77` / `67.30` UAH mean/median regret for the calibrated
source. The oracle-gap audit then found that calibrated V2+ selected the best
available candidate on `71 / 90` final tenant-anchor rows and missed a better
candidate on `19 / 90` rows. This is now the target shape for DT/LAVA: learn a
prior-only safe switch for the missed-candidate minority, with fallback to the
corrected V2+ comparator.

The additive oracle-gap safe-switch layer is now the immediate bridge before any
new DT/LAVA policy. It creates `dfl_oracle_gap_safe_switch_label_frame`,
`dfl_oracle_gap_safe_switch_feature_panel_frame`,
`dfl_oracle_gap_safe_switch_scorer_frame`,
`dfl_oracle_gap_safe_switch_strict_lp_benchmark_frame`, and
`dfl_oracle_gap_safe_switch_rolling_robustness_frame`. The selector predicts
regret delta and tail-risk probability from prior/train anchors only, switches
only when the candidate looks safe, and otherwise falls back to corrected V2+.
See [DFL_ORACLE_GAP_SAFE_SWITCH.md](DFL_ORACLE_GAP_SAFE_SWITCH.md).

Oracle-gap safe-switch materialization result: Dagster run
`d9ca0064-8fc9-4da1-880a-47ae0d62958d` completed the label, feature, scorer,
strict benchmark, and rolling robustness assets. The label frame preserved the
`71 / 90` V2+-best and `19 / 90` missed-candidate split. The scorer found no
prior-safe switch profile, fell back to corrected calibrated V2+ on `90 / 90`
latest holdout rows, matched V2+ at `174.77` / `67.30` UAH mean/median regret,
and produced `0 / 4` robust challenger windows. This closes the current
safe-switch slice as negative diagnostic evidence, not a promoted result.

## Materialization

After upstream V2+ and Poland evidence rows are available:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_v2_plus_schedule_neighbor_teacher_label_frame,dfl_lava_schedule_neighbor_candidate_frame,dfl_lava_candidate_value_scorer_frame,dfl_lava_candidate_value_strict_lp_benchmark_frame `
  -c configs/real_data_dfl_lava_schedule_neighbor_week3.yaml
```

Tail-risk target materialization:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_v2_plus_schedule_neighbor_teacher_label_frame,dfl_lava_schedule_neighbor_candidate_frame,dfl_lava_candidate_value_scorer_frame,dfl_lava_candidate_value_strict_lp_benchmark_frame,dfl_lava_tail_risk_diagnostic_frame,dfl_lava_tail_risk_aware_target_frame,dfl_lava_tail_risk_aware_strict_lp_benchmark_frame,dfl_lava_tail_risk_safe_switch_scorer_frame,dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame `
  -c configs/real_data_dfl_lava_tail_risk_target_week3.yaml
```

Oracle-gap safe-switch materialization:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_oracle_gap_safe_switch_label_frame,dfl_oracle_gap_safe_switch_feature_panel_frame,dfl_oracle_gap_safe_switch_scorer_frame,dfl_oracle_gap_safe_switch_strict_lp_benchmark_frame,dfl_oracle_gap_safe_switch_rolling_robustness_frame `
  -c configs/real_data_dfl_oracle_gap_safe_switch_week3.yaml
```

Claim boundary remains unchanged: Offline Strategy Promotion/read-model
evidence only, no live dispatch, no dashboard/API default switch, and no
deployed DT controller.
