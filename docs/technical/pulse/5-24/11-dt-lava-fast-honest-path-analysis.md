# DT/LAVA Fast Honest Path Analysis

Source set:

- ChatGPT share pulse texts saved in `docs/technical/pulse/5-24/00-index.md`.
- Local deep-research note: `docs/technical/deep-research-reports/DAM-First Academic MVP and the Path to Full Decision-Focused Learning.md`.
- Current V13 packet: `data/research_runs/week3_dfl_ua_context_acquisition_v13_safe_switch_only/dfl_ua_context_v13_acquisition_summary.json`.
- Ecosystem checks: [Solver-Free DFL / LAVA](https://openreview.net/pdf?id=xMcKyUGTt1), [Decision Transformer](https://arxiv.org/abs/2106.01345), [Hugging Face Decision Transformer docs](https://huggingface.co/docs/transformers/model_doc/decision_transformer), [d3rlpy](https://d3rlpy.readthedocs.io/en/v2.8.1/), [DT preference study](https://huggingface.co/papers/2305.14550), [offline RL review](https://arxiv.org/abs/2005.01643), [Nixtla NBEATSx](https://nixtlaverse.nixtla.io/neuralforecast/models.nbeatsx.html), and [Nixtla TFT tutorial](https://nixtlaverse.nixtla.io/neuralforecast/docs/tutorials/forecasting_tft.html).

## Current State Check

The current project is still in the correct DAM-first MVP state:

- `v13_candidate_generation_ready=false`.
- `dt_lava_ready=false`.
- `market_execution_enabled=false`.
- Explicit DAM publication receipts are still blocked: the May OREE source audit probed `01.2026` through `05.2026`, found no candidate receipt source, and generated no receipt CSV. The additional OREE PXS DAM trading-results endpoint is row-level DAM data, but it still lacks the explicit `source_publication_timestamp` column needed for V13.
- Safe-switch evidence is now at the configured `20 / 20` floor for every tenant/source in the current safe-switch-only V13 packet.
- V13 still has `ready_rows=0/5` and `data_acquisition_needed` because `explicit_dam_publication_receipts` is the remaining required source-family blocker.

This means DT/LAVA can move faster as a research lane, but it cannot be promoted, made a dashboard default, or treated as executable control.

## Useful Pulse Ideas To Keep

1. Keep the tiny NPZ idea, but frame it as a research smoke artifact.

   The `Rolling-Window Notebook With Tiny NPZ`, `Neighbor Surrogate Prototype Files`, and `NPZ Schema and One-Window Run` pulse texts all point toward a useful low-cost protocol: precompute a small, deterministic set of feasible schedule vertices or neighbors, store it in NPZ, and run a CI-fast solver-free surrogate smoke. This matches the Solver-Free DFL / LAVA paper's adjacent-vertex idea, but in this repo it must be named as a LAVA-style schedule-neighbor smoke until the LP vertex/adjacency assumptions are proven.

2. Adopt a metrics JSON contract for research lanes.

   The `Auditable metrics.json and CI Gate` pulse text is directly useful. Every future DT/LAVA or surrogate-DFL smoke should emit one compact JSON per lane/seed/window with:

   - `claim_scope`;
   - `market_execution_enabled=false`;
   - `tenant_id`;
   - `source_model_name`;
   - `window_id`;
   - `seed`;
   - regret/value metrics versus frozen V2+ and strict comparator;
   - data-readiness fields copied from V13;
   - `permits_model_training=false` unless V13 source readiness is actually passed.

3. Use ecosystem libraries only after the data contract is strong.

   Hugging Face exposes a Decision Transformer implementation for vector-state tasks. d3rlpy provides offline RL algorithms and off-policy evaluation machinery. Those are good candidates for later baselines, especially CQL/BC/DT comparisons, but the DT preference study says DT needs more data than CQL for competitive policies. Even with the safe-switch floor now at `20 / 20`, the ecosystem answer remains: prepare dataset contracts and CI baselines now, do not train or promote DT until the explicit DAM receipt blocker is closed and V13 grants training permission.

4. Keep NBEATSx/TFT as forecast-layer choices, not bidding proof.

   Nixtla documents NBEATSx as an exogenous-variable forecasting model and TFT as a multi-step forecasting model with gating, recurrent encoding, and attention. The pulse texts are right that these should be compared under rolling-origin, regret/value, and calibration metrics. They do not by themselves justify market execution or full DFL.

5. Treat full DFL as a claim ladder.

   The local DAM-first deep-research note is aligned with the pulse texts: current work is decision-aware offline evidence; next is DAM-only surrogate DFL; then multistage differentiable schedule learning; later strategic bid DFL with market clearing; only after separate gates comes live execution.

## Ideas To Reject Or Hold

- Do not turn the tiny NPZ smoke into a thesis claim. It is a CI guard and learning artifact.
- Do not start raw hourly BUY/SELL/HOLD imitation. The current safer target is candidate index, schedule family, schedule block, or schedule-neighbor value.
- Do not use DT because the library exists. Use DT only after V13 proves enough source-backed teacher trajectories.
- Do not route OREE source-audit negatives into receipt rows. Negative source audit evidence is useful, but it does not satisfy explicit DAM publication receipts.
- Do not describe LAVA-style schedule-neighbor scoring as full differentiable DFL until the training objective truly optimizes downstream decision quality through a justified surrogate or differentiable optimization layer.

## Fast Honest Path

Phase 0: finish V13 source readiness.

- Acquire and validate an explicit OREE DAM publication receipts CSV.
- Keep the validated safe-switch CSV configured in the V13 packet.
- Keep the current V13 packet blocked until the receipt path is configured and validated.

Phase 1: add a solver-free LAVA-style smoke that cannot promote.

- Use a tiny deterministic NPZ of schedule-neighbor candidates.
- Validate schema, masks, dimensions, and `market_execution_enabled=false`.
- Emit metrics JSON, not dashboard defaults.
- Compare against V2+ and strict fallback.

Phase 2: convert V13-passing teacher rows into candidate-index or schedule-family data.

- Input state: forecast context, battery/SOC context, tenant context, return/value target.
- Output target: candidate index, schedule family, or schedule block.
- Evaluation: strict LP/oracle regret and value, not imitation accuracy alone.

Phase 3: run DT only as an offline challenger.

- Use Hugging Face Decision Transformer or d3rlpy only after the V13 data floor is met.
- Keep CQL/BC or simpler supervised baselines as controls.
- Require out-of-sample regret/value evidence, safe-switch coverage, and deterministic safety projection.
- Keep `market_execution_enabled=false`.

Phase 4: pursue full schedule-level DFL separately.

- Move from decision-aware evaluation to a true surrogate or differentiable training objective.
- Keep DAM-only and operator-facing first.
- Strategic bidding and market-clearing DFL remain later than full schedule-level DFL.

## Immediate Repo Actions

Already aligned:

- `src/smart_arbitrage/dfl/lava_schedule_neighbor_bridge.py` contains V2+-anchored schedule-neighbor research surfaces.
- `src/smart_arbitrage/decision_transformer/policy.py` contains a small offline DT primitive plus deterministic battery action projection.
- V13 packet exports safe-switch acquisition targets and receipt-source audit evidence.
- The operator read model now exposes V13 receipt audit freshness and safe-switch target details while keeping execution disabled.
- `src/smart_arbitrage/dfl/dt_lava_research_metrics.py` validates future DT/LAVA metrics JSON before publication.
- `src/smart_arbitrage/dfl/lava_npz_smoke_contract.py` validates tiny NPZ schedule-neighbor smoke artifacts before any solver-free LAVA-style research use.
- `scripts/materialize_lava_npz_smoke_artifact.py` creates that NPZ from existing train-selection schedule-neighbor candidate evidence instead of inventing standalone toy rows.
- `scripts/run_lava_npz_margin_smoke.py` consumes the validated NPZ and emits normalized DT/LAVA research metrics from adjacent-vertex margin diagnostics.
- `scripts/materialize_lava_npz_margin_smoke_packet.py` wraps the NPZ export, margin diagnostic, one-metric aggregate JSON, V13 acquisition-summary attachment, manifest, and validation summary into one CI-fast evidence packet.
- `scripts/validate_lava_npz_margin_smoke_packet.py` verifies packet hashes, optional V13 acquisition-summary hash, NPZ contract, metrics, aggregate JSON, complete strict/V2+ fallback baseline coverage, manifest summary counters, V13 blocker counts, and non-promotion flags before citation.
- `scripts/aggregate_dt_lava_research_metrics.py` aggregates validated DT/LAVA metrics JSON files across additional windows or seeds into a non-promotion CI evidence summary.
- `scripts/materialize_dt_lava_prototype_readiness_packet.py` emits a separate readiness packet that distinguishes passed upstream schedule/value offline promotion, LAVA CI-smoke readiness, V13 DT/LAVA training permission, and market-execution gate status without turning any blocker into a DT/LAVA promotion.
- `scripts/verify.ps1` now has an optional LAVA NPZ margin-smoke hook without making it a promotion gate. It skips by default, runs only when `SMART_ARBITRAGE_VERIFY_LAVA_NPZ_CANDIDATE_FRAME_PICKLE` points to a real candidate-frame pickle, attaches the current V13 acquisition summary when present, attaches validated LAVA NPZ smoke evidence and the official schedule/value promotion registry to the DT/LAVA readiness packet when present, writes under `.tmp_runtime\verify_lava_npz_margin_smoke`, and preserves `promotion_gate=false`, `permits_model_training=false`, and `market_execution_enabled=false`.
- `scripts/audit_v13_dam_receipt_source_leads.py` classifies DAM receipt source-discovery leads separately from validated receipt CSVs. It can mark row-level leads for manual validation, but keeps dataset-level metadata, auth-blocked APIs, and negative probes from closing the explicit DAM publication receipt blocker.
- `scripts/probe_energy_map_dam_receipt_metadata.py` makes the Energy Map DAM metadata check repeatable. The current live probe found `8` file-metadata leads across the DAM trading-results and DAM indexes datasets, including file update timestamps, but the rows remain `file_level_publication_metadata_only` with `candidate_receipt_source_found=false`.
- The V13 packet exporter now folds receipt-source lead rows into `dfl_ua_context_v13_source_acquisition_backlog.csv` as `backlog_item_type=receipt_source_lead`, preserving source URLs, lead status, blocking reasons, and lead-specific next steps without changing readiness, training, or execution gates.
- The V13 packet exporter now also accepts repeatable `--safe-switch-candidate-audit-json` attachments and writes `dfl_ua_context_v13_safe_switch_candidate_audits.json`, with `safe_switch_candidate_audit_summary` kept explicitly non-promotional.
- `scripts/export_ua_context_v13_safe_switch_curation_worksheet.py` turns a safe-switch review backlog into a human curation worksheet that resets canonical V13 evidence fields to pending instead of trusting weak/noncanonical diagnostics.
- `scripts/extract_ua_context_v13_safe_switch_examples_from_curation.py` extracts only `approved_source_backed_v13_safe_switch` rows with source evidence into the existing V13 safe-switch validator contract; it still keeps `permits_model_training=false` and `market_execution_enabled=false`.

Next useful implementation slice:

- Use the current LAVA NPZ validation summary as the CI-smoke input to the DT/LAVA readiness packet.
- Build the V13-gated teacher contract only from V13-passing rows; until the DAM receipt blocker closes, that contract remains a blocked/non-training artifact.
- This remains faster and more academically honest than training a larger DT before source readiness exists.

Current prototype smoke result:

- A local prototype candidate frame was materialized from existing V2+/Poland evidence pickles at `.tmp_runtime/dt_lava_prototype/dfl_lava_schedule_neighbor_candidate_frame.pkl`.
- The durable readiness packet at `data/research_runs/week3_dt_lava_prototype_readiness_current/dt_lava_prototype_readiness_summary.json` reports `upstream_offline_strategy_promotion_gate.passed=true`, `ci_smoke_ready=true`, `dt_lava_prototype_gate_passed=true`, `lava_npz_smoke_packet_validation_gate.passed=true`, and `no_market_execution_safety_gate.passed=true`. It still reports `dt_lava_training_ready=false`, DT/LAVA `promotion_gate_passed=false`, `market_execution_gate_passed=false`, and `market_execution_enabled=false` because V13 still has blocked DAM receipts.
- The NPZ margin-smoke packet at `data/research_runs/week3_dt_lava_lava_npz_smoke_current/lava_npz_margin_smoke_packet_validation.json` validates `8` instances and `8` valid adjacent-neighbor rows with `baseline_comparison_valid=true`, `strict_fallback_anchor_count=8`, `v2_plus_anchor_count=8`, `artifact_hashes_valid=true`, `promotion_gate=false`, `permits_model_training=false`, and `market_execution_enabled=false`.
- The safe-switch backfill path has produced a validated incremental CSV with `77` canonical rows, projecting every tenant/source to `20 / 20` examples in the safe-switch-only V13 packet. That closes the safe-switch-count precondition but does not grant DT/LAVA training permission while explicit DAM receipts remain blocked.
- The receipt source-lead audit path now gives Energy Map/OREE-style ecosystem leads a place in the evidence trail without converting page-level catalog metadata, auth-gated downloads, or row-level data without publication columns into row-level receipts. A lead audit can improve acquisition targeting, but the V13 packet stays blocked until a validated CSV is configured through `oree_dam_publication_receipts_csv_path`.
- The live Energy Map metadata probe at `.tmp_runtime/oree_receipt_probe/energy_map_dam_receipt_metadata_summary_2026-05-24.json` found `8` file-level metadata leads with dataset file update timestamps, but `candidate_receipt_source_found=false`, `receipt_csv_generated=false`, `validated_receipt_csv_ready=false`, and `market_execution_enabled=false`.
- The refreshed local V13 acquisition backlog now has receipt-source blockers/leads plus audit evidence; the top blocker remains `explicit_dam_publication_receipts`.
- Public OREE/PXS and Energy Map observations remain negative receipt evidence, not receipt rows. Workbook-generated timestamps and download observation times are explicitly rejected as `source_publication_timestamp`.
- The next coding path is the credentialless academic MVP packet and offline DT/LAVA evidence protocol. Authenticated SCMO/OREE receipt extraction stays an external-access lane for market-submission-grade receipt proof, not a blocker for the diploma MVP.
