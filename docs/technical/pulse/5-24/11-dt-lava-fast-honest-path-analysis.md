# DT/LAVA Fast Honest Path Analysis

Source set:

- ChatGPT share pulse texts saved in `docs/technical/pulse/5-24/00-index.md`.
- Local deep-research note: `docs/technical/deep-research-reports/DAM-First Academic MVP and the Path to Full Decision-Focused Learning.md`.
- Current V13 packet: `data/research_runs/week3_dfl_ua_context_acquisition_v13/dfl_ua_context_v13_acquisition_summary.json`.
- Ecosystem checks: [Solver-Free DFL / LAVA](https://openreview.net/pdf?id=xMcKyUGTt1), [Decision Transformer](https://arxiv.org/abs/2106.01345), [Hugging Face Decision Transformer docs](https://huggingface.co/docs/transformers/model_doc/decision_transformer), [d3rlpy](https://d3rlpy.readthedocs.io/en/v2.8.1/), [DT preference study](https://huggingface.co/papers/2305.14550), [offline RL review](https://arxiv.org/abs/2005.01643), [Nixtla NBEATSx](https://nixtlaverse.nixtla.io/neuralforecast/models.nbeatsx.html), and [Nixtla TFT tutorial](https://nixtlaverse.nixtla.io/neuralforecast/docs/tutorials/forecasting_tft.html).

## Current State Check

The current project is still in the correct DAM-first MVP state:

- `v13_candidate_generation_ready=false`.
- `dt_lava_ready=false`.
- `market_execution_enabled=false`.
- Explicit DAM publication receipts are still blocked: the May OREE source audit probed `01.2026` through `05.2026`, found no candidate receipt source, and generated no receipt CSV.
- Safe-switch evidence is still short by `77` examples across `5` tenant/source pairs.
- The largest safe-switch target is `client_004_kharkiv_hospital`, which needs `18` additional prior/train non-tail-risk material safe-switch examples.

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

   Hugging Face exposes a Decision Transformer implementation for vector-state tasks. d3rlpy provides offline RL algorithms and off-policy evaluation machinery. Those are good candidates for later baselines, especially CQL/BC/DT comparisons, but the DT preference study says DT needs more data than CQL for competitive policies. With only `2-7` current prior safe-switch examples per tenant/source, the ecosystem answer is: prepare the dataset and baselines now, do not promote DT now.

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
- Backfill `77` source-backed safe-switch examples, prioritizing `client_004_kharkiv_hospital`.
- Keep the current V13 packet blocked until those paths are configured and validated.

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

Next useful implementation slice:

- Connect a CI-fast smoke run to these two validators now that a deterministic NPZ fixture can be sourced from existing schedule-neighbor evidence.
- The smoke should emit validated summary JSON and DT/LAVA metrics JSON, not a trained policy, dashboard default, or market-submittable bid.
- This remains faster and more academically honest than training a larger DT before the V13 data floor exists.
