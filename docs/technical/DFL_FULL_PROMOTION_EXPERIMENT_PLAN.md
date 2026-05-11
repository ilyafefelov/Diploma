# DFL Full Promotion Experiment Plan

Date: 2026-05-11

This document is the working protocol for the long-running experiment loop whose
goal is to find, verify, and academically document a real Decision-Focused
Learning (DFL) research challenger for Ukrainian BESS DAM arbitrage. It is not a
promise that DFL will pass promotion. It is the reproducible path for discovering
whether DFL can beat the frozen `strict_similar_day` LP control under strict
rolling-origin evidence.

Claim boundary: until the final gate passes, every result remains offline
research evidence. The system does not claim live market execution, a deployed
Decision Transformer controller, full multi-market optimization, or a full
electrochemical digital twin.

## 1. Current Evidence State

The project already has a useful evidence stack:

- observed OREE DAM and tenant historical Open-Meteo benchmark path;
- frozen `strict_similar_day` Level 1 control comparator;
- strict LP/oracle evaluation with UAH-native regret, throughput, degradation
  proxy, SOC feasibility, and claim-boundary flags;
- compact `nbeatsx_silver_v0` and `tft_silver_v0` forecast candidates;
- calibration, selector, trajectory/value, residual DFL, offline DT, and
  source-specific challenger experiments;
- Dagster evidence checks and Postgres/API read-model validation.

The strongest blocker is also clear:

- current all-tenant Ukrainian evidence ceilings at 104 eligible anchors per
  tenant;
- `2026-03-29 23:00` remains an unrecovered `price_and_weather_gap`;
- the 180-anchor promotion target is blocked with `coverage_gap`;
- TFT has shown a useful latest-holdout signal, but rolling strict-control
  robustness remains insufficient;
- compact and official-smoke NBEATSx/TFT evidence has not yet proven stable
  downstream LP value superiority.

The next work must therefore separate five possible causes:

1. data coverage is too small or has a source-backed gap;
2. timestamp/horizon/IO alignment is wrong;
3. compact forecast candidates are too weak or undertrained;
4. forecast accuracy is not aligned with BESS decision value;
5. DFL/DT training is using the wrong target, loss, or trajectory representation.

## 2. Research Grounding

The experiment loop is anchored in the following academic and implementation
sources:

- [Smart Predict-then-Optimize](https://arxiv.org/abs/1710.08005) and SPO/SPO+
  justify replacing pure forecast-error training with downstream decision-loss
  evaluation.
- [Decision-Focused Learning survey](https://huggingface.co/papers/2307.13565)
  frames DFL as training models for constrained-decision quality.
- [Electricity Price Prediction for ESS Arbitrage: A Decision-focused
  Approach](https://arxiv.org/abs/2305.00362) is the closest direct reference
  for storage-arbitrage price forecasting by regret/surrogate-regret.
- [Decision-Focused Forecasting for Multistage
  Optimisation](https://arxiv.org/abs/2405.14719) supports trajectory/value
  learning because BESS dispatch is SOC-path dependent.
- [Perturbed DFL for strategic energy storage](https://arxiv.org/abs/2406.17085)
  supports perturbed or surrogate storage losses when strict LP is hard to
  differentiate through directly.
- [NBEATSx](https://arxiv.org/abs/2104.05522) and [Nixtla NBEATSx
  docs](https://nixtlaverse.nixtla.io/neuralforecast/models.nbeatsx.html)
  support official exogenous electricity-price forecasting experiments.
- [Temporal Fusion Transformer](https://arxiv.org/abs/1912.09363) and [PyTorch
  Forecasting TFT docs](https://pytorch-forecasting.readthedocs.io/en/latest/api/pytorch_forecasting.models.temporal_fusion_transformer.html)
  support multi-horizon forecasting with static, known-future, observed-past,
  and interpretable variable-selection inputs.
- [PriceFM](https://huggingface.co/papers/2508.04875) and
  [THieF](https://huggingface.co/papers/2508.11372) support future
  market-coupling and temporal hierarchy features, not current promotion.
- [TSFM leakage evaluation](https://huggingface.co/papers/2510.13654) and
  [GIFT-Eval](https://arxiv.org/abs/2410.10393) reinforce strict temporal
  evaluation, data availability, and no hidden benchmark leakage.
- [ENTSO-E Transparency Platform API guide](https://transparency.entsoe.eu/content/static_content/download?path=%2FStatic+content%2Fweb+api%2FIG-for-TP-data-extraction-process.pdf)
  is the future source for European day-ahead market-coupling covariates.

## 3. Non-Negotiable Experimental Rules

Every implementation slice must obey these constraints:

1. Final scoring uses strict LP/oracle evaluation, never relaxed training loss.
2. `strict_similar_day` remains the default fallback and frozen comparator.
3. Final-holdout actuals may affect scoring only, never training, selection,
   thresholds, feature generation, checkpoint selection, or model tuning.
4. Any European market row is `training_use_allowed=false` until licensing,
   timezone, currency, market-rule, price-cap, publication-time, and domain-shift
   checks pass.
5. Synthetic/demo rows cannot enter a `thesis_grade` promotion claim.
6. A negative result is evidence, not a bug, unless the pipeline truth audit
   proves an alignment or persistence defect.
7. Documentation must be updated in the same slice as the experiment result.
8. Commits must remain small enough to isolate code fixes from evidence/docs.

## 4. Gate Definition

The final offline/read-model promotion gate is:

- five canonical tenants;
- observed OREE/Open-Meteo coverage, or a documented narrower accepted scope;
- at least 90 validation tenant-anchors per promoted source/regime;
- no train/final leakage;
- zero safety violations;
- `not_full_dfl=true` and `not_market_execution=true`;
- latest holdout mean regret improves by at least 5% versus
  `strict_similar_day`;
- median regret is not worse than `strict_similar_day`;
- at least 3 of 4 rolling strict-control windows pass for the same
  source/regime;
- fallback behavior remains strict in OOD or undercovered regimes.

If the gate passes, the allowed claim is:

> Source/regime-specific DFL research controller promoted for offline strategy
> evidence.

It is still not live market execution.

## 5. Phase A: Forecast Pipeline Truth Audit

Purpose: prove whether NBEATSx/TFT/DFL failures are genuine model failures or
pipeline defects.

Add a Dagster-visible diagnostic asset:

- `forecast_pipeline_truth_audit_frame`
- group: `gold_dfl_training`
- evidence scope: `research_only`

Required checks:

- perfect forecast sanity: `forecast_vector == actual_vector` should approach
  oracle strict value and reveal any LP/scoring bug;
- horizon shift audit: score selected candidates at `-2`, `-1`, `0`, `+1`,
  `+2` hour forecast shifts to detect off-by-one alignment;
- timestamp/DST audit around `2026-03-29 23:00`;
- vector round-trip audit: in-memory vector, stored vector, and reloaded vector
  must match;
- UAH/MWh unit audit and price-cap sanity;
- forecast-available weather audit: no future observed weather in training
  features;
- source-kind audit: no synthetic/mixed row in thesis-grade scoring.

Exit decision:

- If perfect forecast fails, fix LP/scoring/persistence before any modeling.
- If shifted forecast beats unshifted forecast, fix horizon alignment.
- If IO round-trip changes vectors, fix persistence before training.
- If all checks pass, proceed to official forecast rolling benchmark.

Implementation status:

- Added `forecast_pipeline_truth_audit_frame` and asset check
  `forecast_pipeline_truth_audit_evidence`.
- Materialization run `b78b16aa-1da8-4f58-8ce1-89c5d508a9e2` passed the new
  truth-audit check over the five-tenant 104-anchor panel.
- No blocking failures were found for source provenance, UAH/MWh unit sanity,
  vector round-trip, leaky horizon rows, or non-hourly horizon gaps.
- Shift warnings remain high for compact NBEATSx/TFT and must be carried into
  the official rolling forecast experiment.

## 6. Phase B: UA Coverage Repair And Backfill

Purpose: determine whether the current 104-anchor ceiling is real.

Actions:

- inspect OREE and Open-Meteo local/source caches for `2026-03-29 23:00`;
- recover only source-backed observed rows;
- if recovery succeeds, rematerialize 120/180/365 anchor configs;
- if recovery fails, update docs and lock the current panel as the true local
  ceiling until new source access is added.

Exit decision:

- If 180+ anchors become thesis-grade, rerun robustness gates on the larger
  panel.
- If not, keep 104 anchors and tighten source/regime-specific gates without
  claiming broad production promotion.

## 7. Phase C: Official Forecast Rolling Benchmark

Purpose: test whether the compact in-repo candidates are the bottleneck.

Use optional `sota` dependencies only when available:

```powershell
uv sync --extra dev --extra sota
```

Candidate assets:

- existing `nbeatsx_official_price_forecast`;
- existing `tft_official_price_forecast`;
- new rolling strict benchmark if the current official path is only
  current-horizon readiness.

Experimental settings:

- deterministic seed;
- CPU-safe serious run first;
- NBEATSx: `max_steps=100` or higher if runtime permits;
- TFT: `max_epochs=15-30`, small hidden size, quantile output;
- same five tenants;
- same rolling-origin anchors;
- same strict LP/oracle evaluator.

Exit decision:

- If official forecasts materially improve LP regret, use them as DFL source
  models.
- If official forecasts still lose badly, focus on exogenous market-coupling
  features and decision loss.

## 8. Phase D: Market-Coupling Exogenous MVP

Purpose: add the missing market context that literature suggests is important
for electricity-price forecasting.

Initial sources:

- ENTSO-E day-ahead prices for Poland and neighboring bidding zones;
- optional cross-border capacity/flow indicators if available;
- OPSD/Ember as research-only source-map context first.

Feature rules:

- European data enters only as `feature_*` covariates;
- Ukrainian OREE DAM remains the target;
- all EU rows require timezone, DST, publication-time, currency, and
  market-rule metadata;
- no EU row is allowed as a Ukrainian training target.

Candidate features:

- `feature_pl_dam_price_lag_24h`;
- `feature_neighbor_price_spread_uah_mwh`;
- `feature_neighbor_spread_volatility_7d`;
- `feature_cross_border_regime`;
- `feature_eu_market_coupling_context_available`.

Exit decision:

- If market-coupling features improve official TFT/NBEATSx strict LP regret,
  include them in the DFL v2 dataset.
- If not, keep them as analysis-only external-validation context.

Implementation status:

- Added `forecast_afe_feature_catalog_frame` source-governance fields for
  ENTSO-E, PriceFM, OPSD, Ember, Nord Pool, and THieF.
- Added `market_coupling_temporal_availability_frame` as the executable gate
  before any European/neighbor-market source can become a training feature.
- The gate records PriceFM Dataset Viewer metadata checked on 2026-05-11:
  `140,257` rows, `191` first-row columns, `default/train` split.
- All external sources remain `training_use_allowed=false`; the unresolved
  blockers are licensing, timezone, currency, market rules, temporal
  availability, and domain shift.
- Added `entsoe_neighbor_market_query_spec_frame` to prepare the first concrete
  ENTSO-E day-ahead price query shape: `A44` price document, `A01` day-ahead
  process, Poland/Slovakia/Hungary/Romania mapped as neighbor candidates, and
  Moldova left as `review_required`.
- No ENTSO-E security token is available locally, so the access gate blocks all
  fetches and records the blocker instead of silently inventing data.
- Next executable decision: either map an ENTSO-E Poland/neighbor sample with
  publication timestamps and terms, or keep external sources as validation-only
  while DFL v2 uses Ukrainian prior-only features.

## 9. Phase E: DFL v2 Schedule/Value Learner

Purpose: move from horizon-bias correction to actual decision-value learning.

Inputs:

- candidate schedule library v2;
- official forecast strict schedules;
- compact forecast strict schedules;
- strict baseline schedule;
- calibrated schedule variants;
- market-coupling feature panel if Phase D passes;
- strict-failure feature audit clusters.

Model classes:

- linear schedule ranker;
- shallow MLP score model;
- residual forecast correction with bounded output;
- pairwise value ranking against strict/best-known schedules.

Loss:

- pairwise regret ranking;
- relaxed decision loss;
- spread/rank AFL stabilizers;
- small MAE stabilizer;
- fallback penalty when non-strict is chosen and strict wins;
- degradation/throughput regularization.

Exit decision:

- If DFL v2 improves raw neural schedules but not strict, document development
  pass only.
- If DFL v2 beats strict in latest but not rolling, feed Phase G robustness.
- If DFL v2 passes rolling and median gates, it becomes a promotion candidate.

## 10. Phase F: Offline DT Candidate

Purpose: test Decision Transformer only after trajectory/value labels are clean.

Dataset:

- state: prior prices, calendar, tenant static features, weather/load context,
  market-coupling context, SOC, candidate schedule features;
- action: LP dispatch or discrete charge/discharge/hold label;
- reward: UAH net value per step;
- return-to-go: remaining schedule value;
- teacher labels: best-known feasible schedule on train/inner anchors only;
- final-holdout labels: scoring only.

Baselines:

- filtered behavior cloning;
- residual schedule/value ranker;
- strict fallback.

Tiny DT first:

- context length 24;
- hidden dim 32;
- one layer;
- two heads;
- max 5 epochs;
- fixed seed;
- no live execution path.

Exit decision:

- DT is useful only if it beats filtered behavior cloning and survives strict
  LP/oracle scoring.
- If DT fails, keep it as roadmap evidence and continue with schedule/value DFL.

## 11. Phase G: Robustness And Promotion Loop

For any promising candidate:

1. run latest-holdout strict gate;
2. run four rolling 18-anchor windows;
3. run tenant-slice analysis;
4. run source-specific analysis;
5. run regime-specific analysis;
6. rerun production promotion gate;
7. export registry summary;
8. update thesis docs and research integration plan.

Promotion is blocked unless the same source/regime passes all gates.

## 12. Phase H: Documentation And Commit Protocol

Every experiment run updates:

- this plan if the strategy changes;
- [RESEARCH_INTEGRATION_PLAN.md](RESEARCH_INTEGRATION_PLAN.md);
- the relevant technical evidence doc;
- [docs/README.md](../README.md) if a new durable entrypoint is created;
- [../thesis/chapters/02-literature-review.md](../thesis/chapters/02-literature-review.md)
  only when literature/claim boundaries change;
- weekly report/demo docs when supervisor-facing evidence changes.

Commit pattern:

1. code fix commit, if pipeline behavior needed correction;
2. experiment asset/config commit;
3. docs/evidence commit;
4. rerun commit only when results materially change tracked docs.

Generated `data/` outputs stay local unless a concise summary is intentionally
selected for tracking.

## 13. Immediate Next Slice

Phase A is complete and the next implementation slice is Phase C, with Phase B
remaining available if coverage limits block promotion:

> Official Forecast Rolling-Origin Benchmark.

This is the fastest way to determine whether compact NBEATSx/TFT failures are
caused by the lightweight in-repo candidates or persist when official adapters
are trained per anchor with prior-only inputs.

Acceptance criteria for the next slice:

- `official_forecast_rolling_origin_benchmark_frame` materializes;
- tests prove future targets are masked in official adapter inputs;
- strict LP/oracle rows include `strict_similar_day`, `nbeatsx_official_v0`,
  and `tft_official_v0`;
- Dagster definitions validate;
- docs record whether official adapters improve strict LP/oracle evidence;
- commit is created before moving to the next slice.

Phase C status:

- First official rolling run completed under
  `768c9796-422d-40b7-8f8d-083a861cc0e7`.
- It produced 30 rows for five tenants, two anchors per tenant, and three
  forecast candidates.
- It also exposed and fixed a real NBEATSx rolling-window null-feature bug.
- Current evidence does not promote official NBEATSx/TFT: strict remains better
  on mean regret in the first CPU-safe sample.
- The four-anchor scale run `bbbd5828-2414-42ce-b0df-ad175cbac445` also kept
  strict ahead: strict mean/median regret `1,020.821` / `771.866` UAH versus
  NBEATSx official `1,508.667` / `1,277.428` UAH and TFT official `1,535.299` /
  `1,065.955` UAH.

Next iteration after the official rolling commit:

- move to Phase D market-coupling/exogenous context and Phase E decision-loss
  learning on the now-fixed source path;
- scale official rolling anchors further only when richer features or a stronger
  objective exist, because CPU runtime is already about 30 minutes for four
  anchors per tenant;
- keep strict as fallback until rolling robustness passes.
