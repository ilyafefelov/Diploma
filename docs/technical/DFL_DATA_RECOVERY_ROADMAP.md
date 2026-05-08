# DFL Data Recovery Roadmap

Date: 2026-05-08

This roadmap follows the blocked action-classifier result. The next source of
improvement should be data coverage and trajectory/value evidence, not another
static per-hour classifier tweak.

## UA-First Recovery

Current checked Ukrainian panel:

| Evidence item | Current state |
|---|---:|
| Canonical tenants | 5 |
| Source models | 2 |
| Eligible anchors per tenant/model | 104 |
| Train-selection rows | 860 |
| Final-holdout rows | 180 |
| Final-holdout anchors per tenant/model | 18 |
| Persisted action-label rows | 1,040 |
| Claim flags | `not_full_dfl=true`, `not_market_execution=true` |

Next audit target:

- determine whether OREE/Open-Meteo local/source history can support more than
  104 eligible anchors per tenant;
- if yes, backfill toward 180-365 anchors per tenant with the same
  rolling-origin/no-leakage split discipline;
- if no, document the true ceiling and move to richer features plus
  trajectory/value learning.

The audit must keep tenant-specific anchor eligibility, price/weather gap
counts, latest-batch freshness, and observed/thesis-grade coverage visible in
Dagster metadata.

## European Dataset Bridge

European data is useful now for source mapping and future external validation,
but not for Ukrainian DFL training yet.

| Source | Status | Metadata / reason |
|---|---|---|
| [RunyaoYu/PriceFM](https://huggingface.co/datasets/RunyaoYu/PriceFM) | include/watch | Hugging Face Dataset Viewer works; 140,257 rows; 15-minute timestamps; European price/load/generation columns; future external validation only. |
| [PriceFM paper](https://huggingface.co/papers/2508.04875) | include/watch | European cross-region electricity-price foundation-model direction. |
| [lipiecki/thief](https://huggingface.co/datasets/lipiecki/thief) | watch | Paper is relevant, but Dataset Viewer is currently unavailable. |
| [THieF paper](https://huggingface.co/papers/2508.11372) | watch | Future temporal hierarchy/block-product forecasting idea. |
| [ENTSO-E Transparency Platform](https://www.entsoe.eu/data/transparency-platform/) | watch | Future market-coupling and external validation context. |
| [Open Power System Data time series](https://data.open-power-system-data.org/time_series/) | watch | Future hourly European price/load/renewables context. |
| [Ember API](https://ember-energy.org/data/api/) | watch | Future generation, demand, emissions, and carbon-intensity context. |
| [Nord Pool Data Portal](https://www.nordpoolgroup.com/en/services/power-market-data-services/dataportalregistration/) | watch/restricted | Commercial/API-gated source; reference only unless access is approved. |

European rows remain `training_use_allowed=false` until these blockers are
resolved:

- currency and UAH/EUR normalization;
- timezone and DST alignment;
- Ukrainian DAM versus European market-rule differences;
- price caps and market-coupling semantics;
- licensing/API access;
- domain-shift validation against Ukrainian OREE evidence.

## Trajectory/Value Learner Direction

The next modeling slice should learn over feasible schedules rather than raw
hourly labels.

Candidate schedule sources:

- frozen `strict_similar_day` LP schedule;
- raw `tft_silver_v0` and `nbeatsx_silver_v0` LP schedules;
- calibrated forecast schedules;
- simple price-perturbation schedules that preserve LP feasibility;
- later, differentiable relaxed-LP candidates if the data gate improves.

Training target:

- select or rank feasible schedules using prior anchors only;
- optimize realized strict LP/oracle regret, net value, and safety;
- keep final-holdout scoring unchanged.

Promotion remains blocked unless the candidate beats `strict_similar_day` under
the conservative strict LP/oracle gate.

## Acceptance For Next Slice

The next slice is ready when:

- the UA coverage audit states whether 180-365 anchors per tenant are possible;
- external sources are registered but not mixed into training;
- a trajectory/value dataset contract is defined from existing feasible
  schedule rows;
- tests prove final holdout does not influence feature selection, schedule
  generation, or model selection;
- the strict LP/oracle gate remains the only promotion authority.
