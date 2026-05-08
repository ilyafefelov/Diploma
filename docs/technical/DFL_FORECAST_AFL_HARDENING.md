# Forecast AFL Hardening For DFL

Date: 2026-05-08

This slice fixes the next root-cause layer before another selector experiment:
the project needs cleaner forecast evidence before stronger DFL claims. It
defines **Arbitrage-Focused Learning (AFL)** as the forecast-layer bridge from
NBEATSx/TFT research forecasts to full differentiable DFL.

Claim boundary: AFL is research evidence only. It is not full DFL, not Decision
Transformer control, not a production bid policy, and not market execution.

## Why This Slice Exists

The latest feature-aware strict-failure selector nearly cleared the conservative
strict gate:

| Source model | Feature-aware selector mean | Improvement vs strict |
|---|---:|---:|
| `nbeatsx_silver_v0` | 299.73 UAH | 4.79% |
| `tft_silver_v0` | 299.19 UAH | 4.96% |

The threshold is 5%, so promotion remains blocked. That near miss is useful, but
it also shows that the next work should improve the forecast/evidence substrate,
not add another hourly classifier.

Two hardening decisions are now explicit:

- rolling-origin neural forecasts use forecast-available weather mode, so future
  observed weather does not silently enter NBEATSx/TFT inputs;
- current `nbeatsx_silver_v0` and `tft_silver_v0` are compact in-repo research
  candidates, while `nbeatsx_official_v0` and `tft_official_v0` remain optional
  official-backend readiness paths until a tracked full training run is executed.

## New Assets

| Asset | Purpose |
|---|---|
| `forecast_candidate_forensics_frame` | Labels forecast candidates as frozen control, compact Silver candidate, official backend readiness, or unclassified research candidate. |
| `afl_training_panel_frame` | Builds a sidecar AFL panel with prior-only `feature_*` columns and realized `label_*` decision-value columns. |

Config:
[real_data_afl_hardening_week3.yaml](../../configs/real_data_afl_hardening_week3.yaml).

Materialization:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize -m smart_arbitrage.defs --select observed_market_price_history_bronze,tenant_historical_weather_bronze,real_data_benchmark_silver_feature_frame,real_data_rolling_origin_benchmark_frame,forecast_candidate_forensics_frame,afl_training_panel_frame -c configs/real_data_afl_hardening_week3.yaml
```

## AFL Panel Semantics

`feature_*` columns are usable for prior-only research:

- anchor hour and weekday;
- forecast price spread;
- forecast active-hour count;
- prior model regret;
- prior strict-control regret;
- prior model advantage versus strict control.

`label_*` columns are labels, not deployable forecast inputs:

- realized regret and regret ratio;
- realized decision value and oracle value;
- degradation penalty and throughput;
- actual price spread;
- decision weight for arbitrage-focused learning.

This keeps the evaluation line clear: realized prices and oracle values can teach
future experiments, but they cannot enter a live forecast path.

## Next Technical Use

Use this panel before full DFL to answer:

1. Are compact NBEATSx/TFT failures mostly forecast shape failures, rank/extrema
   failures, or LP-value failures?
2. Does an official NBEATSx/TFT training run improve AFL labels before strict
   LP/oracle scoring?
3. Does a simple AFL-weighted forecast baseline beat the compact candidates
   before we spend more time on differentiable training?

Promotion still belongs to the strict LP/oracle gate against
`strict_similar_day`.
