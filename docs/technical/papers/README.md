# Core Paper Context

This folder stores the core research references currently used to ground the Level 1 baseline and the later DFL target strategy.

## Downloaded PDFs

- `2505.01551-predict-then-bid.pdf` — A Decision-Focused Predict-then-Bid Framework for Energy Storage Arbitrage
- `2104.05522-nbeatsx.pdf` — Neural basis expansion analysis with exogenous variables: Forecasting electricity prices with NBEATSx

## Paywalled References

- Profitability of energy arbitrage net profit for grid-scale battery energy storage considering dynamic efficiency and degradation
  - DOI: `10.1016/j.est.2024.112380`
  - Canonical URL: `https://doi.org/10.1016/j.est.2024.112380`
  - Status: not stored locally because the article is paywalled

- Probabilistic electricity price forecasting based on penalized temporal fusion transformer
  - DOI: `10.1002/for.3084`
  - Canonical URL: `https://doi.org/10.1002/for.3084`
  - Status: not stored locally because the article is paywalled

## Context Split

- Level 1 baseline: hourly DAM-only LP baseline, strict similar-day naive forecast, rolling-horizon execution
- Level 2 target: predict-then-bid DFL architecture with differentiable clearing, venue-aware bidding, and learned strategy layers