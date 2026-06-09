
Here’s a fast, plain‑English playbook to choose between **N‑BEATSx** and **Temporal Fusion Transformer (TFT)** for forecasting, plus one quick experiment to run next.

# TL;DR

* **Use N‑BEATSx** when you want a **strong global model** that scales across many related series with minimal feature wrangling. Great at learning seasonality/trend from the panel itself; add exogenous features if you have them, but it doesn’t *need* rich feature engineering to shine.
* **Use TFT** when your signal depends heavily on **known future inputs** (prices, tariffs, holidays, weather forecasts) and you want **per‑series & per‑feature conditioning** with attention to interpretability (feature importances, variable selection).

## Background (super short)

* **Global panel** = train one model on many series together (e.g., loads/markets/regions), sharing patterns.
* **Exogenous variables** = extra inputs beyond the target (weather, calendar, market signals). Some are *known* in the future (holiday flags), some are *forecasted* (weather), some are *unknown* (lagged features).

## Where each excels

* **N‑BEATSx**
  * Strengths: fast to train, robust global inductive bias (trend/seasonality blocks), handles sparse panels well, solid with limited or noisy covariates.
  * When it wins: lots of related series; medium feature quality; you need speed and stability.
* **TFT**
  * Strengths: explicit handling of  **known‑future covariates** , dynamic gating, attention over time & variables → better when exogenous signals drive the future.
  * When it wins: rich, reliable exogenous set; you care about attribution/interpretability; horizon is long and covariates matter.

## A quick, fair validation metric

* Report **sMAPE** (scale‑free) and **MAE** (money‑interpretable)  **per horizon** , then average:
  * sMAPE@H, MAE@H (e.g., H∈{1…24} for hourly, or {1…7} for daily).
  * Also compute a  **calibration check** : for probabilistic outputs (if you enable them), use **P50 coverage error** and **P90 coverage** vs. nominal.
* For your energy/BESS use case, also compute **cost‑weighted MAE** (penalize errors more during high‑price hours).

## One easy experiment to try next (fast iteration)

**“Calibrated global panel → local fine‑tune”**

1. Train a **global N‑BEATSx** on all series with light covariates (calendar, simple weather summaries).
2. **Temperature scale** or **quantile re‑map** residuals on a recent validation window (keeps it fast).
3. **Clone weights** and **fine‑tune per critical series** (or per market regime) for a few epochs with a small LR.
4. In parallel, train a **lean TFT** using only *known‑future* covariates you truly trust (calendar + forecasted weather/prices).
5. **Ensemble** : average P50 or do a simple residual‑blend (N‑BEATSx baseline + TFT residuals).
6. Pick the winner by **MAE@H (cost‑weighted)** and  **calibration** ; keep the simpler model unless TFT is clearly better.

## Practical tips

* If feature quality is uncertain →  **start N‑BEATSx** .
* If you have solid, horizon‑aligned known‑future signals →  **test TFT** .
* Keep training/runtime budget in mind: N‑BEATSx usually gets you to “good” fastest; TFT earns its keep when exogenous structure truly drives the future.

If you want, I can sketch a minimal training script (PyTorch/Lightning) that logs sMAPE/MAE@H and runs the global→local fine‑tune/ensemble exactly as above.
