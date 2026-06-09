# Forecasting Metrics That Actually Matter

- Source URL: https://chatgpt.com/share/6a12d229-c614-832b-9b30-754d2a94e20e
- Share ID: `6a12d229-c614-832b-9b30-754d2a94e20e`
- Fetched at: `2026-05-24T11:26:07+00:00`
- Extraction: embedded ChatGPT share payload, text parts only; tool outputs remain redacted where the share redacts them.

## Full Text

### Message 1: assistant (model_editable_context)

{
  "content_type": "model_editable_context",
  "model_set_context": ""
}

### Message 2: assistant (text)

Here’s a compact, plain‑English cheat sheet to pick the *right* forecasting metrics (and plots) so your results from NBEATSx, TFT, or an ensemble actually answer business questions.

# Matching metrics to the real question

- **“How far off are we on average?” → MAE**  
  Mean Absolute Error. Robust to outliers, easy to explain in units (e.g., MWh, $, orders).

- **“How much do big mistakes hurt us?” → RMSE**  
  Root Mean Squared Error. Squares errors, so large misses dominate. Use when big spikes are costly.

- **“How wrong are we in percentage terms?” → MAPE/sMAPE (with guards)**  
  Only when values aren’t near zero. Prefer **sMAPE** or add a small ε to avoid infinities. Good for cross‑SKU/cross‑region comparability.

- **“Do we systematically over‑ or under‑forecast?” → MPE/BIAS**  
  Mean Percentage Error (sign keeps direction). Great for detecting persistent skew (e.g., overcharging battery SoC targets).

- **“Do we hit required service levels?” → Quantile loss / Pinball loss**  
  Use τ=0.5 for median; τ=0.9 for “keep under‑forecast risk low.” This aligns with P50/P90 planning.

- **“What’s the $ value of better forecasts?” → Business loss / Regret**  
  Convert the forecast into the actual decision (dispatch, procurement, staffing) and measure **profit vs. oracle** (or vs. baseline policy). This is the most honest metric for operations like BESS arbitrage.

- **“Are we improving against something sensible?” → Scaled/relative metrics**  
  - **MASE** (Mean Absolute Scaled Error): normalizes by a naïve seasonal forecast; lets you compare across series.  
  - **RMSSE**: squared version when you care about spikes.

# When to prefer normalized metrics

Use **MASE/RMSSE/sMAPE** when:
- You compare models across items with different scales (small loads vs. large loads).
- Your portfolio changes over time (adds/removes assets or SKUs).
- You need leaderboards that are fair across series.

Use **MAE/RMSE** when:
- A single asset dominates value (e.g., one site’s revenue).  
- Stakeholders think in units (“we miss by ~3 MWh per hour”).

# Practical metric set (safe default)

- **Primary:** MASE (robust comparability), MAE (interpretability).  
- **Risk:** Pinball losses at τ ∈ {0.1, 0.5, 0.9}.  
- **Bias:** MPE (or mean error in units).  
- **Business:** Regret vs. oracle/benchmark policy (daily/weekly $).

# Comparing NBEATSx, TFT, and ensembles—what to report

1. **Point accuracy:** MAE, MASE, RMSE.  
2. **Calibration & quantiles:** Pinball loss at P50/P90 + **coverage** (does the P90 interval contain ≈90% of realizations?).  
3. **Bias:** MPE by horizon bucket (e.g., 1–6h, 6–24h, 24–168h).  
4. **Stability:** Std‑dev of errors across time windows (rolling).  
5. **Business impact:** Regret/$ lift per asset and portfolio.  
6. **Ablations:**  
   - NBEATSx vs. TFT vs. **simple ensemble** (mean or median).  
   - **Ensemble almost always wins** on stability; highlight when it reduces tail losses or improves P90 coverage.

# Minimal diagnostic plot checklist (fast to read, hard to fool)

- **Forecast vs. actual (rolled windows):** 3–4 representative weeks; mark events/spikes.  
- **Residuals over time:** look for drift, seasonality left in errors.  
- **Residuals vs. forecasted level:** heteroskedasticity (errors grow with level?).  
- **QQ plot of residuals:** heavy tails → prefer pinball/quantile metrics and robust losses.  
- **Coverage plot:** fraction of points inside P10–P90 band per horizon.  
- **Bias by horizon:** bar chart of mean error across horizons.  
- **Business curve:** cumulative profit and **regret** over time; highlight worst drawdowns.

# Gotchas (save you hours)

- **Zeros kill MAPE.** Use sMAPE, MASE, or add ε.  
- **Outliers distort RMSE.** If you keep RMSE, always pair with MAE.  
- **Aggregate vs. disaggregate.** Report both **macro** (mean of per‑series metrics) and **micro** (pooled) to avoid large series dominating.  
- **Data leakage check.** If validation looks too good, verify time splits, holiday flags, and feature lags.  
- **Quantile consistency.** Ensure P90 ≥ P50 ≥ P10 (monotonic quantiles) or post‑hoc reorder.

# Tiny playbook for your runs

1. Train **NBEATSx**, **TFT**, and a **median ensemble**.  
2. Score **MASE/MAE/RMSE + pinball(0.1/0.5/0.9) + MPE**.  
3. Convert forecasts into the downstream **schedule** and compute **$ regret vs. oracle** (or vs. current policy).  
4. Produce the 6 diagnostics above (same axes/limits for easy visual compare).  
5. Declare a winner by **(a)** lowest MASE, **(b)** best P90 coverage, **(c)** lowest regret. Break ties with stability (variance across windows).

If you want, I can output a ready‑to‑run evaluation template (Python) that ingests your NBEATSx/TFT/ensemble predictions, computes these metrics (including pinball & MASE), and generates the plots + a compact PDF report.
