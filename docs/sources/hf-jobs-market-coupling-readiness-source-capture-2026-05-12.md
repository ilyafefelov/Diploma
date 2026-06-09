# HF Jobs And Market-Coupling Readiness Source Capture

Date: 2026-05-12

Purpose: support the guarded Hugging Face Jobs submission wrapper, the
market-coupling readiness preflight, and the thesis evidence freeze around the
365-anchor Offline Strategy Promotion result.

## Hugging Face Jobs

Primary sources:

- [Hugging Face Hub: Run and manage Jobs](https://huggingface.co/docs/huggingface_hub/guides/jobs)
- [Hugging Face Hub: Jobs pricing and billing](https://huggingface.co/docs/hub/main/en/jobs-pricing)
- [Hugging Face Hub: Jobs configuration](https://huggingface.co/docs/hub/jobs-configuration)
- [Hugging Face Hub CLI guide](https://huggingface.co/docs/huggingface_hub/guides/cli)

Captured implementation facts:

- Jobs can run UV scripts, select hardware with `flavor`, and set explicit
  `timeout` values.
- Jobs support environment variables and encrypted secrets; tokens must not be
  written into local payload or receipt artifacts.
- Jobs require a paid-capable Hugging Face plan/account path and run as
  pay-as-you-go compute.
- The pricing table currently lists Nvidia T4 small at `$0.40/hour`, so the
  default `t4-small` plus `4h` screen caps the nominal compute exposure at
  about `$1.60`, excluding subscription/account requirements and retries.

Repo decision:

- `scripts/build_hf_official_schedule_value_job.py` remains the payload builder.
- `scripts/submit_hf_official_schedule_value_job.py` is the guarded execution
  wrapper. Its default behavior is dry-run receipt generation; paid submission
  requires explicit `--submit`.
- Receipt artifacts record run identity, flavor, timeout, artifact repo, claim
  boundary, and job id/status if submitted, while preserving
  `market_execution_enabled=false`.

## Market-Coupling Readiness

Primary sources already tracked in the market-coupling interface capture:

- [ENTSO-E Transparency Platform](https://www.entsoe.eu/data/transparency-platform/)
- [Nixtla NeuralForecast exogenous variables](https://nixtlaverse.nixtla.io/neuralforecast/docs/capabilities/exogenous_variables.html)
- [Nixtla NBEATSx](https://nixtlaverse.nixtla.io/neuralforecast/models.nbeatsx.html)
- [RunyaoYu/PriceFM dataset](https://huggingface.co/datasets/RunyaoYu/PriceFM)

Captured implementation facts:

- Neighbor-market or European rows are useful only as prior-only covariates, not
  as Ukrainian training rows.
- ENTSO-E feature candidates need source-backed rows, publication-time evidence,
  prior-known currency normalization, timezone/DST alignment, market-rule
  mapping, licensing/API approval, and Ukrainian-domain validation before they
  can enter official NBEATSx/TFT or DFL training.
- Source-backed samples alone do not unlock training.

Repo decision:

- `smart_arbitrage.forecasting.market_coupling_readiness` reports these blockers
  as a preflight summary.
- External rows remain blocked until the existing feature route marks them
  approved and the preflight has no blockers.
- The 365-anchor Offline Strategy Promotion evidence remains Ukrainian-only:
  observed OREE DAM, Open-Meteo/weather context, tenant context, strict LP/oracle
  scoring, and `strict_similar_day` fallback.
