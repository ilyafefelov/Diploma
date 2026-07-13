# v1.3 aligned DFL full-context experimental result

Status: **positive within-architecture DFL evidence; not promoted**.

## Inputs and protocol

The ENTSO-E token-backed source run `5e2f4096-9034-4f99-8a20-d034fba48c45`
materialized 11,638/11,638 source-backed, prior-safe Poland lag-24 rows and
485 NBU EUR/UAH observations. Licensing, timezone, currency, market-rule, and
temporal-availability checks were ready. Domain shift remains unvalidated, so
the joined 58,190-row frame is explicitly `experimental_ablation_only`.

The full context builder joined that frame to the previously materialized
365-day rolling TFT p10/p50/p90 packet. It produced 1,825 complete 24-hour
examples: 365 chronological anchors per tenant. Every tenant used 319 train,
28 validation, and 18 untouched future-test anchors. Inputs were forecast
quantiles, Ukrainian lag-24 price, weather, calendar, and Poland lag-24;
realized prices remained labels only.

One 2-layer contextual transformer was trained under forecast loss, then copied
into the same-architecture hybrid condition. A validation-only grid selected
`hybrid_weight=0.5` and `smoothing_weight=0.0`; the final test was strict LP.

## Strict future-test result

| Condition | Mean regret (UAH) | Delta vs forecast transformer |
| --- | ---: | ---: |
| Forecast-loss transformer | 284.9300 | — |
| Warm-started hybrid transformer | 267.0265 | **−17.9034** |

This is a 6.28% regret reduction relative to the same transformer trained under
forecast loss. It is the first positive result for the preregistered primary
DFL question: decision loss improved strict decision quality over the same
forecast architecture on an untouched future block.

## Limits and interpretation

- The 90 profile outcomes share only 18 market dates; they are not 90
  independent episodes and no p-value is reported.
- The raw TFT p50 mean regret on the same block was 2033.5835 UAH, which rules
  out an interpretation that the result simply copied the original TFT output.
- The differentiable training log emitted four `Solved/Inaccurate` warnings.
  Strict LP scoring is deterministic and reported separately, but this remains
  preliminary research evidence rather than a promotion result.
- This experiment is Poland `experimental_ablation_only`; domain-shift
  validation is still required for official training. It does not replace the
  released V2+ headline, does not establish a system-level V2+ improvement,
  and does not authorize a temporal Decision Transformer run.

`market_execution_enabled=false` throughout.
