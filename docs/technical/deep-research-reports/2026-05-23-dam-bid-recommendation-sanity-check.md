# DAM Bid Recommendation Sanity Check - 2026-05-23

## Scope

This note audits the current V11/operator boundary around:

```text
forecast_generated_at / anchor_timestamp
  -> market gate status
  -> target delivery date/hour
  -> ProposedBid eligibility
  -> Gatekeeper
```

The question is whether the project can honestly say it recommends DAM market bids now, or whether it is still an operator read-model preview.

## Executive Verdict

The project is now correct only under the narrower claim:

> The operator dashboard shows a DAM-only delivery-day planning preview backed by read-model evidence. It does not emit market-submittable ProposedBid objects, does not evaluate DAM gate eligibility, does not submit market orders, and does not run IDM recommendations.

The project must not currently claim:

- live bid recommendations;
- IDM bid recommendations;
- market-ready DAM ProposedBid generation;
- runtime Bid Gatekeeper execution over emitted ProposedBid rows;
- promoted Decision Transformer control.

#28 fixed the product-claim boundary by making the negative states visible. It did not implement the missing bid-generation or Bid Gatekeeper observability layers. Therefore #10 and #11 remain valid.

## Current Chain Status

| Stage | Current status | Evidence |
| --- | --- | --- |
| `forecast_generated_at` | Present only when persisted forecast-store rows drive the preview; otherwise `null` is valid and visible. | `api/main.py`, `docs/technical/OPERATOR_DAM_TIMING_AND_BID_BOUNDARY.md` |
| `anchor_timestamp` | Read-model/as-of anchor, not first delivery hour. | Operator API tests and #28 follow-up commits |
| Target delivery window | Next DAM delivery day, currently 00:00 -> 00:00 next day. | Live `/dashboard/operator-recommendation` JSON on 2026-05-23 returned `2026-05-24T00:00:00` -> `2026-05-25T00:00:00`. |
| Market gate status | Explicit negative state: `not_evaluated_preview_only`. | API field and UI badge |
| ProposedBid eligibility | Explicit negative state: `not_applicable_no_proposed_bid`. | API field and UI badge |
| ProposedBid emission | Not implemented in operator path. | #10 remains open. |
| Gatekeeper | Pydantic schemas exist, but durable Bid Gatekeeper observability for ProposedBid validation is not implemented. | #11 remains open. |

This is a good intermediate state: the UI no longer hides missing functionality behind optimistic language.

## Market Timing Sanity Check

The Ukrainian Market Operator DAM process says DAM orders for delivery day `D` may be submitted/corrected/deleted up to DAM gate closure at 12:00 on `D-1`; IDM opens after DAM and closes one hour before real time for open settlement periods. This supports separating DAM delivery-day planning from current-hour IDM adjustment. Source: [JSC Market Operator DAM/IDM process](https://www.oree.com.ua/index.php/web/215?lang=english).

The OECD Ukraine electricity sector study describes DAM as next-day hourly trading via blind auction, while IDM adjusts positions after DAM closure and has different continuous/auction mechanics. Source: [OECD Competition Market Study of Ukraine's Electricity Sector](https://www.oecd.org/content/dam/oecd/en/publications/reports/2023/06/competition-market-study-of-ukraine-s-electricity-sector_045239a1/f28f98ed-en.pdf).

Therefore the dashboard should not show "next hour from now" as a DAM recommendation. It should show the DAM delivery-day schedule. The current #28 branch now does this.

## Literature Fit

Decision-focused storage literature supports the project direction, but it also makes the current missing layers more important, not less important.

- Predict-then-bid DFL for strategic storage explicitly combines price prediction, storage optimization, and market-clearing optimization. This supports a future #10-style bid path, not claiming that a price/schedule preview is already a bid. Source: [Yi et al., 2025, arXiv:2505.01551](https://arxiv.org/abs/2505.01551).
- ESS arbitrage DFL optimizes downstream regret/value instead of raw forecast error. This supports the repo's strict LP/oracle regret gates. Source: [Sang et al., 2022/2023, arXiv:2305.00362](https://arxiv.org/abs/2305.00362).
- Multistage DFL is relevant because storage decisions have intertemporal effects through SOC. Source: [Decision-Focused Forecasting for Multistage Optimisation, arXiv:2405.14719](https://arxiv.org/abs/2405.14719).
- Perturbed DFL for storage supports integrating physical storage constraints into learning, but it still stays in evaluated optimization/decision quality space. Source: [Yi, Alghumayjan, Xu, arXiv:2406.17085](https://arxiv.org/abs/2406.17085).

For forecasting:

- NBEATSx is a strong electricity price forecasting baseline with exogenous variables and interpretable decomposition. Source: [Olivares et al., NBEATSx, arXiv:2104.05522](https://arxiv.org/abs/2104.05522).
- TFT is a credible multi-horizon model with feature selection, gating, and interpretable attention. Source: [Lim et al., TFT, arXiv:1912.09363](https://arxiv.org/abs/1912.09363).
- PriceFM and THieF are useful 2025-2026 forecast-layer references, especially for cross-region topology and block/hour reconciliation, but they are not drop-in proof of market-bid readiness. Sources: [PriceFM, arXiv:2508.04875](https://arxiv.org/abs/2508.04875), [THieF, arXiv:2508.11372](https://arxiv.org/abs/2508.11372).

For Decision Transformer:

- DT is an offline RL sequence model, not a magic replacement for missing bid labels or sparse safe-switch examples. Source: [Decision Transformer, arXiv:2106.01345](https://arxiv.org/abs/2106.01345).
- Empirical DT guidance says DT can require more data than CQL for competitive policies, although it can be robust in some sparse/low-quality settings. This supports the current V11 blocker: only 2-7 prior safe-switch examples per tenant is not enough to promote DT/LAVA. Source: [When should we prefer Decision Transformers, arXiv:2305.14550](https://arxiv.org/abs/2305.14550).

## V11 Boundary

`docs/technical/DFL_V11_LOWER_TAIL_RISK_LAVA_DT.md` is correctly conservative:

- V11 is diagnostic evidence, not promoted policy.
- V11/DT/LAVA falls back to V2+ and matches V2+ at 174.77 UAH mean regret / 67.30 UAH median regret.
- `market_execution_enabled=false` remains true.
- DT/LAVA is blocked because teacher labels are sparse.

This is defensible academically. Overriding that blocker to show a DT market action would be an overclaim.

## IDM Decision

Do not "just plug IDM prices into the same DAM pipeline" as active recommendations.

Acceptable near-term IDM uses:

- read-only context beside the DAM preview;
- exogenous feature/covariate experiments that cannot drive bid labels by themselves;
- separate research issue with its own target, validation split, market timing, liquidity/acceptance assumptions, and gate semantics.

Required before active IDM recommendations:

- separate IDM target definition;
- horizon and settlement-period semantics;
- gate closure / eligibility model;
- bid acceptance or market-clearing model;
- no-leakage validation split;
- ProposedBid schema extension or venue-specific IDM bid schema;
- Gatekeeper observability for rejected IDM bids.

## Issue Impact

#28 can stay closed: it fixed the truth-in-UX/API problem and the visual chart regressions.

#10 must stay open: the current system still has no Gold Target Strategy asset emitting typed, venue-scoped ProposedBid rows.

#11 must stay open and should be implemented before any serious market-bid demo: durable validation-failure storage and first-class Bid Gatekeeper observability are the safety audit layer that prevents "No Bid" and physical "HOLD" from being confused.

## Recommended Next Slice

1. Implement #11 first: durable `validation_failures`, latest validation read model, Dagster asset-check/metadata visibility, and canonical `No Bid` vs `HOLD` semantics.
2. Then implement #10 for one DAM venue and one interval type: consume Silver/Gold lineage, produce typed ProposedBid, evaluate market gate status, and send the candidate through Gatekeeper.
3. Treat IDM as a later vertical slice, not a switch inside the current DAM preview path.

