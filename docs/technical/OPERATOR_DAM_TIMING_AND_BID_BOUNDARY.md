# Operator DAM Timing and Bid Boundary

## Current Contract

The operator recommendation surface is a DAM-only hourly planning preview. It is a read-model contract over FastAPI/Dagster evidence, not a market execution path.
This is the diploma MVP product boundary: an operator can review the next DAM
delivery-day schedule, projected SOC, and value evidence, but the system does
not produce `ProposedBid`, market order payloads, cleared trades, or dispatch
commands.

Current flow:

```text
forecast/read-model context
  -> deterministic Level 1 DAM LP preview
  -> feasible hourly recommendation schedule for the next DAM delivery day
  -> operator review UI
```

Current non-flow:

```text
recommendation schedule
  -/-> market gate closure check
  -/-> ProposedBid eligibility
  -/-> ProposedBid
  -/-> market submission
  -/-> ClearedTrade
  -/-> DispatchCommand
```

## API Boundary Fields

`GET /dashboard/operator-recommendation` now exposes explicit timing and
claim-boundary metadata. `GET /dashboard/baseline-lp-preview` also exposes the
same DAM timing fields and non-execution boundary fields so the baseline DAM
preview remains self-describing when consumed outside the combined operator
recommendation read model:

| Field | Current meaning |
| --- | --- |
| `market_scope` | `dam_hourly_planning_preview` |
| `market_venue` | `DAM` |
| `interval_minutes` | `60` |
| `anchor_timestamp` | As-of forecast/read-model anchor; it is not the first delivery hour |
| `forecast_generated_at` | Persisted forecast generation time when an official forecast-store row drives the operator preview; `null` for the strict similar-day baseline preview |
| `target_delivery_window_start` / `target_delivery_window_end` | Next DAM delivery-day window covered by the visible hourly schedule |
| `market_execution_enabled` | Always `false` for the current operator surface |
| `read_model_boundary` | `operator_preview_no_market_submission` |
| `market_gate_status` | `not_evaluated_preview_only` |
| `bid_eligibility_status` | `not_applicable_no_proposed_bid` |
| `proposed_bid_status` | `not_emitted_operator_preview` |
| `bid_recommendation_preview` | Operator-facing DAM BUY/SELL/HOLD preview derived from `recommendation_schedule`; every row is non-submittable with `preview_only=true`, `market_order_payload_emitted=false`, and `market_execution_enabled=false` |
| `v13_readiness` | Current V13 acquisition/source-readiness summary from the local V13 packet: gate status, ready rows, missing safe-switch examples, missing configured inputs, top blocker, receipt-source audit freshness, and `market_execution_enabled=false` |
| `v13_readiness.source_governance_status` | Current source-governance status for the operator preview; while explicit OREE/SCMO receipt rows are missing this is `receipt_gated_for_market_submission` |
| `v13_readiness.source_governance_label` | Human-facing label, currently `receipt-gated for market submission` |
| `v13_readiness.market_submission_receipt_gate_status` | Market-submission-grade receipt gate, currently `blocked_external_access` for credential-gated SCMO/OREE receipt proof |
| `v13_readiness.scmo_credentials_required_for_diploma_mvp` | Always `false` for the current DAM delivery-day recommendation preview |
| `v13_readiness.scmo_credentials_required_for_market_submission_grade_receipts` | `true` only for stronger market-submission/source-readiness receipt proof while SCMO access remains credential-gated |

These fields are intentionally explicit even when the value is negative. A missing gate or bid path should be visible to the UI and thesis narrative instead of being inferred from absent fields.

The receipt-source audit fields are negative/positive source-readiness evidence
only. They expose how many OREE DAM publication receipt source months were
probed, which months were checked, whether a candidate receipt source was found,
and whether a normalized receipt CSV was generated. They do not satisfy the
explicit receipt blocker unless source-backed receipt rows are configured in the
V13 acquisition packet.

SCMO credentials are not required for the diploma MVP. Missing SCMO
username/password/cert/P12 material blocks only market-submission-grade receipt
readiness and any stronger source-readiness claim for market-submittable bids.
For the current operator preview, the honest status is
`receipt-gated for market submission`: public OREE/SCMO probes and
credential-gated source status should be shown as source-governance evidence,
while `market_execution_enabled=false` remains fixed.

## V13 / DT Boundary

The operator strategy list must not enable `decision_transformer` solely because
offline DT preview rows exist. DT/LAVA selection is gated by
`v13_readiness.dt_lava_ready=true`, which currently requires the V13 packet to
clear explicit DAM publication receipts, every required source family, and the
`20` prior/train non-tail-risk material safe-switch example floor for every
tenant/source. While the V13 packet reports `data_acquisition_needed`, a
requested `decision_transformer` strategy falls back to strict similar-day
control and remains a research diagnostic only.

The credentialless academic MVP can still pass its own gates while V13 remains
blocked for receipt readiness: operator preview is allowed, LAVA NPZ smoke and
teacher-contract artifacts can be cited, and the offline challenger packet can
explain why DT/LAVA is not promoted. That is not DT training permission and not
market execution.

## UI Boundary

The operator first viewport should say:

- DAM hourly planning preview.
- DAM delivery-day review for schedule rows, not current-hour dispatch.
- No `ProposedBid`.
- No market submission.
- No IDM recommendation mode.

The schedule dock may show charge/discharge/hold schedule intervals and the
derived DAM BUY/SELL/HOLD `bid_recommendation_preview`, but the first visible
recommendation row must start at 00:00 of the next DAM delivery day relative to
the read-model anchor. It must frame those rows as preview/review intervals,
not dispatch automation, not a `ProposedBid`, and not a market-order payload.
The credentialless MVP packet summarizes those preview rows as
`bid_preview_summary` with BUY/SELL row counts, total preview MWh, and
indicative notional value. That summary is evidence of an operator-facing DAM
delivery-day recommendation shape, not a market-submittable bid.

## Market Rule Basis

- [JSC Market Operator DAM process](https://www.oree.com.ua/index.php/web/215?lang=english): participants may submit, correct, or delete DAM orders for delivery day `D` up to the DAM gate closure at 12:00 on `D-1`.
- [Energy Map DAM explainer](https://energy-map.info/en/chain/20/details/59): DAM is the market segment where electricity is bought and sold on the next day after trading.

## IDM Boundary

IDM/ВДР is future scope for the recommendation engine. It can appear later as read-only observed market context, but it should not be selectable as an active recommendation mode until a separate IDM target, validation split, market-gate model, and `ProposedBid` path exist.

## Relationship to #10 and #11

- #10 remains the future Gold Target Strategy / `ProposedBid` generation slice.
- #11 remains the Bid Gatekeeper observability slice.
- This boundary repair does not close either gap; it prevents the current operator dashboard from implying those gaps are already solved.
