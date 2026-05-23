# Operator DAM Timing and Bid Boundary

## Current Contract

The operator recommendation surface is a DAM-only hourly planning preview. It is a read-model contract over FastAPI/Dagster evidence, not a market execution path.

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

`GET /dashboard/operator-recommendation` now exposes explicit timing and claim-boundary metadata:

| Field | Current meaning |
| --- | --- |
| `market_scope` | `dam_hourly_planning_preview` |
| `market_venue` | `DAM` |
| `interval_minutes` | `60` |
| `anchor_timestamp` | As-of forecast/read-model anchor; it is not the first delivery hour |
| `forecast_generated_at` | Persisted forecast generation time when an official forecast-store row drives the preview; otherwise `null` |
| `target_delivery_window_start` / `target_delivery_window_end` | Next DAM delivery-day window covered by the visible hourly schedule |
| `market_execution_enabled` | Always `false` for the current operator surface |
| `read_model_boundary` | `operator_preview_no_market_submission` |
| `market_gate_status` | `not_evaluated_preview_only` |
| `bid_eligibility_status` | `not_applicable_no_proposed_bid` |
| `proposed_bid_status` | `not_emitted_operator_preview` |

These fields are intentionally explicit even when the value is negative. A missing gate or bid path should be visible to the UI and thesis narrative instead of being inferred from absent fields.

## UI Boundary

The operator first viewport should say:

- DAM hourly planning preview.
- DAM delivery-day review for schedule rows, not current-hour dispatch.
- No `ProposedBid`.
- No market submission.
- No IDM recommendation mode.

The schedule dock may show charge/discharge/hold schedule intervals, but the first visible recommendation row must start at 00:00 of the next DAM delivery day relative to the read-model anchor. It must frame those rows as preview/review intervals, not dispatch automation.

## Market Rule Basis

- [JSC Market Operator DAM process](https://www.oree.com.ua/index.php/web/215?lang=english): participants may submit, correct, or delete DAM orders for delivery day `D` up to the DAM gate closure at 12:00 on `D-1`.
- [Energy Map DAM explainer](https://energy-map.info/en/chain/20/details/59): DAM is the market segment where electricity is bought and sold on the next day after trading.

## IDM Boundary

IDM/ВДР is future scope for the recommendation engine. It can appear later as read-only observed market context, but it should not be selectable as an active recommendation mode until a separate IDM target, validation split, market-gate model, and `ProposedBid` path exist.

## Relationship to #10 and #11

- #10 remains the future Gold Target Strategy / `ProposedBid` generation slice.
- #11 remains the Bid Gatekeeper observability slice.
- This boundary repair does not close either gap; it prevents the current operator dashboard from implying those gaps are already solved.
