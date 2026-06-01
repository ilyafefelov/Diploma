# DAM/IDM Operator Preview Demo Script - 2026-06-01

Use this script for the demo-ready read-model dashboard at:

`http://127.0.0.1:64163/operator`

## Opening Claim

This is a DAM/IDM hourly recommendation preview, not a trading robot. Published
delivery windows use official/source-backed OREE price rows first. Unpublished
delivery windows can use complete NBEATSx/TFT forecast-store rows as scenario
input. Deterministic LP creates feasible schedules, and ML evidence ranks,
explains, or abstains. The system does not emit `ProposedBid`, live IDM bids,
settlement, or market-submission payloads.

## Demo Flow

1. Select `DAM` and `Latest official`.
   - Expected state: official/source price context, selected delivery period
     visible, `24/24` hourly rows.
   - Explain: the model is not re-predicting a published official DAM row.

2. Select `IDM` and `Latest official`.
   - Expected state: IDM hourly preview, official/source-backed context, no
     disabled/no-IDM wording.
   - Explain: IDM is a full hourly recommendation-preview lane, but not live
     intraday bidding.

3. Select `DAM` and `Day +2` / `2026-06-03`.
   - Expected state: `ML forecast` context with `nbeatsx_official_v0`, selected
     period visible, `24/24` coverage.
   - Explain: this is where NBEATSx/TFT are useful: pre-publication scenario
     context before the official row exists.

4. Select `IDM` and `Day +2` / `2026-06-03`.
   - Expected state: `ML forecast` context with `nbeatsx_official_idm_v0`,
     selected period visible, `24/24` coverage.
   - Explain: IDM forecast evidence is venue-aware; it is not DAM copy.

5. Select an unavailable far-future IDM date such as `2030-01-01`.
   - Expected state: source-backed preview blocker, no chart made from stale
     signal rows, no BUY/SELL/HOLD advice.
   - Explain: missing OREE rows or incomplete forecast horizons block output
     instead of substituting demo prices.

6. Toggle chart horizon: `6H`, `12H`, `24H`, `All`.
   - Expected state: visible rows change client-side; API economics remain the
     full returned delivery-window values unless a panel explicitly labels a
     visible-window metric.

## Commission Answer

If asked what useful work the system does when official prices already exist:

> For a published DAM/IDM window, the system should not re-predict the official
> price. Its value is to turn the official/source-backed row into a feasible BESS
> schedule, compare candidate strategies, explain the decision, and block unsafe
> or unsupported output. For unpublished future windows, NBEATSx/TFT provide
> scenario context; V2+/DFL/DT-style evidence can rank, explain, or abstain over
> feasible LP candidates. In all cases, this remains operator preview, not market
> execution.

## Evidence Pointers

- Freeze note:
  `docs/technical/DAM_IDM_DEMO_FREEZE_2026_06_01.md`
- Screenshot set:
  `C:\Users\ilyaf\AppData\Local\Temp\codex-dashboard-hardening-qa-20260601-final`
- Dashboard unit test command:
  `npm -C dashboard run test:unit`
- Dashboard typecheck command:
  `npm -C dashboard run typecheck`
