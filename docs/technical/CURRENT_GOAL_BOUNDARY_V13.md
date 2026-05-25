# Current Goal Boundary V13

## Defensible Goal

Build a DAM delivery-day recommendation preview and offline strategy-evidence
system for BESS arbitrage, with DFL/DT research lanes gated by V13
source-readiness. This is not market-submittable DAM/IDM bids, no deployed Decision Transformer control, no full differentiable DFL claim, and
`market_execution_enabled=false` until separate promotion and execution gates are
designed, implemented, and passed.

## Current Evidence

The current V13 acquisition packet remains blocked:

- readiness rows: `ready_rows=0/5`;
- decision: `data_acquisition_needed`;
- missing configured input: `oree_dam_publication_receipts_csv_path`;
- top source blocker: `explicit DAM publication receipts`;
- staged safe-switch support: `77` safe-switch examples across five
  tenant/source pairs, meeting the `20 / 20` prior/train example floor as
  precondition evidence;
- execution boundary: `market_execution_enabled=false`.

This is a valid source-readiness result. It is not a failed model run and not a
reason to promote another selector over the same evidence.

Direct DT Candidate Shadow now answers a narrower engineering question: the repo
can train a HF DecisionTransformer over candidate-index/schedule-family teacher
targets without LAVA promotion. That run is research-shadow evidence only: it
ties V2+ on the direct packet, remains worse than strict/oracle, and is exposed
only as a manual dashboard preview source.

## Promotion Rules

DAM recommendation preview may use observed DAM rows and offline schedule/value
evidence for operator planning. It must not be described as a market order, bid
submission, or clearing path.

DT/LAVA remains blocked until every tenant/source has at least `20` prior/train non-tail-risk material safe-switch examples and every required V13 source family, including explicit DAM publication receipts, is ready.

Research-shadow DT training is allowed only when it stays clearly labeled as
non-promoted, does not use market execution semantics, does not change the
dashboard/API default strategy, and keeps `promotable_v13_permitted_training_rows=0`
until V13 source-readiness passes.

Full differentiable DFL requires a separately gated stack that covers price
prediction, storage optimization, and market-clearing or settlement assumptions.
Regret-weighted calibration and schedule/value ranking are decision-quality
evidence, not a full differentiable controller.

IDM remains a separate future market lane. DAM and IDM can both be discussed in
the motivation, but the current product boundary is DAM-only recommendation
preview.

## Literature Framing

- Yi et al. 2025 supports predict-then-bid as a research direction, but its
  stronger claim depends on a price prediction layer, storage optimization
  layer, and market-clearing layer.
- Sang et al. supports regret and surrogate-regret framing for ESS arbitrage.
- Decision Transformer literature supports offline sequence-policy research,
  while the DT preference study motivates the current data floor because
  sequence modeling can require more data than Q-learning.

## Required Wording

Use:

- "DAM delivery-day recommendation preview";
- "offline/read-model strategy evidence";
- "V13 acquisition/source-readiness gate";
- "`market_execution_enabled=false`";
- "DT/LAVA blocked until the 20-example and source-family gates pass."

Avoid:

- "live DAM/IDM bidding";
- "market-submittable bids";
- "deployed Decision Transformer controller";
- "full differentiable DFL controller";
- "`ProposedBid` output";
- "dashboard/API default strategy switch."
