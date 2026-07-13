# Current Goal Boundary V13

## Defensible Goal

Build a DAM/IDM hourly recommendation preview and offline strategy-evidence
system for BESS arbitrage, with DAM/V2+ as the primary evaluated research
result and DFL/DT research lanes gated by V13 source-readiness. This is not
market-submittable DAM/IDM bids, no deployed Decision Transformer control, no full differentiable DFL claim, and
`market_execution_enabled=false` until separate promotion and execution gates are
designed, implemented, and passed.

## Current Evidence

The current V13 acquisition packet remains blocked:

- readiness rows: `ready_rows=0/5`;
- decision: `data_acquisition_needed`;
- missing configured input: `oree_dam_publication_receipts_csv_path`;
- top source blocker: `explicit OREE DAM/IDM source/publication evidence for preview`;
- staged safe-switch support: `77` safe-switch examples across five
  tenant/source pairs, meeting the `20 / 20` prior/train example floor as
  precondition evidence;
- execution boundary: `market_execution_enabled=false`.

This is a valid source-readiness result. It is not a failed model run and not a
reason to promote another selector over the same evidence.

Direct DT Candidate Shadow now answers a narrower engineering question: the repo
can train a HF DecisionTransformer over candidate-index/schedule-family teacher
targets without LAVA promotion. That run is research-shadow evidence only: it
ties the V13 fallback row in the direct packet, remains worse than
strict/oracle, and is exposed only as a manual dashboard preview source. The
separate apples-to-apples DT packet compares against the real V2+ result and
does not beat it (`460.30` UAH DT mean regret versus `174.77` UAH V2+).

A post-defense time-separated DT suite now replaces the mirrored-row smoke as
the strongest DT generalization check. Across 36 runs (two sources, three
rolling protocols, two objectives, and three seeds), zero runs beat V2+. The
decision-aware regret/value objective ties V2+ in 18/18 runs; candidate-index
cross entropy ties in 15/18 and is worse in 3/18. Every run uses nonzero action
targets and return-to-go values with zero train/evaluation content overlap.
This is honest negative research-shadow evidence, not DT or DFL promotion.

The preregistered v1.2 differentiable forecast-to-storage suite now provides a
separate result. Across 72 runs, zero beats V2+ and all remain non-promotable.
The profile-aware differentiable layer executes without surrogate fallback in
all 36 decision-focused runs. Transformer correction has lower strict regret
than a matched MLP in 28/36 comparisons; the forecast-loss transformer improves
the raw schedule in 15/18 runs. This is positive transformer-architecture
evidence and negative V2+ promotion evidence, not full predict-then-bid.

The public OREE path was probed again for June/July 2026 and a 24-row DAM day
was observed. No row-level source publication timestamp or HTTP Last-Modified
metadata was exposed, so HTTP Date and first-seen time remain retrieval-only
evidence. Closing V13 still requires an authenticated source-signed
XMtrade/SCMO response or export accepted by the receipt contract.

## Promotion Rules

DAM/IDM hourly recommendation preview may use observed OREE rows and offline
schedule/value evidence for operator planning. DAM remains the primary evaluated
thesis evidence packet; IDM is a source-backed hourly preview/read-model lane.
Neither must be described as a market order, bid submission, or clearing path.

DT/LAVA remains blocked until every tenant/source has at least `20` prior/train non-tail-risk material safe-switch examples and every required V13 source family, including explicit OREE DAM/IDM source/publication evidence for preview, is ready. Market-submission receipts remain a separate execution-contour requirement.

Research-shadow DT training is allowed only when it stays clearly labeled as
non-promoted, does not use market execution semantics, does not change the
dashboard/API default strategy, and keeps `promotable_v13_permitted_training_rows=0`
until V13 source-readiness passes.

Full differentiable DFL requires a separately gated stack that covers price
prediction, storage optimization, and market-clearing or settlement assumptions.
Regret-weighted calibration and schedule/value ranking are decision-quality
evidence, not a full differentiable controller.

IDM is no longer excluded from the read-model product surface. DAM and IDM can
both be operator-facing hourly recommendation preview lanes; 15-minute IDM bids,
settlement, and market submission remain out of scope.

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

- "DAM/IDM hourly recommendation preview";
- "offline/read-model strategy evidence";
- "V13 acquisition/source-readiness gate";
- "`market_execution_enabled=false`";
- "DAM/V2+ headline evidence";
- "DT/LAVA blocked until the 20-example and source-family gates pass."

Avoid:

- "live DAM/IDM bidding";
- "market-submittable bids";
- "deployed Decision Transformer controller";
- "full differentiable DFL controller";
- "`ProposedBid` output";
- "dashboard/API default strategy switch."
