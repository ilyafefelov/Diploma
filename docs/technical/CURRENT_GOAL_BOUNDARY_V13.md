# Current Goal Boundary V13

## Defensible Goal

Build a DAM/IDM hourly recommendation preview and offline strategy-evidence
system for BESS arbitrage, with DAM/V2+ as the primary evaluated research
result and DFL/DT research lanes gated by V13 source-readiness. This is not
market-submittable DAM/IDM bids, no deployed Decision Transformer control, no full differentiable DFL claim, and
`market_execution_enabled=false` until separate promotion and execution gates are
designed, implemented, and passed.

## Current Evidence

The official V13 acquisition/training packet remains blocked:

- readiness rows: `ready_rows=0/5`;
- decision: `data_acquisition_needed`;
- missing configured input: `oree_dam_publication_receipts_csv_path`;
- top source blocker: `explicit OREE DAM/IDM source/publication evidence for preview`;
- staged safe-switch support: `77` safe-switch examples across five
  tenant/source pairs, meeting the `20 / 20` prior/train example floor as
  precondition evidence;
- execution boundary: `market_execution_enabled=false`.

This is a valid source-readiness result. It is not a failed model run and not a
reason to promote another selector over the same evidence. The token-backed
ENTSO-E Poland lag-24 ablation used for the v1.3 aligned-DFL experiment does
not close this Ukrainian V13 source-family gate: it remains
`experimental_ablation_only` pending domain-shift validation.

Direct DT Candidate Shadow now answers a narrower engineering question: the repo
can train a HF DecisionTransformer over candidate-index/schedule-family teacher
targets without LAVA promotion. That run is research-shadow evidence only: it
ties the V13 fallback row in the direct packet, remains worse than
strict/oracle, and is exposed only as a manual dashboard preview source. The
separate apples-to-apples DT packet compares against the real V2+ result and
does not beat it (`460.30` UAH DT mean regret versus `174.77` UAH V2+).

The v1.2 temporal DT suite is no longer a DT generalization check. The v1.3
audit found that its tokens were candidate rows within one anchor rather than
time-ordered transitions, and that its state included outcome-derived fields
and the target candidate index. It is therefore retained only as a reproducible
**invalid/non-causal candidate-list diagnostic**, not as positive or negative
evidence about a temporal Decision Transformer policy.

The v1.2 differentiable forecast-to-storage suite remains negative evidence
only for its tested small-data residual corrector: a six-epoch MLP/one-layer
transformer correction with a relaxed terminal-SOC-equality training contract
and a different strict-evaluation terminal policy. It is not evidence against
aligned DFL or temporal DT.

The v1.3 full-history HF candidate-set encoder is a separate frozen negative
result: on the 18-token compatible universe it is worse than its internal V2+
fallback in both calibrated and raw lanes. It is not a temporal DT policy.

The v1.3 aligned DFL experiment supplies a distinct, positive
within-architecture result: on 18 untouched future dates across five profiles,
the warm-started hybrid transformer has mean strict regret 267.0265 UAH versus
284.9300 UAH for the same forecast-loss transformer (-17.9034 UAH; 6.28%).
It is experimental Poland-context evidence, not a V2+ promotion or a DT gate.

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
