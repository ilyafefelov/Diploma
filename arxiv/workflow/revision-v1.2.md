# Research release v1.2

Date: 2026-07-13

Version 1.2 follows the immutable v1.1 correction. It does not replace the
defended thesis or rewrite either earlier tag.

## Added evidence

1. A 36-run time-separated Hugging Face Decision Transformer suite uses
   nonzero actions and returns-to-go with zero train/evaluation content overlap.
   It has 0 beneficial, 33 tie, and 3 harmful runs versus V2+.
2. A preregistration freezes temporal protocols, seeds, outcomes, comparators,
   and secondary transformer metrics before differentiable training.
3. A 72-run differentiable forecast-to-storage suite trains MLP and transformer
   correctors with forecast-loss and decision-focused objectives.
4. The differentiable layer executes in all 36 decision-focused runs. No run
   beats V2+, but transformer correction beats matched MLP in 28/36 comparisons
   and the forecast-loss transformer improves raw schedules in 15/18 runs.
5. A new OREE public-source audit observes a complete 24-row DAM day but refuses
   to convert retrieval or first-seen time into publication evidence. V13 stays
   externally blocked pending an authenticated source-signed export.

## Release boundary

- V2+ remains the main result and default fallback.
- DT and differentiable suites are research-shadow evidence.
- No full predict-then-bid, market clearing, bidding, or execution claim.
- `promotable_v13_permitted_training_rows=0`.
- `market_execution_enabled=false`.
