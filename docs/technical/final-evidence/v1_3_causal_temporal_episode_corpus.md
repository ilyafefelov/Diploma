# v1.3 causal temporal episode corpus

Status: **corpus materialized; temporal DT training remains blocked**.

## Construction

The full-context experimental Poland panel was expanded into `1,825` episodes:
one 24-hour, chronologically ordered sequence for every tenant/source/anchor
combination. The corpus has `43,800` transition rows (`1,825 x 24`).

For each episode, strict LP receives only the point-in-time p50 forecast and
the tenant's frozen battery contract. The emitted state contains forecast p10,
p50, p90, Ukrainian lag-24, weather, calendar, Poland lag-24, and the LP SOC
state. Teacher dispatch is an action label. Realized Ukrainian price, reward,
degradation cost, and return-to-go are labels only; `state_actual_price_uah_mwh`
is absent by contract.

The materializer is
`scripts/materialize_causal_temporal_episode_corpus.py`; its pure contract is
`build_causal_temporal_episode_frame` in
`src/smart_arbitrage/decision_transformer/causal_episodes.py`.

## Integrity result

- `episode_count=1,825`;
- `row_count=43,800`;
- every episode has `step_index=0..23` exactly once;
- no state field contains realized price;
- `dt_training_eligible=false` for every row;
- `market_execution_enabled=false` for every row.

This repairs the *data-contract* blocker identified in the legacy DT route: a
candidate list is not treated as a temporal episode and hindsight price cannot
enter the policy state.

## Boundary

This is a causal, offline research corpus — not a trained Decision Transformer,
not V2+ replacement evidence, and not market execution. The official V13 gate
still has `ready_rows=0/5` because authenticated OREE/SCMO source-publication
receipt evidence is absent. Consequently the corpus must not be used to train
or evaluate a temporal DT until that independent source-family gate is passed.
