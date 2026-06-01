# Regret-Aware V2+ Selector Shadow

Run slug: `dt_v2_plus_canonical_seed_42`

Candidate-level random-forest residual selector over point-in-time DT-shadow teacher-row features with explicit V2+ abstention.

## Result

| Metric | Value |
|---|---:|
| Selector mean regret | `168.16` UAH |
| V2+ mean regret | `174.77` UAH |
| Selector minus V2+ regret | `-6.61` UAH |
| Non-V2+ switches | `3` |
| V2+ abstentions | `87` |

## Training

- Loss: `random_forest_regret_delta_vs_v2_plus`.
- Model kind: `random_forest`.
- Feature set: `expanded_prior_context_v1`.
- Sample weights: `1 + abs(regret_delta_vs_v2_plus_uah) / 100`.
- Train weighted RMSE: `309.01` UAH.
- Minimum predicted improvement for a switch: `50.00` UAH.

## Boundary

- Research-shadow only; no out-of-sample promotion claim.
- Explicit abstention falls back to V2+ when the signal is weak.
- `market_execution_enabled=false`; no DT/LAVA promotion and no market-submittable bid.
