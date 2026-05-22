# DFL / DT Traceability Note - 2026-05-22

This note links the thesis literature claims to the current repo evidence
ladder. It is not a new result packet; it is a claim-boundary index.

| Source basis | Thesis-safe method claim | Repo evidence boundary |
|---|---|---|
| Sang et al., ESS arbitrage DFL, arXiv:2305.00362 | Storage learning should be judged by downstream value/regret, not only forecast MAE. | All V2+/V3/V4/V5/V6/V7 rows are strict LP/oracle regret evidence, not raw forecast promotion. |
| Predict-then-bid storage DFL, arXiv:2505.01551 | The learned model must be evaluated through the market decision layer. | Candidate schedules are feasible rows scored against realized Ukrainian DAM prices; `market_execution_enabled=false` remains fixed. |
| Perturbed/implicit DFL for storage, arXiv:2406.17085 | Differentiable or surrogate optimization can be a training tool, but final validation must use the real decision objective. | Regret-Surrogate V1 and V7 use candidate-value labels as training/screening evidence; final evaluator remains strict LP/oracle scoring. |
| Hugging Face Decision Transformer docs | DT is an offline trajectory model and should not be promoted without strong teacher trajectories. | DT/LAVA remains blocked until V7 creates enough prior-supported material safe-switch labels; no raw hourly BUY/SELL/HOLD imitation is promoted. |

Current evidence interpretation:

- Frozen calibrated Ukrainian-only V2+ remains the thesis headline:
  `174.77` UAH mean regret, `67.30` UAH median regret, `4 / 4` rolling windows.
- V6 is valid negative evidence: feature contract passed, but V6 selected
  `0 / 90` non-V2+ final rows and matched V2+.
- V7 is the next evidence gate. It asks whether the project needs Ukrainian
  context backfill, new feasible schedule candidates, DT readiness, or a stop
  decision for the current candidate space.
- None of these claims imply live dispatch, deployed DT control, EU rows in
  Ukrainian target training, or dashboard/API default switching.
