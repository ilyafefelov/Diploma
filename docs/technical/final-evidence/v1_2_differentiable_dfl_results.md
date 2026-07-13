# Version 1.2 Differentiable DFL and Transformer Results

Date: 2026-07-13

## Experiment

The preregistered suite trains price-correction models on genuinely earlier
rolling windows and evaluates them on later windows with zero train/evaluation
content overlap. It crosses:

- two NBEATSx source lanes;
- three temporal protocols;
- MLP and `TransformerEncoder` price correctors;
- forecast-loss and decision-focused objectives;
- seeds 42, 2026, and 7.

The decision-focused objective backpropagates realized storage value through
strictly convex `cvxpylayers` storage problems using each tenant's capacity,
power, starting SOC, efficiency, SOC bounds, and 842.17 UAH/MWh throughput-
degradation contract. Every output is then rescored by the frozen strict
LP/oracle contour. The suite contains 72 model runs, 90
profile rows and 18 market dates per run. All safety-violation and content-
overlap counts are zero. The differentiable solver was used in all 36
decision-focused runs without a surrogate fallback or non-finite-gradient
guard.

This is a differentiable forecast-to-storage research shadow, not full
predict-then-bid: market clearing, submission, and settlement are absent;
`promotable_v13_permitted_training_rows=0` and
`market_execution_enabled=false`.

## Primary result: V2+ remains the winner

No trained run beats V2+.

| Outcome versus V2+ | Runs |
|---|---:|
| Beneficial | 0 / 72 |
| Tie | 0 / 72 |
| Harmful | 72 / 72 |

Across the four architecture/objective cells, mean regret is 676.05--854.21
UAH, while the matched V2+ mean averaged over the same protocols is 279.67 UAH.
The smallest individual run delta is still harmful: +196.11 UAH versus V2+.
The primary scientific decision therefore does not change: V2+ remains the
main result and fallback.

## Honest positive transformer signal

The transformer does show a consistent architecture-level signal against the
predeclared MLP and raw-forecast baselines.

| Training objective | Model | Mean regret | Mean delta vs raw | Runs better than raw |
|---|---|---:|---:|---:|
| Decision-focused | MLP | 836.39 | +124.56 | 5 / 18 |
| Decision-focused | Transformer | 741.06 | +29.23 | 7 / 18 |
| Forecast loss | MLP | 854.21 | +142.38 | 5 / 18 |
| Forecast loss | Transformer | **676.05** | **-35.78** | **15 / 18** |

In matched architecture comparisons, the transformer has lower strict regret
than the MLP in 28/36 cases and by 136.74 UAH on average. The direction appears
under both objectives: 13/18 matched comparisons for decision-focused training
and 15/18 for forecast-loss training.

The forecast-loss transformer improves mean regret over the raw schedule in
five of six source/window protocol aggregates. Its protocol deltas versus raw
are -31.71, -34.57, and -43.46 UAH for the calibrated source, and -118.29,
-7.20, and +20.54 UAH for the raw source. The single harmful aggregate is raw
source window 3; two of its three seeds improve but one large loss reverses the
mean. This is evidence of a useful but not yet stable transformer signal.

The signal is decision-relevant rather than a universal forecast-accuracy
claim. Across all matched comparisons the transformer lowers regret by 136.74
UAH versus MLP, but mean MAE is 32.79 UAH/MWh higher. Under forecast-loss
training alone it improves both matched regret (-178.16 UAH) and MAE (-24.09
UAH/MWh) versus MLP; under decision-focused training it improves regret
(-95.33 UAH) while MAE is worse (+89.66 UAH/MWh).

## What the DFL objective did and did not achieve

Decision-focused training is technically successful: gradients pass through
the physical storage optimizer, checkpoints are selected on earlier inner
validation, and strict evaluation is feasible. It does not produce the best
transformer result in this packet.

- For MLP, decision-focused training lowers mean regret by 17.82 UAH versus the
  matched forecast-loss model and wins 10/18 comparisons.
- For Transformer, decision-focused training raises mean regret by 65.01 UAH
  versus forecast-loss and wins only 6/18 comparisons.

Thus the positive result belongs to the transformer architecture, especially
the forecast-loss transformer, not to a general claim that end-to-end DFL is
already superior. A likely explanation is the small number of distinct market
dates and the mismatch between the relaxed terminal-SOC training problem and
the broader hand-designed candidate families available to V2+. The
decision-focused loss also has a noisier optimization surface and fewer
independent price paths than the 90 profile-row count suggests.

## Publication wording

Defensible wording:

> In a post-defense time-separated suite, transformer price correction reduced
> strict regret relative to a matched MLP in 28/36 comparisons and the
> forecast-loss transformer improved on the raw schedule in 15/18 runs. It did
> not beat Schedule/Value Learner V2+ in any of 72 preregistered runs. The
> differentiable storage objective executed end to end but did not improve the
> transformer over ordinary forecast-loss training.

This is positive transformer evidence and negative promotion evidence at the
same time. It must not be rewritten as DT control, full predict-then-bid, or a
V2+ replacement.

## Artifacts

- `runs/v1_2_differentiable_dfl/suite_summary.json`
- `runs/v1_2_differentiable_dfl/suite_rows.csv`
- `runs/v1_2_differentiable_dfl/paired_profile_rows.csv`
- `docs/technical/final-evidence/v1_2_differentiable_dfl_preregistration.md`
