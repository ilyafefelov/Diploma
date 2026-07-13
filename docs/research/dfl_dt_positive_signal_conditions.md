# When DFL and Decision Transformers can improve BESS decision quality

Date: 2026-07-13

## Bottom line

The primary literature supports a narrower claim than “DFL or a Decision
Transformer should beat V2+.” Decision-focused learning (DFL) can improve the
decisions induced by an imperfect predictor relative to training the same
predictor only for forecast accuracy. A Decision Transformer (DT) can be
competitive in some offline-RL regimes when it is trained on a sufficiently
large and diverse trajectory dataset with real state, action and return-to-go
tokens. Neither literature gives a guarantee that either method will beat a
full-information oracle or a mature hand-engineered selector on a small
retrospective packet.

Within the same objective and feasible set, a strict LP solved using realized
future prices is the full-information optimum. Its regret is zero by definition;
a learned policy cannot validly “beat” it. The learnable opportunity is to reduce
the regret caused by unknown future prices, policy approximation, or candidate
selection.

## What the DFL literature actually establishes

### Smart Predict-then-Optimize (SPO)

Elmachtoub and Grigas define the SPO loss as the decision error caused by
predicted objective coefficients and derive the convex SPO+ surrogate. Their
statistical consistency result holds under stated distributional assumptions;
their experiments show improvements especially when the prediction model is
misspecified. This is a comparison against ordinary predict-then-optimize
training, not a theorem that SPO beats an oracle or every alternative selector.

Source: [Elmachtoub and Grigas, *Smart “Predict, then Optimize”*, Management
Science 68(1), 2022](https://doi.org/10.1287/mnsc.2020.3922).

The broad DFL benchmark reinforces the absence of a universal winner. Across
eleven methods and seven problems, no single DFL method is best everywhere;
some DFL methods are worse than prediction-focused learning on particular
problem classes. The benchmark also shows that relaxing an integer problem can
fail when the relaxation does not preserve the relevant combinatorial
structure, and that DFL training may be roughly two orders of magnitude more
expensive than prediction-focused training in tested instances.

Source: [Mandi et al., *Decision-Focused Learning: Foundations, State of the Art,
Benchmark and Future Opportunities*](https://arxiv.org/abs/2307.13565),
especially Sections 2.2–2.3 and 5.2.3.

### Conditions needed for a credible DFL benefit

1. **An actual predict-then-optimize uncertainty.** The model must predict an
   unknown parameter that changes the downstream decision. If V2+ already
   selects from a nearly saturated finite candidate library, the remaining
   improvement is bounded by its missed-candidate gap.
2. **Matched optimization contracts.** Training and strict evaluation should
   use the same SOC dynamics, efficiencies, degradation objective, terminal-SOC
   convention, and feasible set. A smooth QP used only during training is a
   surrogate; the regularization/perturbation bias must be validated against the
   strict solver.
3. **Useful gradients.** LP solution mappings are piecewise constant and have
   zero gradients almost everywhere. SPO+, quadratic smoothing, or perturbation
   can provide gradients, but each introduces a surrogate and hyperparameters.
4. **Enough independent data.** DFL is still empirical risk minimization. A
   small number of correlated market days does not become a large dataset by
   duplicating each price path across battery profiles.
5. **A fair ablation.** The primary causal comparison is the same architecture,
   features, training rows, validation protocol, and strict evaluator, changing
   only `forecast loss` versus `forecast + decision loss`. Comparison with V2+
   is a separate system-level benchmark.
6. **Temporal generalization.** All loss weights, smoothing strengths,
   thresholds, and early-stopping choices must be selected on prior validation
   dates, followed by an untouched future test period with date-clustered
   uncertainty estimates.

### What the energy-storage DFL papers used

Sang et al. train price predictors with a hybrid of MSE and surrogate regret and
compare them with predictors trained only for price accuracy. They use six years
of hourly PJM price, temperature, and load data. Their downstream storage model
includes explicit physical constraints and a big-M no-simultaneous-charge/
discharge formulation. Their claim is improved profit and lower decision error
relative to the prediction-loss baselines, not superiority to an oracle. A
relevant limitation is that 20% of the data is randomly assigned to test and 20%
of the remainder is randomly assigned to validation; this is weaker than a
strict chronological holdout for a dependent electricity-price series.

Source: [Sang et al., *Electricity Price Prediction for Energy Storage System
Arbitrage: A Decision-focused Approach*, IEEE Transactions on Smart Grid
13(4)](https://doi.org/10.1109/TSG.2022.3166791); [open manuscript](https://arxiv.org/abs/2305.00362).

Yi, Alghumayjan, and Xu use a perturbed differentiable storage optimizer and a
hybrid decision/prediction loss. Their self-scheduling experiment uses NYISO
hourly data from 2017–2020 for training and 2021 for testing, with rolling
24-hour windows: 35,017 training samples and 8,713 test samples. They explicitly
note the smoothing trade-off: too little perturbation gives insufficient
smoothness, while too much moves the solution away from the original optimum.
This data scale and strict future-year split are materially different from an
experiment with only tens of distinct price days.

Source: [Yi, Alghumayjan, and Xu, *Perturbed Decision-Focused Learning for
Modeling Strategic Energy Storage*, IEEE Transactions on Smart Grid
16(3)](https://doi.org/10.1109/TSG.2025.3548009); [open manuscript](https://arxiv.org/abs/2406.17085),
Sections IV-C and V-A.

The later predict-then-bid paper is a different problem from self-scheduling. It
uses a tri-layer pipeline—price prediction, storage optimization, and market
clearing—and differentiates through both decision stages. Its NYISO experiments
train on 2017–2018 and test on 2019, using rolling hourly 24-hour windows and
target decisions derived from ground-truth real-time prices. Reported gains
depend on price-taker or specified price-maker market assumptions. They therefore
do not establish that a read-only schedule selector without a market-clearing
model should obtain the same gains.

Source: [Yi et al., *A Decision-Focused Predict-then-Bid Framework for Strategic
Energy Storage*](https://arxiv.org/abs/2505.01551).

## What the Decision Transformer literature actually establishes

The original DT is a causally masked sequence model trained to predict actions
conditioned on desired return-to-go, past states, and past actions. A transformer
backbone receiving zeroed actions, rewards, and returns-to-go is therefore not a
return-conditioned DT policy; it is more accurately a transformer candidate
scorer.

Source: [Chen et al., *Decision Transformer: Reinforcement Learning via Sequence
Modeling*, NeurIPS 2021](https://proceedings.neurips.cc/paper_files/paper/2021/hash/7f489f642a0ddb10272b5c31057f0663-Abstract.html).

Bhargava et al. find empirically that DT needs more data than Conservative
Q-Learning (CQL) to learn competitive policies. DT is favored in their tests for
sparse rewards, low-quality data, longer horizons, and human-demonstration data;
CQL is favored when high stochasticity is combined with low-quality data. On
Atari, increasing DT data five-fold produced a 2.5-fold average score increase.
These are benchmark-dependent empirical findings, not universal guarantees.

Source: [Bhargava et al., *When should we prefer Decision Transformers for
Offline Reinforcement Learning?*, ICLR 2024](https://openreview.net/forum?id=vpV7fOFQy4).

Return conditioning is not a substitute for behavioral support. The original DT
sometimes extrapolated beyond the maximum recorded return, but did not claim it
would always do so. Later work demonstrates that standard DT can fail to
“stitch” the best segments of suboptimal trajectories; Q-learning DT adds
dynamic-programming return relabeling specifically to address that limitation.

Source: [Yamagata, Khalil, and Santos-Rodriguez, *Q-learning Decision
Transformer*, ICML 2023](https://openreview.net/forum?id=6lETsLXxta).

### Conditions needed for a credible DT benefit in this project

1. **Real trajectories, not candidate rows renamed as trajectories.** Each
   episode needs time-ordered states, feasible charge/discharge actions, rewards,
   and nonzero returns-to-go under one explicit storage contract.
2. **Behavioral and return coverage.** The offline set should contain multiple
   policy qualities and meaningful high-return behavior: strict MPC, V2, V2+,
   stochastic/perturbed MPC, feasible exploratory policies, and near-oracle
   demonstrations generated only from training-period data. Otherwise DT mostly
   imitates the narrow behavior distribution it sees.
3. **Enough distinct market episodes.** Tens of distinct daily price paths are
   below the data regime used in the cited DT and storage-DFL studies. Battery
   profiles sharing the same DAM price path are correlated outcomes, not new
   market trajectories.
4. **A sequential reason to use attention.** DT is most defensible when history,
   long-horizon credit assignment, or partial observability matters. If the task
   is only to choose one member of a finite daily candidate set from complete
   day-ahead inputs, a set/sequence transformer ranker is a cleaner model and
   should not be labeled a DT policy.
5. **Appropriate baselines.** Compare DT with behavior cloning, a same-capacity
   MLP/recurrent model, and an offline-RL baseline such as CQL where the action
   representation permits it. V2+ and the full-information LP remain separate
   operational and oracle references.
6. **Prospective return target selection.** Desired return and safe-switch
   thresholds must be frozen from prior validation data. Selecting them on the
   evaluation packet invalidates the out-of-sample value claim.

## Most defensible route to a positive result

The literature suggests prioritizing an aligned DFL experiment before a full DT
policy:

1. Materialize substantially more distinct prior hourly OREE price history and
   freeze a later, untouched temporal test block.
2. Use one shared forecast architecture and data pipeline for two arms:
   prediction loss versus a preregistered hybrid prediction/decision loss.
3. Make the differentiable training problem and strict evaluator agree on the
   physical/economic contract; validate every smoothing approximation.
4. Report whether DFL improves the identical prediction-loss model. Then report
   both against V2+ and the full-information oracle as separate comparisons.
5. In parallel, train the promising HF architecture as an honestly named
   **value-aligned transformer candidate ranker** on the full prior candidate
   history, with calibrated fallback to V2+. This can test the transformer
   architectural signal without pretending it is already a DT policy.
6. Attempt a genuine DT only after assembling a policy-mixture trajectory corpus
   with adequate action and return coverage. Preregister the episode definition,
   return construction, context length, comparator set, and temporal holdout.

Positive findings under this design would be academically useful and close to
the original thesis direction without retrofitting the result. A negative DT
result would still be compatible with the literature: the storage problem may be
better expressed as differentiable constrained optimization or safe candidate
ranking than as return-conditioned offline RL.
