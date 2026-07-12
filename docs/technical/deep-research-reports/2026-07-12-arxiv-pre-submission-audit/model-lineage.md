# Model-lineage forensic report

| Repository lineage | Actual implementation | Verified result | Scientific interpretation |
|---|---|---:|---|
| `runs/dt_v2_plus` | `RandomForestRegressor`; 500 trees, depth 6, leaf minimum 1 | 168.1566 UAH; 4 switches; 86 abstentions | Exact-mirror in-packet diagnostic; not DT and not OOS |
| Early apples-to-apples DT | Hugging Face `DecisionTransformerModel`; context 4, hidden 48, 2 layers/heads | 460.3023 UAH | Mirrored-row pipeline smoke; negative for this objective only |
| Decision-aware HF DT | Same HF backbone with value/regret ranking | 174.7684 UAH; 90/90 fallback | No improvement over V2+ on the packet |
| HF value-aligned scorer | `DecisionTransformerModel` backbone; hidden 64, 2 layers/heads, 170,400 parameters | 158.7121 UAH five-seed mean | Mirrored and threshold-selected in packet; not OOS |
| 32-day HF read-model audit | Loaded HF scorer over a finite schedule-template library | 20/32 nonfallback; mean selected value 1174.29 UAH | Separate readiness/value audit; no realized-regret estimate |
| Custom DT prototype | Causal PyTorch `TransformerEncoder` with return/state/action inputs | No canonical thesis headline result | Real return-conditioned architecture prototype |
| LAVA lane | Tabular/ridge bridge and deterministic NPZ diagnostics | No promoted result | Readiness/prototype smoke, not trained full LAVA |

## Decisive code evidence

- `scripts/materialize_dt_v2_plus_canonical_seed_metrics.py` imports and selects
  `MODEL_KIND_RANDOM_FOREST`, while separately hard-coding the historical model
  label `dt_v2_plus`.
- `src/smart_arbitrage/dfl/regret_aware_v2_plus_selector.py` creates the
  `RandomForestRegressor` with the parameters above.
- `src/smart_arbitrage/dfl/hf_safe_switch_scorer.py` loads a genuine Hugging Face
  `DecisionTransformerModel`; however, the candidate-scorer input construction
  sets actions, rewards, and returns-to-go to zero. It is best described as a DT-
  backbone candidate scorer, not a full return-conditioned DT policy.

## Exact-mirror audit

The RF teacher packet has 360 `train_selection` candidate rows and 360
`final_holdout` rows. After removing identifiers, split fields, timestamps, and
`is_training_row`, all features and targets are identical (maximum numerical
difference 0). The training timestamps are the evaluation timestamps shifted
back one year. The year is not a used model feature, so this does not create an
independent historical sample.

All four RF nonfallback profile-row changes occur on delivery date 2026-04-15.
The five profiles share the same DAM path; therefore "4 switches" does not mean
four independent market episodes. A date-cluster moving-block bootstrap of the
RF-minus-V2+ difference includes zero.

The three nominal RF seeds produce identical selected paths and means. The
stored zero standard deviation and p-value are not valid inferential evidence.

## Reproduction record

The canonical RF materializer was rerun into
`.tmp_runtime/dt_lineage_forensic_2026_07_12`. The aggregate JSON and selected-
row CSV SHA-256 values matched the frozen artifacts byte-for-byte. Seven focused
selector tests passed. The frozen HF checkpoint loaded as
`DecisionTransformerModel` with 170,400 parameters.
