# Temporal Decision Transformer evidence

Date: 2026-07-13

This compact packet preserves the aggregate outputs of the post-defense
time-separated Decision Transformer experiment. Large per-run tensors and
model outputs remain reproducible local research artifacts and are not required
to inspect the reported aggregate.

| File | Purpose | SHA-256 |
|---|---|---|
| `temporal_suite_summary.json` | 36-run aggregate over two sources, three windows, two objectives, and three seeds | `A2A6A7327D92216208D78794B0DC5EC7306E7344BC32F11E14A4B36C453B12FC` |
| `temporal_suite_rows.csv` | One scalar metric row per protocol/objective/seed | `77090AF3034E090EA0E0D806C2F1BA64ED9877C5F4B3F4AF2AC29C9EAEAAAB31` |
| `epoch10_calibrated_window1_rows.csv` | Ten-epoch sensitivity for calibrated window 1 | `63B9450E5295286E8BB14334A8FDF72C32744B5DFF66849434C5705C1C7B28B7` |
| `epoch10_raw_windows1_3_rows.csv` | Ten-epoch cross-entropy sensitivity for raw windows 1 and 3 | `CDE8B016C63BF88A088BBED4652BB49CCE1779037A94DA44D5B835BCEE7EA450` |

Every primary-suite row uses Hugging Face `DecisionTransformerModel`, nonzero
candidate-index action targets, and nonzero regret-based return-to-go values.
Train/evaluation candidate-content overlap is zero. The packet is research
shadow only: `promotable_v13_permitted_training_rows=0`,
`dt_promotion_gate_passed=false`, and `market_execution_enabled=false`.
