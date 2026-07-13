# Frozen evidence subset

This directory contains the compact, reviewable evidence subset used to
recompute the article's retrospective aggregates and audit the RF/DT/HF model
lineage. It excludes large Dagster storage and most per-hour payloads while
preserving every paired headline regret/value row, the rolling-window table,
the complete RF teacher and selected-row packets, and compact HF summaries.

## Reproduce

From the repository root:

```powershell
.\.venv\Scripts\python.exe arxiv\evidence\reconstruct_headline.py `
  --input arxiv\evidence\frozen\headline_rows.csv `
  --output-dir .tmp_runtime\arxiv_headline_reconstruction `
  --bootstrap-replicates 20000 `
  --block-length 3 `
  --seed 20260712
```

Run the acceptance test with:

```powershell
.\.venv\Scripts\python.exe -m unittest arxiv.evidence.test_reconstruct_headline -v
.\.venv\Scripts\python.exe -m unittest arxiv.evidence.test_audit_model_lineage -v
```

From an extracted arXiv source bundle, using stock Python and paths relative to
the extraction directory:

```text
python anc/evidence/reconstruct_headline.py --input anc/evidence/frozen/headline_rows.csv --output-dir reconstructed --bootstrap-replicates 20000 --block-length 3 --seed 20260712
python anc/evidence/test_reconstruct_headline.py -v
python anc/evidence/audit_model_lineage.py --lineage-dir anc/evidence/lineage --output reconstructed/model_lineage_audit.json
python anc/evidence/test_audit_model_lineage.py -v
```

The confidence intervals resample circular moving blocks of three consecutive
anchor dates. Each date remains a cluster containing all five configured
profiles; therefore the procedure does not treat the 90 profile-date rows as
90 independent market observations. The summary also reports percentile
interval sensitivity for block lengths 2, 3, 4, and 6 and capacity-normalized
regret using the tracked 0.15--0.50 MWh profile capacities.

## Files and SHA-256

| File | Purpose | SHA-256 |
|---|---|---|
| `frozen/headline_rows.csv` | 720 paired source/role/profile/date rows | `2A3D184EED6ED54459AFB46C646735415E1AE8CE66F5FE30E632895ADA0E2A20` |
| `frozen/headline_summary.json` | headline aggregates, capacity normalization, and block-bootstrap sensitivity | `7EB8F65F2AB4ECD173A60F8BB792CFABC4724FE7B42C09E78221CC7BB9BB3767` |
| `frozen/date_role_summary.csv` | date-cluster mean regret by source and role | `1A9A649809C625CC79F3F1E07D036E42CE06B7FBF946AA34F5BCAEEF37BA4DC5` |
| `frozen/tenant_role_summary.csv` | profile-level absolute and capacity-normalized mean regret | `1EE2EDF33D607105C8E22B8019E9AB96D5B765C56956F5DC3EFBFEB486B637E9` |

## Model-lineage files

| File | Size | SHA-256 |
|---|---:|---|
| `lineage/hf_32_day_read_model_audit.json` | 13,489 bytes | `891227749348AD7EA05B0B0B75A6BDE7467C26E8E17B9722EFEBFAECC7AA9F29` |
| `lineage/hf_dt_backbone_robustness_summary.json` | 2,810 bytes | `F38C47FBA391F02C613555040D0FDEAB5FE96C538FBC1DAB1F0C85CF6CA81F28` |
| `lineage/hf_dt_backbone_scorer_summary.json` | 8,812 bytes | `1A6605C47B6C7E52C7B400A080F793465D75A9FB8D031E9AE5002DE5E8C0C647` |
| `lineage/hf_dt_candidate_index_summary.json` | 3,607 bytes | `88DEBABE7C7573F7426F3C5FD6F30BE2755E24C9A0B5282EABD397A94116597F` |
| `lineage/model_lineage_audit.json` | 4,423 bytes | `19FC7A45EB805C3C1D8E986B403E0CEA3328118E7F8DE4B4E78A3345C04AF6FE` |
| `lineage/rf_safe_switch_selected_rows.csv` | 53,000 bytes | `901709B8D37346AFBB5806DA2CA59104752F08F159C01E3F1B173C0E84038E98` |
| `lineage/rf_safe_switch_summary.json` | 6,274 bytes | `E103FF58DDD75D18D83C65ABAA88734E4D71AEEB9C8997D89E345EAE9410BBBC` |
| `lineage/rf_safe_switch_temporal_replay_summary.json` | 3,196 bytes | `8963979345BB3DF31815DDB60E6D225BB86C9637810745812BB31D24D5D976AD` |
| `lineage/rf_safe_switch_temporal_suite_summary.json` | 18,703 bytes | `6209A0A221808A3176D5D328DEB4F2061D5EDA63C0548645637CC366E44A2E76` |
| `lineage/rf_safe_switch_temporal_suite_rows.csv` | 4,024 bytes | `0F8F6D94A2B1F7A9E4CEE172B008083E6810B99B1AEFD12A9F2A68ABE93AA32D` |
| `lineage/rf_safe_switch_teacher_rows.csv` | 2,126,687 bytes | `C0CC40742D25A3B33D24D473FA00D8FEEFED31BB978126E4B452B35E9B16BB1D` |
| `lineage/v2_plus_rolling_robustness.csv` | 10,036 bytes | `19614B57BBDE6E7074A7C17BC3162425F31AF37ED25FEDA9AA685DB1AC8D3D9F` |
| `lineage/dt_temporal_v2_plus_suite_summary.json` | 1,891 bytes | `A2A6A7327D92216208D78794B0DC5EC7306E7344BC32F11E14A4B36C453B12FC` |
| `lineage/dt_temporal_v2_plus_suite_rows.csv` | 17,977 bytes | `C0E42161A9D58E8EFE8D8F22F3F4A3AC02F848764447632B643536E6AF99245E` |
| `lineage/v1_2_differentiable_dfl_suite_summary.json` | 993 bytes | `8803C9B07B3D97A7D99D8A4F695158533F56B52D04402C2C2C2D75BF101579A9` |
| `lineage/v1_2_differentiable_dfl_suite_rows.csv` | 31,553 bytes | `52782A1EE8504BF0D8181B3FB992085FCD634DC5D474BD3D58748F14E9891193` |
| `lineage/v1_2_differentiable_dfl_paired_profile_rows.csv` | 1,558,839 bytes | `FFF4C7818CDA6F8B6B4DF4AFBE10570F7A657705930256700A4218E4BC40C857` |
| `lineage/v1_2_oree_public_probe_summary.json` | 1,288 bytes | `C0F934FE417920537D5421BC5A5405A0FEFA4CED0E482BC442B2934D6825A0D3` |
| `lineage/v1_3_evidence_audit.json` | 661 bytes | `160B061E905D67C75970C4DA676EA88DDA64369EEAD8344306D5930896DB0BC7` |
| `lineage/v1_3_full_history_hf_ranker_result.json` | 1,729 bytes | `A4168345D592CE2372E6406C81F6D1416E6B6F4AE64786E6786F9EAF8E94CA66` |
| `lineage/v1_3_aligned_dfl_source_gate_result.json` | 1,180 bytes | `D6E72D348A875FE1CD823A887CA413EB649E38AFE1140B6DFBAFD8282E335B35` |
| `lineage/v1_3_aligned_dfl_full_context_result.json` | 1,060 bytes | `3689A038E9F87292FEB1B0DB3D050355F554C61F1B0B45FFB83A6FA3151AE309` |
| `lineage/v1_3_causal_temporal_episode_corpus.json` | 794 bytes | `E5ECAC839DBD2754C2FFB2D0D801040745063C44D59BDE679569F73C1F08B037` |

The lineage audit proves that the historical `dt_v2_plus` artifact is a random
forest trained on exact timestamp-shifted copies of the evaluation packet and
that its four switches occur on one date. The HF frozen result remains a
separate mirrored-packet diagnostic; the 32-day read-model audit is reported
separately and contains no realized-regret estimate.

The post-defense RF temporal suite is a separate corrective artifact. Across 14
protocol rows, two source models, three evaluation windows, five latest-window
thresholds, and three-seed checks for the frozen 20 UAH operating point, every
train/evaluation content-overlap count is zero. No protocol improves on V2+;
three earlier-window protocols increase mean regret, with the largest primary-
seed increase equal to 123.0814 UAH. This is negative retrospective evidence,
not prospective confirmation.

Version 1.2 retains two reproducible model packets. The former 36-run
candidate-list suite is reclassified by the v1.3 lineage audit as an
invalid/non-causal diagnostic: it is neither a temporal trajectory nor causal
DT-policy evidence. The differentiable forecast-to-storage suite contains 72
runs and 6,480 paired profile rows; it is negative only for its exact
small-data residual-corrector implementation with mismatched terminal-SOC
contracts. The OREE public probe is retained to show that 24 available DAM rows
still lack the explicit source publication timestamp required by V13.

Version 1.3 adds the machine-readable correction, a frozen full-history HF
candidate-set encoder result (negative relative to its compatible V2+
fallback), and an aligned full-context DFL result. The latter improves strict
regret by 17.9034 UAH relative to the same forecast-loss transformer on an
18-date future block, but uses experimental Poland context and is not V2+
promotion or permission to train temporal DT.

The v1.3 causal temporal corpus contains 1,825 24-hour episodes and 43,800
rows. It stores point-in-time forecast/context state separately from realized
price, reward, and return-to-go labels. It is preparatory evidence only:
`dt_training_eligible=false` and the independent V13 source-family gate remains
blocked.
