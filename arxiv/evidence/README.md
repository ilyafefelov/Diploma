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
| `lineage/hf_32_day_read_model_audit.json` | 13,483 bytes | `057A63F6536673745E6105B488D862A6AF2C5A1F9D12F6B18E030D48CF6C20DE` |
| `lineage/hf_dt_backbone_robustness_summary.json` | 2,810 bytes | `F38C47FBA391F02C613555040D0FDEAB5FE96C538FBC1DAB1F0C85CF6CA81F28` |
| `lineage/hf_dt_backbone_scorer_summary.json` | 8,812 bytes | `1A6605C47B6C7E52C7B400A080F793465D75A9FB8D031E9AE5002DE5E8C0C647` |
| `lineage/hf_dt_candidate_index_summary.json` | 3,607 bytes | `88DEBABE7C7573F7426F3C5FD6F30BE2755E24C9A0B5282EABD397A94116597F` |
| `lineage/model_lineage_audit.json` | 4,423 bytes | `19FC7A45EB805C3C1D8E986B403E0CEA3328118E7F8DE4B4E78A3345C04AF6FE` |
| `lineage/rf_safe_switch_selected_rows.csv` | 52,909 bytes | `D5DE57B8139D27F39E0F808D5C16C8EBF0BF44B5EC9420CC2D26D053730EBC71` |
| `lineage/rf_safe_switch_summary.json` | 6,274 bytes | `E103FF58DDD75D18D83C65ABAA88734E4D71AEEB9C8997D89E345EAE9410BBBC` |
| `lineage/rf_safe_switch_temporal_replay_summary.json` | 3,196 bytes | `8963979345BB3DF31815DDB60E6D225BB86C9637810745812BB31D24D5D976AD` |
| `lineage/rf_safe_switch_temporal_suite_summary.json` | 18,703 bytes | `6209A0A221808A3176D5D328DEB4F2061D5EDA63C0548645637CC366E44A2E76` |
| `lineage/rf_safe_switch_temporal_suite_rows.csv` | 4,024 bytes | `0F8F6D94A2B1F7A9E4CEE172B008083E6810B99B1AEFD12A9F2A68ABE93AA32D` |
| `lineage/rf_safe_switch_teacher_rows.csv` | 2,125,966 bytes | `1BC2699BE55869D30F8262F32F73AB0FD407F6E02096B987D3DC5F25145F5655` |
| `lineage/v2_plus_rolling_robustness.csv` | 10,036 bytes | `19614B57BBDE6E7074A7C17BC3162425F31AF37ED25FEDA9AA685DB1AC8D3D9F` |

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
