# Manual Resolution Of 8 `needs-human` Citation Spot-Checks

Date: 2026-06-08

Primary document: Google Doc `1jjja9ng99O-xCisijMUbPrEM-3UJi_hilwnFJY8nups`, tab `t.yjnmt9u1j0ux`.

## Summary

- Automated audit input: 8 rows marked `needs-human`.
- Manual resolution: 8 reviewed, 0 left as `full-text-needed`.
- Confirmed `wrong-source`: 0.
- Confirmed `contradicted`: 0.
- Confirmed `unsupported`: 0.
- Text edits required: 1 scoped wording/citation-role edit in Table 2.1.

The original automated audit rows remain in `citation_context_support_audit.csv`. This file records the manual follow-up verdicts and the action taken for each flagged row.

## Resolution Table

| Mention | Source | Manual verdict | Evidence checked | Action |
|---:|---:|---|---|---|
| 10 | 10 | supported | Monash metadata/abstract for Li et al. confirms BESS, energy arbitrage, DRL, spot and contingency FCAS bidding, and transformer-based temporal feature extraction; arXiv abstract is also available for the same work. | No thesis text edit. Claim remains high-level market-design context and explicitly says it is not proof of Ukrainian live execution. |
| 27 | 4 | supported after text edit | JSC Market Operator page confirms DAM/IDM operator context and the 2026 tariff for purchase/sale transactions on DAM/IDM. It does not itself prove source-readiness blockers. | Edited Table 2.1 wording so `[3], [4]` support local DAM/IDM/operator context and `[8]` supports market-coupling/source-readiness boundary. |
| 36 | 27 | supported | Official NREL ATB PV-plus-battery page and local source note support PV-plus-battery cost/performance context, round-trip efficiency, replacement assumptions, and cycle degradation as a cost driver. | No thesis text edit. The sentence cites NREL Storage Futures plus NREL ATB together, which matches the combined claim. |
| 39 | 28 | supported | RePEc/Wiley metadata and abstract for Jiang et al. confirm probabilistic electricity price forecasting, temporal fusion transformers, uncertainty/probabilistic forecasting, and Nord Pool/Polish Power Exchange experiments. | No thesis text edit. Wiley DOI page was browser-blocked, but the abstract-level claim is verifiable from indexed metadata. |
| 61 | 45 | supported | OPSD source page confirms an open European power-system data platform with documented public electricity data, including time-series data and spot prices. | No thesis text edit. Source supports the OPSD/data-platform part of the grouped sentence. |
| 64 | 48 | supported | Ember API page confirms open electricity data API and datasets for generation, demand, emissions, carbon intensity, and related energy data. | No thesis text edit. Source supports the Ember/open-data part of the grouped sentence. |
| 69 | 8 | supported | ACER page confirms the Energy Community market-coupling integration plan, NEMO integration into EU day-ahead and intraday market coupling, and ACER review/approval role. | No thesis text edit. Source supports the market-integration part of the grouped sentence. |
| 71 | 50 | supported | IEEE/DOI landing metadata and PowerTech program metadata confirm the paper title `Day-Ahead Zonal Electricity Price Forecasting using 1D-LSTM with Neighbouring Zones Data`. | No thesis text edit. The thesis only claims support for the idea of neighboring-zone/cross-border features, which is title-level and metadata-supported. |

## Google Doc And Local Text Edit

Old Table 2.1 cell:

```text
DAM/IDM hourly preview і source-readiness blockers [3], [4], [8]
```

New Table 2.1 cell:

```text
DAM/IDM local market/operator context [3], [4]; market-coupling/source-readiness boundary [8]
```

Local markdown synchronized in `docs/thesis/chapters/02-literature-review.md`.

## Final Manual Status

- `full-text-needed`: 0.
- Claims weakened only because of paywall/unavailable full text: 0.
- Claims changed due to source-role mismatch: 1.
- Citation numbering impact: none. The same source numbers `[3]`, `[4]`, and `[8]` remain in the same table cell.
