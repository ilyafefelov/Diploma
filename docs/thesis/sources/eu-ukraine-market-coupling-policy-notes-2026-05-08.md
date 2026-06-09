# Source Notes: Ukraine, ENTSO-E, And EU Market-Coupling Context

Purpose: capture official policy and market-structure sources discussed during
the May 2026 AFE/AFL/DFL planning conversation.

Status: include/watch for **external market context** and future deterministic
AFE feature design. These notes do not authorize training on EU-derived signals
until licensing, timezone, currency, market-rule, and temporal-availability
checks pass.

## Sources

| Status | Source | Key thesis relevance |
|---|---|---|
| include | [Energy Community: Ukraine signals its dedication to EU electricity market coupling](https://www.energy-community.org/news/Energy-Community-News/2026/4/8.html) | Ukraine took a legal step toward Electricity Integration Package transposition in April 2026, but secondary legislation and compliance verification remain prerequisites before coupling proceeds. |
| include | [European Commission: 2 years since Ukraine and Moldova synchronised electricity grids with EU](https://energy.ec.europa.eu/news/2-years-ukraine-and-moldova-synchronised-electricity-grids-eu-2024-03-15_en) | Confirms synchronization with the Continental European Network and supports the idea that EU import/export context can matter for Ukrainian market prices. |
| include/watch | [Moldova Ministry of Energy: Moldova joins the Local Implementation Project for coupling electricity markets](https://energie.gov.md/en/content/republic-moldova-joins-local-implementation-project-coupling-electricity-markets?month=2026-04) | Describes the LIP involving Moldova, Ukraine, Romania, Poland, Slovakia, and Hungary for future European day-ahead/intraday market integration. |
| include | [ENTSO-E: Single Day-Ahead Coupling (SDAC)](https://www.entsoe.eu/network_codes/cacm/implementation/sdac/) | Defines SDAC as a pan-European cross-zonal day-ahead market that accounts for cross-border transmission constraints through a common process. |
| include | [IEA: Ukraine's Energy Security and the Coming Winter](https://www.iea.org/reports/ukraines-energy-security-and-the-coming-winter/ukraines-energy-system-under-attack) | Provides 2024 energy-security context for attacks on Ukraine's electricity system, decentralization, cross-border import support, and system resilience needs. |

## Project Interpretation

EU-derived signals are plausible future covariates for Ukrainian DAM
forecasting because Ukraine is synchronized with ENTSO-E and trades electricity
with EU neighbors. However, the current evidence should not treat Ukraine as
fully coupled into SDAC/SIDC for all delivery dates.

Recommended AFE feature status:

| Feature family | Use now? | Why |
|---|---:|---|
| Neighboring EU same-hour realized DAM prices | No | High leakage risk unless publication time is known to be pre-anchor. |
| Lagged neighboring DAM spreads | Later | Plausible and lower leakage after timezone/currency normalization. |
| Cross-border capacity forecasts or auction results | Later | Useful if availability timestamp is pre-anchor and license permits research use. |
| Market-coupling status flag | Yes, as metadata | Helps prevent overclaiming coupled-market semantics. |
| ENTSO-E/OPSD/Ember/Nord Pool direct rows in UA training | No | Domain shift, market rules, licensing, and normalization are unresolved. |

## Required Feature Metadata

Any future external-market AFE feature should carry:

- `source`;
- `license_status`;
- `timezone`;
- `currency`;
- `availability_timestamp`;
- `market_rule_scope`;
- `coupling_status`;
- `training_use_allowed`;
- `claim_boundary`.

## Claim Boundary

These sources support planning for **Automated Feature Engineering (AFE)** and
external market context. They do not change the current implemented claim:
Ukrainian OREE DAM plus Open-Meteo, strict rolling-origin evaluation, frozen
`strict_similar_day` control, no full DFL, no Decision Transformer control, and
no market execution.
