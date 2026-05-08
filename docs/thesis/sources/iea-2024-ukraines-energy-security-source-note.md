# Source Note: Ukraine's Energy Security And The Coming Winter

Source: [IEA - Ukraine's Energy Security and the Coming Winter](https://www.iea.org/reports/ukraines-energy-security-and-the-coming-winter/ukraines-energy-system-under-attack)

Canonical citation from source: IEA (2024), *Ukraine's Energy Security and the
Coming Winter*, IEA, Paris, https://www.iea.org/reports/ukraines-energy-security-and-the-coming-winter,
Licence: CC BY 4.0.

Local PDF: `iea-2024-ukraines-energy-security-and-coming-winter.pdf`

Status: include

## Thesis Use

This source supports the Ukrainian energy-security context for the thesis. It
does not change the current benchmark scope, but it strengthens the rationale
for:

- using Ukraine-specific OREE DAM evidence first;
- treating European interconnection and import/export capacity as future
  forecast covariates;
- keeping resilience, decentralized generation, rooftop PV plus storage, and
  BESS operator safety in the architecture narrative;
- avoiding overclaims about full market coupling while still acknowledging that
  cross-border trade and ENTSO-E synchronization matter.

## Evidence Points To Cite

Use cautiously and with exact dates:

- The report is from 2024 and focuses on Ukraine's energy security ahead of the
  winter.
- It describes intensified attacks on Ukrainian energy infrastructure in 2024.
- It reports severe damage to generation, transmission, district heating, and
  gas infrastructure.
- It states that interconnection with the main European system made a crucial
  contribution to Ukraine's electricity security.
- It gives cross-border trade context, including the 1.7 GW continental Europe
  to Ukraine/Moldova trade limit reported for the period covered by the report.

## Mapping To This Repo

| Source idea | Project use |
|---|---|
| Ukraine's energy system is under stress and generation capacity has been damaged | Motivation for BESS arbitrage/resilience research, not a live-trading claim |
| Interconnection with the European system contributes to electricity security | Future `external_market_context` AFE feature family |
| Rooftop solar plus storage and decentralized generation are resilience-relevant | Tenant/PV/net-load features and dashboard narrative |
| Cross-border trade limits and emergency support matter | Future cross-border capacity features after availability/licensing checks |

## Claim Boundary

This source supports policy and system-context framing. It does not justify
mixing EU data into Ukrainian DFL training without licensing, timezone,
currency, market-rule, and temporal-availability checks. It also does not imply
market execution, full DFL, Decision Transformer control, or a full digital
twin in the current MVP.

