# ENTSO-E Poland Governance Ablation Source Capture

Date: 2026-05-17

This source capture records the repo-local evidence and source-governance basis
for the Poland ENTSO-E market-coupling lane.

## Source Basis

| Topic | Captured source |
|---|---|
| ENTSO-E transparency data role | [market-coupling-exogenous-feature-interface-source-capture-2026-05-12.md](market-coupling-exogenous-feature-interface-source-capture-2026-05-12.md) |
| Market-coupling ablation boundary | [market-coupling-ablation-v1-source-capture-2026-05-16.md](market-coupling-ablation-v1-source-capture-2026-05-16.md) |
| Technical route | [DFL_ENTSOE_POLAND_GOVERNANCE_ABLATION.md](../technical/DFL_ENTSOE_POLAND_GOVERNANCE_ABLATION.md) |

## Governance Claims Captured

- ENTSO-E Poland day-ahead prices are considered only as a point-in-time
  exogenous column.
- No European rows enter Ukrainian training.
- Approval requires publication-time evidence, timezone/DST mapping, prior-known
  EUR/UAH FX, licensing, market-rule mapping, and domain-shift validation.
- The current default config is expected to block training until those controls
  are explicitly provided.
- The current V2+ result remains Ukrainian-only evidence.

## Materialized Packet

- Dagster run id: `65c87210-36f3-4491-add7-995fa0214d86`.
- Local packet:
  `data/research_runs/week3_dfl_entsoe_poland_feature_ablation_v1/`.
- Status: `blocked_by_governance`.
- Approved feature columns: none.
- Blocked Poland column: `entsoe_pl_day_ahead_price_uah_mwh`.
- Market-coupled B training runs: `0`.
- Claim boundary: Offline Strategy Promotion only,
  `market_execution_enabled=false`.
