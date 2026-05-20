# Poland Lag-24 Tail-Risk Audit

This note explains why the richer Poland lag-24 feature route became a near
miss instead of a promoted Offline Strategy Promotion result.

## Claim Boundary

The audit is research/offline evidence only:

- `market_execution_enabled=false`;
- no dashboard/API default switch;
- no live dispatch;
- no European rows in Ukrainian training;
- Poland data is used only as point-in-time exogenous feature context.

## Evidence Packet

The reusable export script is:

```powershell
$env:SMART_ARBITRAGE_STRATEGY_EVALUATION_DSN='postgresql://smart:arbitrage@localhost:5432/smart_arbitrage'
.\.venv\Scripts\python.exe scripts\materialize_poland_lag24_tail_risk_packet.py `
  --baseline-strict-rows-csv data\research_runs\week3_official_global_panel_schedule_value_v2_plus_comparison\dfl_schedule_value_learner_v2_plus_strict_rows.csv `
  --generated-at-iso 2026-05-20T19:25:20.077585+00:00 `
  --dagster-run-id 58e38050-9db1-4f34-9215-bc3e99644f46 `
  --run-slug week3_poland_lag24_richer_tail_risk_audit
```

It writes:

- `data/research_runs/week3_poland_lag24_richer_tail_risk_audit/poland_lag24_tail_risk_summary.json`;
- `data/research_runs/week3_poland_lag24_richer_tail_risk_audit/poland_lag24_tail_risk_summary.md`;
- `data/research_runs/week3_poland_lag24_richer_tail_risk_audit/poland_lag24_tail_risk_rows.csv`;
- `data/research_runs/week3_poland_lag24_richer_tail_risk_audit/poland_lag24_tail_risk_by_tenant.csv`;
- `data/research_runs/week3_poland_lag24_richer_tail_risk_audit/poland_lag24_tail_risk_top_failures.csv`.

## Result

Matched row comparison, calibrated Ukrainian-only V2+ versus calibrated
Poland-enhanced TFT V2+:

| Metric | Frozen Ukrainian-only V2+ | Poland-enhanced calibrated TFT V2+ |
|---|---:|---:|
| Rows | 90 | 90 |
| Mean regret | `174.77` UAH | `177.34` UAH |
| Median regret | `67.30` UAH | `39.46` UAH |
| Wins/losses/ties versus V2+ | n/a | `48 / 32 / 10` |
| Mean delta versus V2+ | n/a | `+2.58` UAH |

The important signal is mixed:

- Poland-enhanced calibrated TFT improves the median row substantially.
- It wins more rows than it loses (`48` wins versus `32` losses).
- It still fails the strict mean-regret promotion gate because the losses are
  asymmetric and concentrated in high-regret tails.

Seven tail-loss rows explain the blocker. They contribute `2074.75` UAH of
positive regret delta, or `68.72%` of all positive loss delta. The biggest
single failure is Dnipro Factory on `2026-04-15T23:00:00`, where the challenger
adds `517.88` UAH regret versus V2+.

Tenant-level result:

| Tenant | Mean delta vs V2+ | Tail-loss rows | Interpretation |
|---|---:|---:|---|
| `client_004_kharkiv_hospital` | `+34.46` UAH | `3` | Main harmed tenant; tail losses dominate. |
| `client_001_kyiv_mall` | `+1.91` UAH | `1` | Near flat, but one large miss blocks trust. |
| `client_005_odesa_hotel` | `-1.16` UAH | `2` | Slight net help, still has unsafe tails. |
| `client_003_dnipro_factory` | `-9.55` UAH | `1` | Net help despite the largest single miss. |
| `client_002_lviv_office` | `-12.79` UAH | `0` | Cleanest net help. |

## Why This Is Weird But Plausible

Additional Poland data is not useless. The median and win-count results show
that it often helps select lower-regret schedules. The problem is not average
feature availability; the problem is safe selection under uncertainty.

The oracle-only diagnostic shows the opportunity size. If a perfect selector
could use final outcomes to choose V2+ on bad Poland rows and Poland-enhanced
TFT on good rows, matched-row mean regret would be `143.80` UAH. That is
`30.97` UAH better than frozen V2+. This is not admissible promotion evidence,
because it uses final outcomes to decide when Poland helps, but it proves the
candidate set contains useful complementary schedules.

Therefore the route failed because of tail-risk control, not because the
external feature has no decision value.

## Next Work

Do not promote or blindly use the Poland-enhanced schedules yet. The next valid
slice is a prior-only tail-risk veto:

1. use only features known before the anchor;
2. predict whether the Poland-enhanced schedule is in a high-risk tail state;
3. fall back to frozen V2+ unless prior evidence predicts a non-degrading win;
4. keep the final strict LP/oracle gate unchanged.

The accept rule remains: mean regret beats frozen V2+, median regret does not
worsen, rolling robustness remains passing, and `market_execution_enabled=false`.
