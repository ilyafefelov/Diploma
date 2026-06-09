# Poland Lag-24 Prior-Only Tail-Risk Veto

This slice tests the next safe improvement after the richer Poland lag-24
near-miss: use Poland-enhanced schedules only when a pre-anchor model predicts
that they are safe, otherwise fall back to frozen Ukrainian-only V2+.

## Claim Boundary

This is still Offline Strategy Promotion evidence only:

- `market_execution_enabled=false`;
- no dashboard/API default switch;
- no live dispatch;
- no European rows in Ukrainian training;
- Poland data remains point-in-time exogenous context;
- final strict LP/oracle scoring is unchanged.

## Method

The input is the tail-risk audit packet:

`data/research_runs/week3_poland_lag24_richer_tail_risk_audit/`.

The prior-only veto trains a small deterministic ridge scorer on earlier
anchors only. It predicts the regret delta of using the Poland-enhanced
calibrated TFT schedule instead of frozen V2+. Current-anchor selection uses
only prior-safe features:

- tenant id;
- candidate family and weight profile;
- TFT quantile/spread metadata;
- forecast peak/trough positions and forecast spread;
- schedule action/power/throughput/degradation deltas versus V2+.

It does not use current-anchor actual prices, current-anchor regret, oracle
value, peak/trough error, or any final-holdout label when selecting the current
row.

The reusable export command is:

```powershell
.\.venv\Scripts\python.exe scripts\materialize_poland_lag24_prior_veto_packet.py `
  --tail-risk-audit-csv data\research_runs\week3_poland_lag24_richer_tail_risk_audit\poland_lag24_tail_risk_rows.csv `
  --dagster-run-id 58e38050-9db1-4f34-9215-bc3e99644f46 `
  --run-slug week3_poland_lag24_prior_tail_risk_veto
```

The same logic is now available as a Dagster asset:

- `dfl_poland_lag24_prior_tail_risk_veto_frame`

Materialization command:

```powershell
docker compose exec -T dagster-webserver uv run dagster asset materialize `
  -m smart_arbitrage.defs `
  --select dfl_poland_lag24_prior_tail_risk_veto_frame `
  -c configs/real_data_official_global_panel_poland_lag24_calibrated_schedule_value_week3.yaml
```

Latest Dagster evidence run: `cb60e2d9-1b52-43b9-bd57-bfa7fa155e7d`.

## Result

The local packet is:

`data/research_runs/week3_poland_lag24_prior_tail_risk_veto/`.

Matched 90-row result:

| Metric | Frozen V2+ | All Poland-enhanced TFT | Prior-only veto |
|---|---:|---:|---:|
| Mean regret | `174.77` UAH | `177.34` UAH | `167.05` UAH |
| Median regret | `67.30` UAH | `39.46` UAH | `55.97` UAH |
| Poland rows selected | n/a | `90 / 90` | `34 / 90` |
| Improvement vs frozen V2+ | n/a | `-1.47%` | `4.41%` |

The veto is a real improvement over frozen V2+ on this 90-row screen, but it is
not a headline replacement because the conservative promotion rule requires at
least `5%` mean-regret improvement plus no median degradation and rolling
robustness evidence. The blocker is `improvement_below_5_percent`.

Coverage boundary:

- current overlap: `18` anchors per tenant, `90` tenant-anchor rows;
- current upstream Poland schedule/value config: `48` forecast anchors total,
  then `18` final validation anchors per tenant;
- current status: `insufficient_for_365_anchor_claim` and
  `insufficient_for_4x18_rolling_windows`;
- a 365-anchor claim requires a larger Poland-enhanced official evidence run
  first, not only re-exporting this selector.

## Larger Evidence Runner

The Poland-enhanced forecast path is now resumable in the same style as the
official global-panel backfill runs. The runner fixes one `generated_at`
timestamp, persists every batch to Postgres, and materializes downstream
calibration/schedule-value/veto assets against the merged persisted rows.

Example full run:

```powershell
.\scripts\run-poland-lag24-calibrated-batches.ps1 `
  -TotalAnchors 365 `
  -BatchSize 2 `
  -AnchorBatchOrder chronological `
  -LocalMode host `
  -NbeatsxMaxSteps 20 `
  -TftMaxEpochs 5 `
  -TftMaxSteps 8 `
  -BatchTimeoutSeconds 10800
```

Resume from a monitor-reported index with the same timestamp:

```powershell
.\scripts\run-poland-lag24-calibrated-batches.ps1 `
  -TotalAnchors 365 `
  -BatchSize 2 `
  -StartAnchorIndex <next_anchor_index> `
  -GeneratedAtIso <same_generated_at_iso> `
  -AnchorBatchOrder chronological `
  -LocalMode host
```

Monitor the persisted raw forecast rows, not only `run.log`:

```powershell
.\scripts\monitor-official-evidence-attempt.ps1 `
  -ManifestPath .tmp_runtime\poland_lag24_calibrated_batches\<run>\attempt_manifest.json `
  -StrategyKind official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark `
  -OutputPath .tmp_runtime\poland_lag24_calibrated_batches\<run>\resume-summary.json
```

This runner still writes Offline Strategy Promotion evidence only. It does not
enable live dispatch, does not switch dashboard/API defaults, and does not admit
European rows as Ukrainian training rows.

Tenant-level effect:

| Tenant | Mean delta vs V2+ | Poland rows selected | Interpretation |
|---|---:|---:|---|
| `client_005_odesa_hotel` | `-17.06` UAH | `9 / 18` | Strongest safe gain. |
| `client_003_dnipro_factory` | `-12.27` UAH | `8 / 18` | Recovers useful Poland schedules. |
| `client_001_kyiv_mall` | `-11.88` UAH | `9 / 18` | Good gain after veto. |
| `client_002_lviv_office` | `+0.56` UAH | `6 / 18` | Almost flat, slight harm. |
| `client_004_kharkiv_hospital` | `+2.07` UAH | `2 / 18` | Veto avoided most but not all tail risk. |

## Why NBEATSx Did Not Automatically Improve

It is reasonable to expect extra Poland features to help NBEATSx/TFT. The
evidence says they do help some decisions, especially TFT median regret.

The reason they did not automatically create a stronger NBEATSx headline is
that forecast training and arbitrage scheduling are different objectives.
NBEATSx is trained to map prior covariates to a price trajectory. The promotion
gate scores a battery schedule under strict LP/oracle regret. A Poland feature
can improve price shape in many hours while still nudging the LP schedule toward
one or two high-cost wrong dispatch decisions. Those rare wrong decisions hurt
mean regret more than many small wins help it.

So the fix is not "add Poland everywhere and trust the model." The fix is:

1. keep Poland features in official NBEATSx/TFT training;
2. calibrate the forecast path;
3. generate feasible schedules;
4. add a prior-only tail-risk veto before selecting the Poland schedule;
5. promote only if the unchanged strict LP/oracle gate passes.

## Next Work

The next valid branch is to turn this 90-row screen into robustness evidence:

1. materialize more Poland-enhanced official forecast/schedule-value rows;
   for a 365-anchor final window, the config needs roughly `365` final anchors
   plus enough prior anchors for calibration/selection;
2. run the same prior-only veto across larger rolling windows;
3. require at least `5%` mean-regret improvement versus frozen V2+;
4. require no median degradation;
5. require rolling robustness;
6. keep `market_execution_enabled=false`.

If the 365-anchor route stays below `5%`, keep V2+ as the thesis headline and
describe the Poland lane as useful but not yet robust enough for replacement.
