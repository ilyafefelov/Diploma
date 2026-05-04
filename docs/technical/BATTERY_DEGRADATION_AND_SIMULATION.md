# Battery Degradation and Tenant Simulator

This note documents the current Level 1 battery simulator used by the FastAPI read models and LP baseline. It is intentionally scoped as an academically defensible MVP model, not a full electrochemical digital twin.

Deep-research update: the current throughput/EFC model remains acceptable as a Level 1 economic proxy, but the thesis should not treat it as battery-physics SOTA. Its strongest role is to make LP decisions degradation-aware while the research benchmark measures realized net value and regret.

## Current Implementation

- Tenant defaults come from `simulations/tenants.yml`, under each tenant `energy_system` block.
- The API resolves `capacity_mwh`, `max_power_mw`, `round_trip_efficiency`, `initial_soc_fraction`, `soc_min_fraction`, `soc_max_fraction`, and degradation economics per tenant.
- Scenario overrides are still accepted by `POST /dashboard/projected-battery-state`, but omitted values no longer use one global demo battery.
- The simulator and LP both enforce hourly granularity, power limits, SOC floor and ceiling, and split round-trip efficiency as `sqrt(round_trip_efficiency)` for charge and discharge.

## Degradation Accounting

The current degradation model is a throughput-based equivalent-full-cycle proxy:

```text
throughput_mwh = abs(feasible_net_power_mw) * interval_hours
efc = throughput_mwh / (2 * capacity_mwh)
degradation_penalty_uah = efc * degradation_cost_per_cycle_uah
```

For the LP baseline, throughput is:

```text
throughput_mwh = (charge_mw + discharge_mw) * interval_hours
```

Per-tenant cycle cost is derived from replacement cost and assumed useful lifetime:

```text
replacement_cost_uah = battery_capex_usd_per_kwh * battery_capacity_kwh * usd_to_uah_rate
lifetime_cycles = battery_lifetime_years * 365 * battery_cycles_per_day
degradation_cost_per_cycle_uah = replacement_cost_uah / lifetime_cycles
```

This is correct for a Level 1 economic LP baseline because the objective needs a tractable marginal wear cost in the same UAH units as market revenue. It is not a claim that physical degradation is purely linear.

## Literature Check

- Hesse et al. model arbitrage dispatch with efficiency losses and battery ageing inside a MILP objective, which supports including degradation directly in dispatch economics: [Energies 2019, 12(6), 999](https://www.mdpi.com/1996-1073/12/6/999).
- Maheshwari et al. show that lithium-ion degradation can be nonlinear in operating conditions, so this MVP proxy should be presented as a first economic approximation, not the final digital-twin ageing model: [Applied Energy 261, 114360](https://dspace.library.uu.nl/handle/1874/409792).
- NREL's Storage Futures input data separates calendar life, cycle life, degradation, RTE, and depth of discharge, which matches the project decision to keep these as explicit tenant assumptions instead of hidden constants: [NREL/TP-5700-78694](https://www.nrel.gov/docs/fy21osti/78694.pdf).
- NREL ATB PV-plus-battery assumptions include fixed battery replacement around year 15, cycle degradation as a real cost driver, and RTE values for grid/PV charging, supporting a 15-year MVP lifetime assumption with explicit RTE: [2023 ATB PV-plus-battery](https://atb.nrel.gov/electricity/2023/residential_battery_storage/utility-scale_pv-plus-battery).
- Kumtepeli, Hesse, et al. warn that depreciation cost is only a proxy for lost future arbitrage revenue in rolling-horizon storage optimization. This is important for the diploma defense: the current formula is useful and testable, but the later DFL layer should evaluate regret/profit, not only accounting depreciation: [arXiv:2403.10617](https://arxiv.org/abs/2403.10617).

## Scope Boundary

Good enough now:

- LP baseline and simulator use the same degradation math.
- Tenant/location read models now persist tenant-specific capacity, power, SOC window, initial SOC, RTE, and cycle-cost assumptions.
- The formula is simple enough to explain and test in an 8-week diploma MVP.

Not yet SOTA:

- No temperature, C-rate, SOC-window, depth-of-discharge stress, or calendar-ageing model is applied.
- No SEI/P2D digital twin state is estimated from inverter telemetry.
- No long-horizon lifetime revenue opportunity cost is learned.

Acceptable next step:

- Keep this as the deterministic LP baseline.
- Add a Silver/Gold battery telemetry asset that estimates SOH/EFC from measured inverter telemetry.
- Use this baseline to compare NBEATSx/TFT forecasts and later DFL regret metrics, while clearly labeling degradation as a throughput/EFC proxy.

## Research Benchmark Implications

The next thesis-grade experiments should keep the current proxy as the deterministic baseline and add sensitivity, not rewrite the physics layer first.

Recommended sensitivity dimensions:

- Battery capex and replacement-cost assumptions.
- Lifetime years and cycles/day assumptions.
- SOC window width and initial SOC source.
- Round-trip efficiency.
- Market Operator per-MWh transaction tariff.
- Date-stamped FX assumptions for USD/EUR cost anchors.

The evaluation table should report both physical and economic outcomes: net UAH value, oracle regret, throughput, EFC, degradation penalty, and feasibility violations. This makes the limitation explicit: the model estimates operational economics, not SEI growth, C-rate stress, calendar ageing, or temperature-dependent capacity fade.

Implemented backend direction:

- Raw 5-minute simulated MQTT telemetry is stored separately from hourly planning state.
- `battery_state_hourly_silver` aggregates latest physical telemetry into hourly SOC/SOH/throughput/EFC snapshots.
- Baseline LP uses a fresh hourly telemetry SOC snapshot when available, then falls back to tenant defaults when telemetry is missing or stale.
