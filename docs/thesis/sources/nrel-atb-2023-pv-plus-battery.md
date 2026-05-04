# NREL ATB 2023: Utility-Scale PV-Plus-Battery

Source URL: https://atb.nrel.gov/electricity/2023/residential_battery_storage/utility-scale_pv-plus-battery

Accessed: 2026-05-04

## Why This Is Stored

This NREL Annual Technology Baseline guide page was used to justify explicit tenant-level battery assumptions in the Level 1 simulator:

- battery energy/power sizing should be explicit, not hidden in a global default;
- round-trip efficiency is a material model input;
- fixed replacement assumptions around year 15 are common in techno-economic battery modeling;
- cycle degradation is a real cost driver even when a simplified model treats battery O&M as fixed.

## Project Usage

The current MVP uses this page as supporting context for:

- `battery_lifetime_years: 15.0` in `simulations/tenants.yml`;
- explicit `round_trip_efficiency` per tenant;
- explicit `battery_max_power_kw` rather than deriving dispatch limits from location;
- documenting that the Level 1 degradation model is a tractable economic proxy, not a full ageing model.

## Citation Note

Use the canonical URL above in the thesis bibliography. If the ATB version changes, cite it as the 2023 NREL Annual Technology Baseline PV-plus-battery page accessed on 2026-05-04.
