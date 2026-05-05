import pytest

from smart_arbitrage.dfl.relaxed_dispatch import solve_relaxed_dispatch


def test_relaxed_dispatch_layer_returns_feasible_charge_then_discharge() -> None:
    result = solve_relaxed_dispatch(
        prices_uah_mwh=[100.0, 1000.0],
        starting_soc_fraction=0.5,
        capacity_mwh=1.0,
        max_power_mw=0.25,
        soc_min_fraction=0.05,
        soc_max_fraction=0.95,
        degradation_cost_per_mwh=0.0,
    )

    assert result.charge_mw[0] > result.discharge_mw[0]
    assert result.discharge_mw[1] > result.charge_mw[1]
    assert all(0.0 <= value <= 0.25 + 1e-5 for value in result.charge_mw)
    assert all(0.0 <= value <= 0.25 + 1e-5 for value in result.discharge_mw)
    assert all(0.05 - 1e-5 <= value <= 0.95 + 1e-5 for value in result.soc_fraction)


def test_relaxed_dispatch_rejects_invalid_horizon() -> None:
    with pytest.raises(ValueError, match="prices_uah_mwh must contain at least one price"):
        solve_relaxed_dispatch(
            prices_uah_mwh=[],
            starting_soc_fraction=0.5,
            capacity_mwh=1.0,
            max_power_mw=0.25,
            soc_min_fraction=0.05,
            soc_max_fraction=0.95,
        )
