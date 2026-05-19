import { describe, expect, it } from 'vitest'

import { buildOperatorResearchMetrics } from './operatorResearchMetrics'
import type {
  DashboardBatteryStateResponse,
  DashboardExogenousSignalsResponse,
  DflScheduleValueProductionGateResponse
} from '../types/control-plane'
import type { DefenseModelRow, ResearchReadinessRow } from './defenseDataset'

describe('operator research metrics', () => {
  it('maps live research read models into operator-safe product metrics', () => {
    const modelRows: DefenseModelRow[] = [
      {
        modelName: 'strict_similar_day',
        role: 'control',
        anchorCount: 90,
        meanRegretUah: 1300,
        medianRegretUah: 900,
        meanDecisionValueUah: 2400,
        meanOracleValueUah: 3700,
        winRate: 0.62,
        meanThroughputMwh: 1.4
      },
      {
        modelName: 'tft_silver_v0',
        role: 'forecast_candidate',
        anchorCount: 90,
        meanRegretUah: 1100,
        medianRegretUah: 1200,
        meanDecisionValueUah: 2600,
        meanOracleValueUah: 3700,
        winRate: 0.38,
        meanThroughputMwh: 1.1
      }
    ]
    const readinessRows: ResearchReadinessRow[] = [
      {
        label: 'DFL',
        status: 'pilot',
        metric: '42 UAH relaxed regret',
        boundary: 'not full DFL'
      }
    ]
    const exogenousSignals: DashboardExogenousSignalsResponse = {
      tenant_id: 'client_003_dnipro_factory',
      resolved_location: { latitude: 48.46, longitude: 35.04, timezone: 'Europe/Kyiv' },
      latest_weather: null,
      latest_grid_event: null,
      grid_event_count_24h: 3,
      tenant_region_affected: true,
      national_grid_risk_score: 0.71,
      outage_flag: true,
      saving_request_flag: false,
      solar_shift_hint: false,
      event_source_freshness_hours: 1.5,
      source_urls: ['https://t.me/s/Ukrenergo'],
      fallback_reason: null
    }
    const batteryState: DashboardBatteryStateResponse = {
      tenant_id: 'client_003_dnipro_factory',
      latest_telemetry: {
        tenant_id: 'client_003_dnipro_factory',
        observed_at: '2026-05-05T12:00:00Z',
        current_soc: 0.64,
        soh: 0.96,
        power_mw: 0.02,
        temperature_c: 24,
        source: 'simulated_mqtt',
        source_kind: 'observed'
      },
      hourly_snapshot: null,
      fallback_reason: null
    }
    const offlineStrategyPromotion: DflScheduleValueProductionGateResponse = {
      generated_at: '2026-05-11T02:54:50Z',
      row_count: 2,
      production_promote_count: 2,
      promoted_source_model_names: ['nbeatsx_silver_v0', 'tft_silver_v0'],
      fallback_strategy: 'strict_similar_day_default_fallback',
      market_execution_enabled: false,
      claim_scope: 'dfl_schedule_value_production_gate_offline_strategy_not_market_execution',
      claim_boundary: 'offline_read_model_strategy_evidence_only_not_market_execution',
      academic_scope: 'Offline Strategy Promotion evidence only.',
      rows: []
    }

    const metrics = buildOperatorResearchMetrics({
      modelRows,
      readinessRows,
      offlineStrategyPromotion,
      exogenousSignals,
      batteryState
    })

    expect(metrics).toEqual([
      {
        label: 'Control regret',
        value: '1,300 UAH',
        meta: 'strict similar-day / 62% win rate',
        tone: 'blue',
        tooltipTitle: 'Control regret',
        tooltipBody: 'Mean lost value versus the perfect-foresight oracle when using strict similar-day forecasts. This stays visible as the default comparator.',
        tooltipFormula: 'regret = oracle_value_uah - decision_value_uah'
      },
      {
        label: 'Offline V2+',
        value: '175 UAH',
        meta: '44% vs strict / 4/4 rolling',
        tone: 'purple',
        tooltipTitle: 'Offline Strategy Promotion headline',
        tooltipBody: 'Current thesis headline evidence is Ukrainian-only official global-panel NBEATSx Schedule/Value Learner V2+. The backend gate is shown only as read-model context.',
        tooltipFormula: '2/2 offline rows passed, market execution disabled'
      },
      {
        label: 'TFT portfolio',
        value: '0/4 rolling',
        meta: '24/90 local opportunities',
        tone: 'orange',
        tooltipTitle: 'Closed TFT complementarity test',
        tooltipBody: 'TFT offered local candidate diversity, but the prior-only selector could not exploit it robustly.',
        tooltipFormula: 'portfolio_rows=120380; fallback=90/90'
      },
      {
        label: 'Grid risk',
        value: '71%',
        meta: 'tenant region affected',
        tone: 'orange',
        tooltipTitle: 'Grid event risk',
        tooltipBody: 'Rule-based Ukrenergo signal from recent public grid-event text. It is context, not proven causal price prediction.',
        tooltipFormula: 'risk = weighted(event_count_24h, outage_flag, saving_request_flag, tenant_region_affected)'
      },
      {
        label: 'Telemetry SOC',
        value: '64%',
        meta: 'live telemetry',
        tone: 'mint',
        tooltipTitle: 'Physical battery state',
        tooltipBody: 'Latest telemetry SOC when available, otherwise latest hourly snapshot. This is physical state, separate from projected planning SOC.',
        tooltipFormula: 'display_soc = latest_telemetry.current_soc ?? hourly_snapshot.soc_close'
      },
      {
        label: 'Claim boundary',
        value: 'offline only',
        meta: 'market_execution_enabled=false',
        tone: 'lime',
        tooltipTitle: 'Research claim boundary',
        tooltipBody: 'Keeps V2+, TFT portfolio, and DT/LAVA evidence in offline/read-model scope. Nothing on this screen is live market execution.',
        tooltipFormula: 'boundary = academic_scope / provenance flags from backend read models'
      }
    ])
  })
})
