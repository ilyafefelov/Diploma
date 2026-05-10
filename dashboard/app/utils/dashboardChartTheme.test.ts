import { describe, expect, it } from 'vitest'

import { buildMarketSignalHeroChartOption } from './dashboardChartTheme'
import type { SignalPreview } from '../types/control-plane'

describe('dashboard chart theme', () => {
  it('uses explicit signal timestamps for operator market chart periods', () => {
    const signalPreview: SignalPreview = {
      tenant_id: 'client_003_dnipro_factory',
      labels: ['18:00', '21:00'],
      label_timestamps: ['2026-05-04T15:00:00Z', '2026-05-04T18:00:00Z'],
      timezone: 'Europe/Kyiv',
      market_price: [4200, 3100],
      weather_bias: [50, -20],
      weather_sources: ['OPEN_METEO', 'OPEN_METEO'],
      charge_intent: [0.2, -0.1],
      regret: [500, 300],
      resolved_location: {
        latitude: 48.46,
        longitude: 35.04,
        timezone: 'Europe/Kyiv'
      }
    }

    const option = buildMarketSignalHeroChartOption(signalPreview) as {
      xAxis: { data: string[] }
    }

    expect(option.xAxis.data).toEqual(['04 May\n18:00', '04 May\n21:00'])
  })
})
