import { describe, expect, it } from 'vitest'

import {
  formatForecastWindowLabel,
  sortFutureForecastSeries
} from './operatorFutureStack'

import type { FutureForecastSeriesResponse } from '~/types/control-plane'

const emptySeries = (modelName: string, sourceStatus: string): FutureForecastSeriesResponse => ({
  model_name: modelName,
  model_family: modelName.includes('tft') ? 'TFT' : 'NBEATSx',
  source_status: sourceStatus,
  uncertainty_kind: modelName.includes('tft') ? 'quantile' : 'point',
  mean_regret_uah: null,
  win_rate: null,
  points: []
})

describe('operator future stack display helpers', () => {
  it('formats exact forecast prediction windows for operator headers', () => {
    expect(formatForecastWindowLabel(
      '2026-05-04T18:00:00Z',
      '2026-05-05T17:00:00Z'
    )).toBe('04 May, 21:00 -> 05 May, 20:00')
  })

  it('prioritizes official forecast backend rows over compact fallback rows', () => {
    const ordered = sortFutureForecastSeries([
      emptySeries('tft_silver_v0', 'compact'),
      emptySeries('nbeatsx_official_v0', 'official'),
      emptySeries('nbeatsx_silver_v0', 'compact'),
      emptySeries('tft_official_v0', 'official')
    ])

    expect(ordered.map(series => series.model_name)).toEqual([
      'nbeatsx_official_v0',
      'tft_official_v0',
      'nbeatsx_silver_v0',
      'tft_silver_v0'
    ])
  })
})
