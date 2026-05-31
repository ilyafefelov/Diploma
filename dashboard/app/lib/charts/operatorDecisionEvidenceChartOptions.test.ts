// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

import {
  buildDecisionControlRegretTimelineOption,
  buildDecisionSensitivityOption,
  buildDecisionStrategyEvidenceOption
} from './operatorDecisionEvidenceChartOptions'
import { dashboardChartTokens } from './dashboardChartCore'
import type {
  ControlRegretTimelinePoint,
  OperatorStrategyEvidenceRow,
  SensitivityEvidenceRow
} from '../../utils/operatorDecisionEvidence'

const panelPath = fileURLToPath(new URL('../../components/dashboard/operator/OperatorDecisionEvidencePanel.vue', import.meta.url))
const panelModelPath = fileURLToPath(new URL('../../composables/useOperatorDecisionEvidencePanelModel.ts', import.meta.url))

describe('operator decision evidence chart options', () => {
  it('keeps ECharts option implementation behind a chart module seam', () => {
    const panel = readFileSync(panelPath, 'utf8')
    const panelModel = readFileSync(panelModelPath, 'utf8')

    expect(panel).toContain('useOperatorDecisionEvidencePanelModel')
    expect(panelModel).toContain('operatorDecisionEvidenceChartOptions')
    expect(panel).not.toContain('buildDecisionStrategyEvidenceOption')
    expect(panel).not.toContain('textStyle: { color: \'#f0fbff\' }')
    expect(panel).not.toContain('lineStyle: { width: 3, color: \'#b8ff32\' }')
    expect(panel).not.toContain('itemStyle: { color: \'#53b2ea\'')
  })

  it('builds strategy evidence chart options from shared dashboard chart tokens', () => {
    const rows: OperatorStrategyEvidenceRow[] = [
      {
        modelName: 'strict_similar_day',
        role: 'control',
        meanRegretUah: 310.4,
        winRate: 0.41,
        regretDeltaVsControlUah: 0,
        controlComparisonLabel: 'control'
      },
      {
        modelName: 'schedule_value_learner_v2_plus',
        role: 'forecast_candidate',
        meanRegretUah: 174.7,
        winRate: 1,
        regretDeltaVsControlUah: -135.7,
        controlComparisonLabel: '-136 UAH vs control'
      }
    ]

    const option = buildDecisionStrategyEvidenceOption(rows)

    expect(option.xAxis).toEqual(expect.objectContaining({
      data: ['strict_similar_day', 'schedule_value_learner_v2_plus']
    }))
    expect(option.series).toEqual([
      expect.objectContaining({
        name: 'Mean regret',
        type: 'bar',
        data: [310, 175],
        itemStyle: expect.objectContaining({ color: dashboardChartTokens.secondary })
      }),
      expect.objectContaining({
        name: 'Win rate',
        type: 'line',
        data: [41, 100],
        lineStyle: expect.objectContaining({ color: dashboardChartTokens.highlightOnDark })
      })
    ])
  })

  it('keeps timeline and sensitivity charts visually consistent with dark HUD surfaces', () => {
    const timeline: ControlRegretTimelinePoint[] = [
      {
        anchorLabel: '01 Jan',
        regretUah: 100.2,
        decisionValueUah: 900,
        oracleValueUah: 1000,
        throughputMwh: 0.412
      }
    ]
    const sensitivity: SensitivityEvidenceRow[] = [
      {
        bucket: 'selected V2+',
        rows: 24,
        meanRegretUah: 174.7,
        meanForecastMaeUahMwh: 0,
        meanDispatchSpreadErrorUahMwh: 0
      }
    ]

    const timelineOption = buildDecisionControlRegretTimelineOption(timeline)
    const sensitivityOption = buildDecisionSensitivityOption(sensitivity)
    const timelineXAxis = timelineOption.xAxis as {
      axisLabel: { hideOverlap: boolean, interval: unknown, rotate: number }
    }
    const timelineGrid = timelineOption.grid as { top: number }

    expect(timelineOption.tooltip).toEqual(expect.objectContaining({
      backgroundColor: dashboardChartTokens.tooltipBackgroundDark,
      borderColor: dashboardChartTokens.tooltipBorderOnDark,
      textStyle: { color: dashboardChartTokens.tooltipTextOnDark }
    }))
    expect(timelineXAxis.axisLabel.hideOverlap).toBe(true)
    expect(typeof timelineXAxis.axisLabel.interval).toBe('function')
    expect(timelineXAxis.axisLabel.rotate).toBe(0)
    expect(timelineGrid.top).toBeGreaterThanOrEqual(58)
    expect(timelineOption.series).toEqual([
      expect.objectContaining({
        name: 'Control regret',
        data: [100],
        lineStyle: expect.objectContaining({ color: dashboardChartTokens.rose })
      }),
      expect.objectContaining({
        name: 'Throughput',
        type: 'line',
        data: [0.412],
        areaStyle: expect.objectContaining({ color: dashboardChartTokens.highlightTranslucentOnDark })
      })
    ])
    expect(sensitivityOption.series).toEqual([
      expect.objectContaining({
        name: 'Mean regret (UAH)',
        data: [175],
        itemStyle: expect.objectContaining({ color: dashboardChartTokens.warning })
      }),
      expect.objectContaining({
        name: 'Evidence rows',
        data: [24],
        lineStyle: expect.objectContaining({ color: dashboardChartTokens.highlightOnDark })
      })
    ])
  })
})
