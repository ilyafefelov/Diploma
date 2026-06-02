import type {
  DecisionPolicyPreviewPointResponse,
  FutureForecastSeriesResponse
} from '~/types/control-plane'
import type { PolicyForecastContextPoint } from '~/utils/operatorFutureStack'
import { dashboardChartTokens } from '../../charts/dashboardChartCore'
import {
  formatPolicyTooltipValue,
  roundOptionalPrice,
  type SelectedRecommendationChartRow
} from '../../../utils/operatorFutureStackPresentation'
import {
  axisLabelStyle,
  baseTooltip,
  valueAxis,
  type ChartOption,
  type ChartSeries
} from './operatorFutureChartTypes'

export function buildOfficialPolicyChartSeries(
  seriesRows: FutureForecastSeriesResponse[]
): ChartSeries[] {
  return seriesRows.flatMap((series) => {
    const isTft = series.model_family === 'TFT'
    const color = isTft ? dashboardChartTokens.rose : dashboardChartTokens.highlightOnDark
    const baseLine: ChartSeries = {
      type: 'line',
      name: isTft ? `${series.model_name} p50` : series.model_name,
      smooth: true,
      symbol: isTft ? 'diamond' : 'circle',
      symbolSize: 7,
      lineStyle: { width: 3, color },
      itemStyle: { color },
      data: series.points.map(point => Math.round(point.p50_price_uah_mwh ?? point.forecast_price_uah_mwh))
    }

    const quantileLines: ChartSeries[] = []
    if (series.points.some(point => point.p10_price_uah_mwh !== null)) {
      quantileLines.push({
        type: 'line',
        name: `${series.model_name} p10`,
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 1.5, color: dashboardChartTokens.roseMutedOnDark, type: 'dashed' },
        data: series.points.map(point => roundOptionalPrice(point.p10_price_uah_mwh))
      })
    }
    if (series.points.some(point => point.p90_price_uah_mwh !== null)) {
      quantileLines.push({
        type: 'line',
        name: `${series.model_name} p90`,
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 1.5, color: dashboardChartTokens.roseMutedOnDark, type: 'dashed' },
        data: series.points.map(point => roundOptionalPrice(point.p90_price_uah_mwh))
      })
    }

    return [baseLine, ...quantileLines]
  })
}

export function buildSelectedRecommendationChartSeries(
  rows: SelectedRecommendationChartRow[],
  labels: {
    netPower: string
    valueGap: string
    priceContext: string
  }
): ChartSeries[] {
  return [
    {
      type: 'bar',
      name: labels.netPower,
      yAxisIndex: 1,
      data: rows.map(row => row.netPowerMw),
      itemStyle: { color: dashboardChartTokens.secondaryStrongOnDark, borderRadius: [8, 8, 0, 0] }
    },
    {
      type: 'line',
      name: labels.valueGap,
      smooth: true,
      data: rows.map(row => row.valueGapUah),
      lineStyle: { width: 4, color: dashboardChartTokens.warning },
      itemStyle: { color: dashboardChartTokens.warning }
    },
    {
      type: 'line',
      name: labels.priceContext,
      smooth: true,
      data: rows.map(row => row.forecastPriceUahMwh),
      lineStyle: { width: 3, color: dashboardChartTokens.highlightOnDark, type: 'dashed' },
      itemStyle: { color: dashboardChartTokens.highlightOnDark }
    }
  ]
}

export function buildDecisionPolicyChartSeries(
  policyRows: DecisionPolicyPreviewPointResponse[],
  policyForecastContextRows: PolicyForecastContextPoint[]
): ChartSeries[] {
  return [
    {
      type: 'line',
      name: 'Policy value gap',
      smooth: true,
      data: policyRows.map(row => Math.round(row.value_gap_uah)),
      lineStyle: { width: 4, color: dashboardChartTokens.warning },
      itemStyle: { color: dashboardChartTokens.warning }
    },
    {
      type: 'bar',
      name: 'Projected action',
      yAxisIndex: 1,
      data: policyRows.map(row => Number(row.projected_net_power_mw.toFixed(3))),
      itemStyle: { color: dashboardChartTokens.secondarySoftOnDark, borderRadius: [8, 8, 0, 0] }
    },
    {
      type: 'line',
      name: 'NBEATSx state forecast',
      smooth: true,
      data: policyForecastContextRows.map(row => Math.round(row.nbeatsxForecastUahMwh)),
      lineStyle: { width: 2.5, color: dashboardChartTokens.highlightOnDark, type: 'dashed' },
      itemStyle: { color: dashboardChartTokens.highlightOnDark }
    },
    {
      type: 'line',
      name: 'TFT state forecast',
      smooth: true,
      data: policyForecastContextRows.map(row => Math.round(row.tftForecastUahMwh)),
      lineStyle: { width: 2.5, color: dashboardChartTokens.rose, type: 'dashed' },
      itemStyle: { color: dashboardChartTokens.rose }
    }
  ]
}

export function buildPolicyOption(input: {
  isOfficialPolicyMode: boolean
  usesDecisionPolicyPreview: boolean
  policyLabels: string[]
  officialPolicyChartSeries: ChartSeries[]
  decisionPolicyChartSeries: ChartSeries[]
  selectedRecommendationChartSeries: ChartSeries[]
  emptyStateMessage?: string
}): ChartOption {
  const selectedSeries = input.isOfficialPolicyMode
    ? input.officialPolicyChartSeries
    : input.usesDecisionPolicyPreview
      ? input.decisionPolicyChartSeries
      : input.selectedRecommendationChartSeries
  const isEmptyChart = input.policyLabels.length === 0 || selectedSeries.every(series => series.data.length === 0)

  return {
    animationDuration: 500,
    backgroundColor: 'transparent',
    tooltip: baseTooltip((params) => {
      const lines = params.map((item) => {
        const value = formatPolicyTooltipValue(item.seriesName, item.value, input.isOfficialPolicyMode)
        return `${item.marker || ''}${item.seriesName}: ${value}`
      })
      if (input.isOfficialPolicyMode) {
        return [`<strong>${params[0]?.axisValue || 'hour'}</strong>`, ...lines, 'Forecast rows only; no schedule command.'].join('<br/>')
      }
      if (input.usesDecisionPolicyPreview) {
        return [`<strong>${params[0]?.axisValue || 'hour'}</strong>`, ...lines, 'Policy value gap is oracle-normalized diagnostic evidence.'].join('<br/>')
      }
      return [
        `<strong>${params[0]?.axisValue || 'hour'}</strong>`,
        ...lines,
        'Orange = max(0, strict LP/reference value - selected schedule value).',
        'High shortfall means the selected preview is worse than strict for that hour; it is not market execution.'
      ].join('<br/>')
    }),
    legend: { show: false },
    grid: { left: 58, right: 44, top: 44, bottom: 42, containLabel: true },
    xAxis: {
      type: 'category',
      data: input.policyLabels,
      axisLabel: axisLabelStyle
    },
    yAxis: input.isOfficialPolicyMode
      ? valueAxis('UAH/MWh')
      : [
          valueAxis('UAH / UAH/MWh'),
          valueAxis('MW')
        ],
    series: selectedSeries,
    ...(isEmptyChart
      ? {
          graphic: [
            {
              type: 'text',
              left: 'center',
              top: 'middle',
              style: {
                text: input.emptyStateMessage || 'Selected preview has no chartable delivery-hour rows.',
                fill: dashboardChartTokens.legendTextOnDark,
                fontSize: 14,
                fontWeight: 800,
                align: 'center',
                width: 320,
                overflow: 'break'
              }
            }
          ]
        }
      : {})
  }
}
