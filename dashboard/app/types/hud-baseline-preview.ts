export interface BaselineCompactItem {
  label: string
  value: string
}

export type BaselineBoundaryItem = BaselineCompactItem

export interface BaselineMetricTooltipItem extends BaselineCompactItem {
  tooltipTitle: string
  tooltipBody: string
  tooltipFormula: string
}

export interface BaselineFeasiblePlanItem extends BaselineMetricTooltipItem {
  note: string
}
