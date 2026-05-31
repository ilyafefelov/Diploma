import type { BaselineLpPreview, OperatorStatus, SignalPreview, TenantSummary } from '~/types/control-plane'

export type OperatorExplanationMode = 'mvp' | 'future'

export type OperatorMarketVenue = 'DAM' | 'IDM'

export type OperatorChartHorizon = '6h' | '12h' | '24h' | 'all'

export type OperatorHudTone = 'blue' | 'green' | 'orange' | 'mint' | 'lime'

export interface OperatorNavItem {
  label: string
  icon: string
  active: boolean
  targetId: string
}

export interface OperatorHeadlineMetric {
  label: string
  value: string
  meta: string
  icon: string
  tone: OperatorHudTone
  tooltipTitle: string
  tooltipBody: string
  tooltipFormula: string
}

export interface OperatorMoodChip {
  label: string
  value: string
  tone: Exclude<OperatorHudTone, 'lime'>
}

export interface OperatorMarketRegimeChip {
  label: string
  icon: string
  active: boolean
  tooltipTitle: string
  tooltipBody: string
}

export type OperatorGatekeeperActionLabel = 'BUY' | 'SELL' | 'HOLD'

export interface OperatorGatekeeperAction {
  label: OperatorGatekeeperActionLabel
  score: number
  icon: string
  active: boolean
  tooltipTitle: string
  tooltipBody: string
  tooltipFormula: string
}

export interface OperatorTimelineSegment {
  time: string
  label: 'Charge' | 'Discharge' | 'Hold' | 'Preview pending'
  value: string
  marketSideLabel: 'BUY' | 'SELL' | 'HOLD' | 'PENDING'
  indicativePriceLabel: string
  marketBoundaryLabel: string
  tone: 'blue' | 'green' | 'orange'
  tooltipTitle: string
  tooltipBody: string
}

export interface OperatorMotiveItem {
  label: string
  value: number
  tone: 'blue' | 'green' | 'orange'
  hint: string
}

export interface OperatorWeatherRunConfig {
  tenant_id: string
  run_config: Record<string, unknown>
  resolved_location: {
    latitude: number
    longitude: number
    timezone: string
  }
}

export interface OperatorWeatherMaterializeResult extends OperatorWeatherRunConfig {
  selected_assets: string[]
  success: boolean
}

export interface OperatorDashboardSnapshot {
  tenants: TenantSummary[]
  selectedTenant: TenantSummary | null
  signalPreview: SignalPreview | null
  baselinePreview: BaselineLpPreview | null
  runConfig: OperatorWeatherRunConfig | null
  materializeResult: OperatorWeatherMaterializeResult | null
  operatorStatus: OperatorStatus | null
}
