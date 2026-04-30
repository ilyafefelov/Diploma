export interface TenantSummary {
  tenant_id: string
  name: string | null
  type: string | null
  latitude: number
  longitude: number
  timezone: string
}

export interface SignalPreview {
  tenant_id: string
  labels: string[]
  market_price: number[]
  weather_bias: number[]
  charge_intent: number[]
  regret: number[]
  resolved_location: {
    latitude: number
    longitude: number
    timezone: string
  }
}

export type OperatorFlowType = 'weather_control' | 'signal_preview' | 'baseline_lp' | 'gatekeeper' | 'dispatch_execution'

export type OperatorFlowStatus = 'idle' | 'prepared' | 'running' | 'completed' | 'failed'

export interface OperatorStatus {
  tenant_id: string
  flow_type: OperatorFlowType
  status: OperatorFlowStatus
  updated_at: string
  payload: Record<string, unknown> | null
  last_error: string | null
}