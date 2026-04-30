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