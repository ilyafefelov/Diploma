export interface PublicBessRecord {
  [key: string]: unknown
  preset_id?: string
  label?: string
  timestamp?: string
  delivery_date?: string
  source_url?: string
  source_name?: string
  model_name?: string
  backend_status?: string
  quality_boundary?: string
  point_in_time_status?: string
  score_status?: string
  training_cutoff?: string
  forecast_generated_at?: string
  generated_at?: string
  points?: PublicBessRecord[]
  hourly_schedule?: PublicBessRecord[]
  metrics?: PublicBessRecord
  battery?: PublicBessRecord
  capacity_mwh?: number
}

export interface PublicBessForecastPoint {
  timestamp?: string
  forecast_price_uah_mwh?: unknown
}

export interface PublicBessForecastModel {
  model_name?: string
  label?: string
  backend_status?: string
  quality_boundary?: string
  point_count?: number
  points?: PublicBessForecastPoint[]
}

export interface PublicBessForecastArtifact {
  models?: PublicBessForecastModel[]
  source?: {
    history_row_count?: number
  }
  generated_at?: string
  target_delivery_date?: string
  market_execution_enabled?: boolean
}

export interface PublicBessScoreboardRow {
  model_name?: string
  target_delivery_date?: string
  mae_uah_mwh?: unknown
  rmse_uah_mwh?: unknown
  dispatch_regret_uah?: unknown
  value_capture_ratio?: unknown
  claim_boundary?: string
}

export interface PublicBessScoreboardArtifact {
  rows?: PublicBessScoreboardRow[]
  row_count?: number
  generated_at?: string
  score_status?: string
}

export interface PublicBessIndexArtifact {
  presets?: PublicBessRecord[]
  rows?: PublicBessRecord[]
  models?: PublicBessRecord[]
  metrics?: string[]
  source?: PublicBessRecord
  realized?: PublicBessRecord
  forecast?: PublicBessRecord
  autonomy?: PublicBessRecord
  generated_at?: string
  target_delivery_date?: string
  score_status?: string
  row_count?: number
  claim_boundary?: string
  proposed_bid_status?: string
  market_execution_enabled?: boolean
  methodology?: {
    optimization_grain?: string
    objective?: string
    terminal_soc?: string
    degradation_proxy?: string
  }
}
