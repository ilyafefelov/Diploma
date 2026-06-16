export const schedulePoint = (
  intervalStart: string,
  recommendedNetPowerMw: number
) => ({
  step_index: 0,
  interval_start: intervalStart,
  forecast_price_uah_mwh: 1000,
  recommended_net_power_mw: recommendedNetPowerMw,
  projected_soc_before_fraction: 0.5,
  projected_soc_after_fraction: 0.55,
  throughput_mwh: Math.abs(recommendedNetPowerMw),
  degradation_penalty_uah: 0,
  gross_market_value_uah: 0,
  net_value_uah: 0
})

export const bidPreviewPoint = (
  intervalStart: string,
  side: 'BUY' | 'SELL' | 'HOLD',
  operatorAction: 'charge' | 'discharge' | 'hold',
  quantityMw: number,
  indicativeLimitPriceUahMwh: number
) => ({
  step_index: 0,
  interval_start: intervalStart,
  market_venue: 'DAM',
  side,
  operator_action: operatorAction,
  quantity_mw: quantityMw,
  indicative_limit_price_uah_mwh: indicativeLimitPriceUahMwh,
  preview_only: true,
  market_execution_enabled: false,
  market_order_payload_emitted: false,
  proposed_bid_status: 'not_emitted_operator_preview',
  read_model_boundary: 'operator_preview_no_market_submission'
})
