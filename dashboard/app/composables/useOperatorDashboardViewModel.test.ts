import { ref } from 'vue'
import { describe, expect, it } from 'vitest'

import { useOperatorDashboardViewModel } from './useOperatorDashboardViewModel'

describe('useOperatorDashboardViewModel', () => {
  it('drives the schedule dock and headline economics from the selected operator strategy', () => {
    const baselinePreview = ref({
      battery_metrics: {
        capacity_mwh: 1,
        max_power_mw: 1,
        round_trip_efficiency: 0.9,
        degradation_cost_per_cycle_uah: 1,
        soc_min_fraction: 0.05,
        soc_max_fraction: 0.95
      },
      recommendation_schedule: [
        schedulePoint('2026-05-19T08:00:00Z', 0)
      ],
      economics: {
        total_gross_market_value_uah: 10,
        total_degradation_penalty_uah: 1,
        total_net_value_uah: 9,
        total_throughput_mwh: 0.1
      }
    })

    const operatorRecommendation = ref({
      selected_strategy_id: 'schedule_value_learner_v2_plus',
      recommendation_schedule: [
        schedulePoint('2026-05-19T08:00:00Z', 0),
        schedulePoint('2026-05-19T09:00:00Z', 0),
        schedulePoint('2026-05-19T11:00:00Z', 0.25),
        schedulePoint('2026-05-19T12:00:00Z', -0.4),
        schedulePoint('2026-05-19T13:00:00Z', 0)
      ],
      economics: {
        total_gross_market_value_uah: 120,
        total_degradation_penalty_uah: 20,
        total_net_value_uah: 100,
        total_throughput_mwh: 0.8
      }
    })

    const viewModel = useOperatorDashboardViewModel({
      tenants: ref([]),
      selectedTenant: ref(null),
      signalPreview: ref(null),
      baselinePreview,
      operatorRecommendation,
      batteryState: ref(null),
      runConfig: ref(null),
      materializeResult: ref(null),
      operatorStatus: ref(null),
      registryError: ref(''),
      weatherError: ref(''),
      signalPreviewError: ref(''),
      baselinePreviewError: ref(''),
      signalPreviewLastLoadedLabel: ref('Loaded 12:00'),
      registryLastLoadedAt: ref(null),
      isMaterializing: ref(false)
    } as never)

    expect(viewModel.latestRecommendedPowerLabel.value).toBe('0.0 MW')
    expect(viewModel.timelineSegments.value.map(segment => segment.label)).toEqual(['Discharge', 'Charge'])
    expect(viewModel.timelineSegments.value.map(segment => segment.time)).toEqual(['11:00', '12:00'])
    expect(viewModel.headlineMetrics.value[0]).toMatchObject({
      label: 'Net plan value',
      value: '100 UAH',
      meta: 'schedule_value_learner_v2_plus'
    })
    expect(viewModel.dispatchModeLabel.value).toBe('Preview only')
  })
})

const schedulePoint = (intervalStart: string, recommendedNetPowerMw: number) => ({
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
