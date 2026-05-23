import { ref } from 'vue'
import { describe, expect, it } from 'vitest'

import { useOperatorDashboardViewModel } from './useOperatorDashboardViewModel'

describe('useOperatorDashboardViewModel', () => {
  it('does not show fake fallback charge or discharge proposals when no DAM schedule is loaded', () => {
    const viewModel = useOperatorDashboardViewModel({
      tenants: ref([]),
      selectedTenant: ref(null),
      signalPreview: ref(null),
      baselinePreview: ref(null),
      operatorRecommendation: ref(null),
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

    expect(viewModel.timelineSegments.value).toEqual([
      expect.objectContaining({
        time: 'DAM delivery',
        label: 'Preview pending',
        value: 'No schedule loaded'
      })
    ])
    expect(viewModel.timelineSegments.value.map(segment => segment.value)).not.toContain('-60 MW')
    expect(viewModel.timelineSegments.value.map(segment => segment.value)).not.toContain('+80 MW')
  })

  it('keeps the schedule dock, right rail action, and headline economics aligned to the selected DAM preview', () => {
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

    expect(viewModel.latestRecommendedPowerLabel.value).toBe('+0.3 MW')
    expect(viewModel.batteryStatusLabel.value).toBe('DAM discharge preview')
    expect(viewModel.gatekeeperActions.value.find(action => action.label === 'SELL')).toMatchObject({
      active: true,
      score: 87
    })
    expect(viewModel.timelineSegments.value.map(segment => segment.label)).toEqual(['Discharge', 'Charge'])
    expect(viewModel.timelineSegments.value.map(segment => segment.tone)).toEqual(['green', 'orange'])
    expect(viewModel.timelineSegments.value.map(segment => segment.time)).toEqual(['DAM 19 May, 11:00', 'DAM 19 May, 12:00'])
    expect(viewModel.headlineMetrics.value[0]).toMatchObject({
      label: 'Net plan value',
      value: '100 UAH',
      meta: 'schedule_value_learner_v2_plus'
    })
    expect(viewModel.dispatchModeLabel.value).toBe('Preview only')
  })

  it('counts read-model gaps in the operator health ribbon', () => {
    const viewModel = useOperatorDashboardViewModel({
      tenants: ref([
        {
          tenant_id: 'tenant_default',
          name: 'Dnipro Manufacturing Plant',
          type: 'industrial',
          latitude: 48.46,
          longitude: 35.04,
          timezone: 'Europe/Kyiv'
        }
      ]),
      selectedTenant: ref(null),
      signalPreview: ref(null),
      baselinePreview: ref(null),
      operatorRecommendation: ref(null),
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
      isMaterializing: ref(false),
      readModelErrorCount: ref(3)
    } as never)

    expect(viewModel.activeAlertCount.value).toBe(3)
    expect(viewModel.headlineMetrics.value.at(-1)).toMatchObject({
      label: 'Read-model health',
      value: '92.4%',
      meta: '3 read-model gap(s)'
    })
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
