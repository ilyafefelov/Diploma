import { ref } from 'vue'
import { describe, expect, it } from 'vitest'

import { useOperatorDashboardViewModel } from './useOperatorDashboardViewModel'
import {
  bidPreviewPoint,
  schedulePoint
} from './test-fixtures/operatorDashboardViewModelFixtures'

describe('useOperatorDashboardViewModel', () => {
  it('does not show fake fallback charge or discharge proposals when no market schedule is loaded', () => {
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
        time: 'Delivery window',
        label: 'Preview pending',
        value: 'No schedule loaded'
      })
    ])
    expect(viewModel.timelineSegments.value.map(segment => segment.value)).not.toContain('-60 MW')
    expect(viewModel.timelineSegments.value.map(segment => segment.value)).not.toContain('+80 MW')
  })

  it('keeps right-rail action labels aligned to selected IDM when source-backed preview is blocked', () => {
    const viewModel = useOperatorDashboardViewModel({
      tenants: ref([]),
      selectedTenant: ref(null),
      selectedMarketVenue: ref('IDM'),
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
      baselinePreviewError: ref('Official observed OREE IDM rows are required.'),
      signalPreviewLastLoadedLabel: ref('Loaded 12:00'),
      registryLastLoadedAt: ref(null),
      isMaterializing: ref(false)
    } as never)

    expect(viewModel.batteryStatusLabel.value).toBe('IDM hold preview')
    expect(viewModel.timelineSegments.value[0]?.tooltipBody).toContain('No DAM/IDM hourly schedule has loaded yet')
  })

  it('does not derive gatekeeper action scores from stale general signal preview when selected preview is blocked', () => {
    const viewModel = useOperatorDashboardViewModel({
      tenants: ref([]),
      selectedTenant: ref(null),
      selectedMarketVenue: ref('IDM'),
      signalPreview: ref({
        tenant_id: 'client_003_dnipro_factory',
        labels: ['31 May'],
        label_timestamps: ['2026-05-31T12:00:00Z'],
        market_price: [7100],
        weather_bias: [0],
        weather_sources: ['OREE_DAM_OLD'],
        charge_intent: [0.45],
        regret: [0],
        resolved_location: {
          latitude: 48.46,
          longitude: 35.04,
          timezone: 'Europe/Kyiv'
        }
      }),
      baselinePreview: ref(null),
      operatorRecommendation: ref(null),
      batteryState: ref(null),
      runConfig: ref(null),
      materializeResult: ref(null),
      operatorStatus: ref(null),
      registryError: ref(''),
      weatherError: ref(''),
      signalPreviewError: ref(''),
      baselinePreviewError: ref('Official observed OREE IDM rows are required.'),
      signalPreviewLastLoadedLabel: ref('Loaded 12:00'),
      registryLastLoadedAt: ref(null),
      isMaterializing: ref(false)
    } as never)

    expect(viewModel.gatekeeperActions.value).toEqual([])
    expect(viewModel.latestRecommendedPowerLabel.value).toBe('0.0 MW')
    expect(viewModel.batteryStatusLabel.value).toBe('IDM hold preview')
  })

  it('keeps the schedule dock, right rail action, and headline economics aligned to the selected market preview', () => {
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
      },
      bid_recommendation_preview: [
        bidPreviewPoint('2026-05-19T08:00:00Z', 'HOLD', 'hold', 0, 1500),
        bidPreviewPoint('2026-05-19T09:00:00Z', 'HOLD', 'hold', 0, 1500),
        bidPreviewPoint('2026-05-19T11:00:00Z', 'SELL', 'discharge', 0.25, 4200),
        bidPreviewPoint('2026-05-19T12:00:00Z', 'BUY', 'charge', 0.4, 900),
        bidPreviewPoint('2026-05-19T13:00:00Z', 'HOLD', 'hold', 0, 1500)
      ]
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
    expect(viewModel.timelineSegments.value.map(segment => segment.marketSideLabel)).toEqual(['SELL', 'BUY'])
    expect(viewModel.timelineSegments.value.map(segment => segment.tone)).toEqual(['green', 'orange'])
    expect(viewModel.timelineSegments.value.map(segment => segment.time)).toEqual(['Delivery 19 May, 11:00', 'Delivery 19 May, 12:00'])
    expect(viewModel.timelineSegments.value[0]).toMatchObject({
      value: '+0.25 MW / 0.25 MWh (25% cap)',
      indicativePriceLabel: '4,200 UAH/MWh',
      marketBoundaryLabel: 'No market payload'
    })
    expect(viewModel.timelineSegments.value[1]).toMatchObject({
      value: '-0.40 MW / 0.40 MWh (40% cap)'
    })
    expect(viewModel.batteryAssetLabel.value).toBe('1.00 MWh / 1.00 MW max')
    expect(viewModel.batteryCapacityContextLabel.value).toContain('Battery: 1.00 MWh usable preview / 1.00 MW max')
    expect(viewModel.timelineSegments.value[0]?.tooltipBody).toContain('non-submittable DAM SELL preview')
    expect(viewModel.timelineSegments.value[0]?.tooltipBody).toContain('no ProposedBid')
    expect(viewModel.headlineMetrics.value[0]).toMatchObject({
      label: 'Net plan value',
      value: '100 UAH',
      meta: 'schedule_value_learner_v2_plus'
    })
    expect(viewModel.dispatchModeLabel.value).toBe('Preview only')
  })

  it('surfaces selected shadow actions even when HF power is below the generic DAM display threshold', () => {
    const operatorRecommendation = ref({
      selected_strategy_id: 'hf_live_safe_switch_value_aligned_shadow',
      market_venue: 'DAM',
      target_delivery_window_start: '2026-06-02T00:00:00',
      target_delivery_window_end: '2026-06-03T00:00:00',
      recommendation_schedule: [
        schedulePoint('2026-06-02T00:00:00', 0),
        schedulePoint('2026-06-02T01:00:00', 0),
        schedulePoint('2026-06-02T10:00:00', -0.045),
        schedulePoint('2026-06-02T20:00:00', 0.045)
      ],
      bid_recommendation_preview: [
        bidPreviewPoint('2026-06-02T00:00:00', 'HOLD', 'hold', 0, 4300),
        bidPreviewPoint('2026-06-02T01:00:00', 'HOLD', 'hold', 0, 4300),
        bidPreviewPoint('2026-06-02T10:00:00', 'BUY', 'charge', 0.045, 2953),
        bidPreviewPoint('2026-06-02T20:00:00', 'SELL', 'discharge', 0.045, 9129)
      ],
      economics: {
        total_gross_market_value_uah: 900,
        total_degradation_penalty_uah: 0,
        total_net_value_uah: 900,
        total_throughput_mwh: 0.09
      }
    })

    const viewModel = useOperatorDashboardViewModel({
      tenants: ref([]),
      selectedTenant: ref(null),
      signalPreview: ref(null),
      baselinePreview: ref({
        battery_metrics: {
          capacity_mwh: 0.5,
          max_power_mw: 0.25,
          round_trip_efficiency: 0.9,
          degradation_cost_per_cycle_uah: 1,
          soc_min_fraction: 0.05,
          soc_max_fraction: 0.95
        }
      }),
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

    expect(viewModel.timelineSegments.value.map(segment => segment.label)).toEqual(['Charge', 'Discharge'])
    expect(viewModel.timelineSegments.value.map(segment => segment.marketSideLabel)).toEqual(['BUY', 'SELL'])
    expect(viewModel.timelineSegments.value.map(segment => segment.time)).toEqual([
      'Delivery 2 Jun, 10:00',
      'Delivery 2 Jun, 20:00'
    ])
    expect(viewModel.timelineSegments.value.map(segment => segment.time)).not.toContain('Delivery 2 Jun, 00:00')
    expect(viewModel.batteryStatusLabel.value).toBe('DAM charge preview')
    expect(viewModel.gatekeeperActions.value.find(action => action.label === 'BUY')).toMatchObject({
      active: true,
      score: 87
    })
  })

  it('does not borrow baseline dock cards for a selected blocked shadow preview', () => {
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
        schedulePoint('2026-05-26T08:00:00', 0.4)
      ],
      economics: {
        total_gross_market_value_uah: 10,
        total_degradation_penalty_uah: 1,
        total_net_value_uah: 9,
        total_throughput_mwh: 0.1
      },
      bid_recommendation_preview: [
        bidPreviewPoint('2026-05-26T08:00:00', 'SELL', 'discharge', 0.4, 5000)
      ]
    })
    const operatorRecommendation = ref({
      selected_strategy_id: 'v13_dt_lava_promoted_training',
      target_delivery_window_start: '2026-05-26T00:00:00',
      target_delivery_window_end: '2026-05-27T00:00:00',
      recommendation_schedule: [],
      bid_recommendation_preview: [],
      economics: {
        total_gross_market_value_uah: 0,
        total_degradation_penalty_uah: 0,
        total_net_value_uah: 0,
        total_throughput_mwh: 0
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

    expect(viewModel.timelineSegments.value).toEqual([
      expect.objectContaining({
        time: 'Delivery window',
        label: 'Preview pending',
        value: 'No schedule loaded'
      })
    ])
    expect(viewModel.timelineSegments.value.map(segment => segment.time)).not.toContain('Delivery 26 May, 08:00')
    expect(viewModel.latestRecommendedPowerLabel.value).toBe('0.0 MW')
    expect(viewModel.deliveryWindowLabel.value).toBe('Delivery window: Delivery 26 May, 00:00 -> Delivery 27 May, 00:00')
  })

  it('does not borrow baseline dock cards while a live shadow preview is still loading', () => {
    const viewModel = useOperatorDashboardViewModel({
      tenants: ref([]),
      selectedTenant: ref(null),
      selectedMarketVenue: ref('DAM'),
      selectedTargetDeliveryDate: ref('2026-06-02'),
      signalPreview: ref(null),
      baselinePreview: ref({
        battery_metrics: {
          capacity_mwh: 0.5,
          max_power_mw: 0.25,
          round_trip_efficiency: 0.9,
          degradation_cost_per_cycle_uah: 1,
          soc_min_fraction: 0.05,
          soc_max_fraction: 0.95
        },
        recommendation_schedule: [
          schedulePoint('2026-06-01T20:00:00', 0.25)
        ],
        bid_recommendation_preview: [
          bidPreviewPoint('2026-06-01T20:00:00', 'SELL', 'discharge', 0.25, 9000)
        ],
        economics: {
          total_gross_market_value_uah: 100,
          total_degradation_penalty_uah: 0,
          total_net_value_uah: 100,
          total_throughput_mwh: 0.25
        }
      }),
      operatorRecommendation: ref(null),
      suppressBaselineFallback: ref(true),
      isSelectedRecommendationLoading: ref(true),
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
        time: 'Delivery window',
        label: 'Preview pending',
        value: 'No schedule loaded'
      })
    ])
    expect(viewModel.timelineSegments.value.map(segment => segment.time)).not.toContain('Delivery 1 Jun, 20:00')
    expect(viewModel.batteryStatusLabel.value).toBe('DAM hold preview')
    expect(viewModel.latestRecommendedPowerLabel.value).toBe('0.0 MW')
  })

  it('uses the selected live shadow delivery date when schedule rows are blocked', () => {
    const viewModel = useOperatorDashboardViewModel({
      tenants: ref([]),
      selectedTenant: ref(null),
      selectedMarketVenue: ref('DAM'),
      selectedTargetDeliveryDate: ref('2026-06-04'),
      signalPreview: ref(null),
      baselinePreview: ref({
        battery_metrics: {
          capacity_mwh: 0.5,
          max_power_mw: 0.25,
          round_trip_efficiency: 0.9,
          degradation_cost_per_cycle_uah: 1,
          soc_min_fraction: 0.05,
          soc_max_fraction: 0.95
        },
        target_delivery_window_start: '2026-06-01T00:00:00',
        target_delivery_window_end: '2026-06-02T00:00:00'
      }),
      operatorRecommendation: ref(null),
      suppressBaselineFallback: ref(true),
      isSelectedRecommendationLoading: ref(false),
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

    expect(viewModel.deliveryWindowLabel.value).toBe('Delivery window: Delivery 4 Jun, 00:00 -> Delivery 5 Jun, 00:00')
    expect(viewModel.timelineSegments.value[0]?.time).toBe('Delivery window')
    expect(viewModel.timelineSegments.value[0]).toMatchObject({
      label: 'No trade preview',
      value: 'Source rows missing',
      marketSideLabel: 'BLOCKED',
      indicativePriceLabel: 'no source-backed price rows'
    })
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

  it('does not count suppressed baseline comparator errors as active live HF alerts', () => {
    const viewModel = useOperatorDashboardViewModel({
      tenants: ref([]),
      selectedTenant: ref(null),
      selectedMarketVenue: ref('DAM'),
      selectedTargetDeliveryDate: ref('2026-06-02'),
      signalPreview: ref(null),
      baselinePreview: ref(null),
      operatorRecommendation: ref(null),
      suppressBaselineFallback: ref(true),
      isSelectedRecommendationLoading: ref(false),
      batteryState: ref(null),
      runConfig: ref(null),
      materializeResult: ref(null),
      operatorStatus: ref(null),
      registryError: ref(''),
      weatherError: ref(''),
      signalPreviewError: ref(''),
      baselinePreviewError: ref('point-in-time forecast metadata rejected for nbeatsx_official_v0'),
      signalPreviewLastLoadedLabel: ref('Loaded 12:00'),
      registryLastLoadedAt: ref(null),
      isMaterializing: ref(false)
    } as never)

    expect(viewModel.activeAlertCount.value).toBe(0)
    expect(viewModel.headlineMetrics.value.at(-1)).toMatchObject({
      label: 'Read-model health',
      meta: 'Preview sources loaded'
    })
  })
})
