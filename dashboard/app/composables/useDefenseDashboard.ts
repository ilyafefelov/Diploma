import { computed, ref, type Ref, watch } from 'vue'

import type {
  DashboardBatteryStateResponse,
  DashboardExogenousSignalsResponse,
  DecisionPolicyPreviewResponse,
  DecisionTransformerTrajectoryResponse,
  DflRelaxedPilotResponse,
  ForecastDispatchSensitivityResponse,
  FutureStackPreviewResponse,
  RealDataBenchmarkResponse,
  SimulatedLiveTradingResponse
} from '~/types/control-plane'
import {
  buildDefenseModelRows,
  buildResearchReadinessRows,
  summarizeDefenseBenchmark
} from '~/utils/defenseDataset'

type DefenseResourceKey
  = | 'benchmark'
    | 'calibratedGate'
    | 'riskGate'
    | 'sensitivity'
    | 'dflPilot'
    | 'dtTrajectories'
    | 'dtPolicyPreview'
    | 'simulatedLiveTrading'
    | 'futureStack'
    | 'exogenousSignals'
    | 'batteryState'

type DefenseErrors = Partial<Record<DefenseResourceKey, string>>

export const useDefenseDashboard = (selectedTenantId: Readonly<Ref<string>>) => {
  const benchmark = ref<RealDataBenchmarkResponse | null>(null)
  const calibratedGate = ref<RealDataBenchmarkResponse | null>(null)
  const riskGate = ref<RealDataBenchmarkResponse | null>(null)
  const sensitivity = ref<ForecastDispatchSensitivityResponse | null>(null)
  const dflPilot = ref<DflRelaxedPilotResponse | null>(null)
  const dtTrajectories = ref<DecisionTransformerTrajectoryResponse | null>(null)
  const dtPolicyPreview = ref<DecisionPolicyPreviewResponse | null>(null)
  const simulatedLiveTrading = ref<SimulatedLiveTradingResponse | null>(null)
  const futureStack = ref<FutureStackPreviewResponse | null>(null)
  const exogenousSignals = ref<DashboardExogenousSignalsResponse | null>(null)
  const batteryState = ref<DashboardBatteryStateResponse | null>(null)
  const errors = ref<DefenseErrors>({})
  const isLoading = ref(false)
  const lastLoadedAt = ref<number | null>(null)

  const benchmarkSummary = computed(() => {
    if (!benchmark.value) {
      return null
    }

    return summarizeDefenseBenchmark(benchmark.value)
  })

  const modelRows = computed(() => {
    if (!benchmark.value) {
      return []
    }

    const extraBenchmarks = [calibratedGate.value, riskGate.value]
      .filter((value): value is RealDataBenchmarkResponse => value !== null)

    return buildDefenseModelRows(benchmark.value, extraBenchmarks)
  })

  const researchReadinessRows = computed(() => buildResearchReadinessRows({
    dfl: dflPilot.value,
    dt: dtTrajectories.value,
    dtPolicy: dtPolicyPreview.value,
    live: simulatedLiveTrading.value
  }))

  const activeErrorCount = computed(() => Object.keys(errors.value).length)

  const lastLoadedLabel = computed(() => {
    if (!lastLoadedAt.value) {
      return 'not loaded'
    }

    return new Date(lastLoadedAt.value).toLocaleTimeString('en-GB', {
      hour: '2-digit',
      minute: '2-digit'
    })
  })

  const loadDefenseDashboard = async (): Promise<void> => {
    if (!selectedTenantId.value) {
      clearData()
      return
    }

    isLoading.value = true
    errors.value = {}

    await Promise.all([
      loadResource('benchmark', benchmark, '/api/control-plane/dashboard/real-data-benchmark'),
      loadResource('calibratedGate', calibratedGate, '/api/control-plane/dashboard/calibrated-ensemble-benchmark'),
      loadResource('riskGate', riskGate, '/api/control-plane/dashboard/risk-adjusted-value-gate'),
      loadResource('sensitivity', sensitivity, '/api/control-plane/dashboard/forecast-dispatch-sensitivity'),
      loadResource('dflPilot', dflPilot, '/api/control-plane/dashboard/dfl-relaxed-pilot', {}, true),
      loadResource(
        'dtTrajectories',
        dtTrajectories,
        '/api/control-plane/dashboard/decision-transformer-trajectories',
        { limit: 120 },
        true
      ),
      loadResource(
        'dtPolicyPreview',
        dtPolicyPreview,
        '/api/control-plane/dashboard/decision-policy-preview',
        { limit: 120 },
        true
      ),
      loadResource(
        'simulatedLiveTrading',
        simulatedLiveTrading,
        '/api/control-plane/dashboard/simulated-live-trading',
        { limit: 120 },
        true
      ),
      loadResource('futureStack', futureStack, '/api/control-plane/dashboard/future-stack-preview', {}, true),
      loadResource('exogenousSignals', exogenousSignals, '/api/control-plane/dashboard/exogenous-signals'),
      loadResource('batteryState', batteryState, '/api/control-plane/dashboard/battery-state')
    ])

    lastLoadedAt.value = Date.now()
    isLoading.value = false
  }

  const loadResource = async <T>(
    key: DefenseResourceKey,
    target: Ref<T | null>,
    endpoint: string,
    extraQuery: Record<string, string | number> = {},
    optional = false
  ): Promise<void> => {
    try {
      const response = await $fetch(endpoint, {
        query: {
          tenant_id: selectedTenantId.value,
          ...extraQuery
        }
      })
      target.value = response as T
    } catch (unknownError) {
      target.value = null
      if (optional) {
        return
      }

      errors.value = {
        ...errors.value,
        [key]: unknownError instanceof Error ? unknownError.message : `Unable to load ${key}.`
      }
    }
  }

  const clearData = (): void => {
    benchmark.value = null
    calibratedGate.value = null
    riskGate.value = null
    sensitivity.value = null
    dflPilot.value = null
    dtTrajectories.value = null
    dtPolicyPreview.value = null
    simulatedLiveTrading.value = null
    futureStack.value = null
    exogenousSignals.value = null
    batteryState.value = null
    errors.value = {}
  }

  watch(selectedTenantId, async () => {
    await loadDefenseDashboard()
  })

  return {
    activeErrorCount,
    batteryState,
    benchmark,
    benchmarkSummary,
    calibratedGate,
    dflPilot,
    dtPolicyPreview,
    dtTrajectories,
    errors,
    exogenousSignals,
    isLoading,
    lastLoadedAt,
    lastLoadedLabel,
    loadDefenseDashboard,
    futureStack,
    modelRows,
    researchReadinessRows,
    riskGate,
    sensitivity,
    simulatedLiveTrading
  }
}
