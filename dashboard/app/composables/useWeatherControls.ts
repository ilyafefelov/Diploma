import { computed, ref } from 'vue'

import type { OperatorStatus, OperatorFlowStatus } from '~/types/control-plane'

type WeatherRunConfigResponse = {
  tenant_id: string
  run_config: Record<string, unknown>
  resolved_location: {
    latitude: number
    longitude: number
    timezone: string
  }
}

type WeatherMaterializeResponse = {
  tenant_id: string
  selected_assets: string[]
  run_config: Record<string, unknown>
  resolved_location: {
    latitude: number
    longitude: number
    timezone: string
  }
  success: boolean
}

export const useWeatherControls = () => {
  const runConfig = ref<WeatherRunConfigResponse | null>(null)
  const materializeResult = ref<WeatherMaterializeResponse | null>(null)
  const operatorStatus = ref<OperatorStatus | null>(null)
  const isPreparing = ref(false)
  const isMaterializing = ref(false)
  const error = ref('')
  const lastPreparedAt = ref<number | null>(null)
  const lastMaterializedAt = ref<number | null>(null)

  const lastActionLabel = computed(() => {
    if (operatorStatus.value?.updated_at) {
      return new Date(operatorStatus.value.updated_at).toLocaleTimeString('en-GB', {
        hour: '2-digit',
        minute: '2-digit'
      })
    }

    const latestActionAt = Math.max(lastPreparedAt.value || 0, lastMaterializedAt.value || 0)

    if (latestActionAt === 0) {
      return 'No actions yet'
    }

    return new Date(latestActionAt).toLocaleTimeString('en-GB', {
      hour: '2-digit',
      minute: '2-digit'
    })
  })

  const statusLabel = computed(() => {
    if (isMaterializing.value) {
      return 'Materializing weather slice'
    }

    if (isPreparing.value) {
      return 'Preparing Dagster config'
    }

    const persistedStatus = operatorStatus.value?.status
    if (persistedStatus) {
      return mapStatusLabel(persistedStatus)
    }

    if (materializeResult.value?.success) {
      return 'Latest weather slice completed'
    }

    if (runConfig.value) {
      return 'Run config prepared'
    }

    return 'Idle'
  })

  const syncOperatorStatus = async (tenantId: string): Promise<void> => {
    if (!tenantId) {
      operatorStatus.value = null
      runConfig.value = null
      materializeResult.value = null
      error.value = ''
      return
    }

    try {
      operatorStatus.value = await $fetch<OperatorStatus>('/api/control-plane/dashboard/operator-status', {
        query: {
          tenant_id: tenantId,
          flow_type: 'weather_control'
        }
      })

      const payload = operatorStatus.value.payload
      if (isWeatherRunConfigPayload(payload)) {
        runConfig.value = payload
      }

      if (isWeatherMaterializePayload(payload)) {
        materializeResult.value = payload
      }
    } catch (unknownError) {
      operatorStatus.value = null
      runConfig.value = null
      materializeResult.value = null

      const fetchError = unknownError as { statusCode?: number, statusMessage?: string } | Error
      if ('statusCode' in fetchError && fetchError.statusCode === 404) {
        error.value = ''
        return
      }

      if ('statusCode' in fetchError && fetchError.statusCode === 502) {
        error.value = fetchError.statusMessage || 'Unable to sync operator status.'
        return
      }

      error.value = fetchError instanceof Error ? fetchError.message : 'Unable to sync operator status.'
    }
  }

  const prepareRunConfig = async (tenantId: string): Promise<void> => {
    if (!tenantId) {
      return
    }

    isPreparing.value = true
    error.value = ''

    try {
      runConfig.value = await $fetch<WeatherRunConfigResponse>('/api/control-plane/weather/run-config', {
        method: 'POST',
        body: {
          tenant_id: tenantId
        }
      })
      lastPreparedAt.value = Date.now()
      await syncOperatorStatus(tenantId)
    } catch (unknownError) {
      error.value = unknownError instanceof Error ? unknownError.message : 'Unable to prepare weather run config.'
    } finally {
      isPreparing.value = false
    }
  }

  const materializeWeatherAssets = async (tenantId: string, includePriceHistory: boolean): Promise<void> => {
    if (!tenantId) {
      return
    }

    isMaterializing.value = true
    error.value = ''

    try {
      materializeResult.value = await $fetch<WeatherMaterializeResponse>('/api/control-plane/weather/materialize', {
        method: 'POST',
        body: {
          tenant_id: tenantId,
          include_price_history: includePriceHistory
        }
      })
      lastMaterializedAt.value = Date.now()
      await syncOperatorStatus(tenantId)
    } catch (unknownError) {
      error.value = unknownError instanceof Error ? unknownError.message : 'Unable to materialize weather assets.'
    } finally {
      isMaterializing.value = false
    }
  }

  const clearWeatherError = (): void => {
    error.value = ''
  }

  return {
    runConfig,
    materializeResult,
    operatorStatus,
    isPreparing,
    isMaterializing,
    error,
    lastPreparedAt,
    lastMaterializedAt,
    lastActionLabel,
    statusLabel,
    syncOperatorStatus,
    prepareRunConfig,
    materializeWeatherAssets,
    clearWeatherError
  }
}

const mapStatusLabel = (status: OperatorFlowStatus): string => {
  switch (status) {
    case 'idle':
      return 'Idle'
    case 'prepared':
      return 'Run config prepared'
    case 'running':
      return 'Materializing weather slice'
    case 'completed':
      return 'Latest weather slice completed'
    case 'failed':
      return 'Weather slice failed'
  }
}

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return typeof value === 'object' && value !== null
}

const isResolvedLocation = (value: unknown): value is WeatherRunConfigResponse['resolved_location'] => {
  return isRecord(value)
    && typeof value.latitude === 'number'
    && typeof value.longitude === 'number'
    && typeof value.timezone === 'string'
}

const isWeatherRunConfigPayload = (value: unknown): value is WeatherRunConfigResponse => {
  return isRecord(value)
    && typeof value.tenant_id === 'string'
    && isRecord(value.run_config)
    && isResolvedLocation(value.resolved_location)
}

const isWeatherMaterializePayload = (value: unknown): value is WeatherMaterializeResponse => {
  return isRecord(value)
    && typeof value.tenant_id === 'string'
    && Array.isArray(value.selected_assets)
    && value.selected_assets.every(item => typeof item === 'string')
    && isRecord(value.run_config)
    && isResolvedLocation(value.resolved_location)
    && typeof value.success === 'boolean'
}
