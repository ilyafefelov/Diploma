import { computed, ref } from 'vue'

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
  const isPreparing = ref(false)
  const isMaterializing = ref(false)
  const error = ref('')

  const statusLabel = computed(() => {
    if (isMaterializing.value) {
      return 'Materializing weather slice'
    }

    if (isPreparing.value) {
      return 'Preparing Dagster config'
    }

    if (materializeResult.value?.success) {
      return 'Latest weather slice completed'
    }

    if (runConfig.value) {
      return 'Run config prepared'
    }

    return 'Idle'
  })

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
    isPreparing,
    isMaterializing,
    error,
    statusLabel,
    prepareRunConfig,
    materializeWeatherAssets,
    clearWeatherError
  }
}