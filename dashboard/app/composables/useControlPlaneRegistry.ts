import { computed, ref } from 'vue'

import type { TenantSummary } from '~/types/control-plane'

const sortTenants = (tenants: TenantSummary[]): TenantSummary[] => {
  return [...tenants].sort((leftTenant, rightTenant) => {
    const leftName = leftTenant.name || leftTenant.tenant_id
    const rightName = rightTenant.name || rightTenant.tenant_id
    return leftName.localeCompare(rightName)
  })
}

export const useControlPlaneRegistry = () => {
  const tenants = ref<TenantSummary[]>([])
  const selectedTenantId = ref('')
  const isLoading = ref(false)
  const error = ref('')
  const lastLoadedAt = ref<number | null>(null)
  let refreshTimer: ReturnType<typeof setInterval> | null = null

  const selectedTenant = computed(() => {
    return tenants.value.find((tenant) => tenant.tenant_id === selectedTenantId.value) || null
  })

  const loadTenants = async (): Promise<void> => {
    isLoading.value = true
    error.value = ''

    try {
      const response = await $fetch<TenantSummary[]>('/api/control-plane/tenants')
      tenants.value = sortTenants(response)
      lastLoadedAt.value = Date.now()

      const tenantStillExists = tenants.value.some((tenant) => tenant.tenant_id === selectedTenantId.value)
      if (!tenantStillExists) {
        selectedTenantId.value = tenants.value[0]?.tenant_id || ''
      }
    } catch (unknownError) {
      const message = unknownError instanceof Error
        ? unknownError.message
        : 'Unable to load the tenant registry.'
      error.value = message
    } finally {
      isLoading.value = false
    }
  }

  const clearError = (): void => {
    error.value = ''
  }

  const startAutoRefresh = (intervalMs = 45_000): void => {
    if (refreshTimer) {
      return
    }

    refreshTimer = setInterval(() => {
      void loadTenants()
    }, intervalMs)
  }

  const stopAutoRefresh = (): void => {
    if (!refreshTimer) {
      return
    }

    clearInterval(refreshTimer)
    refreshTimer = null
  }

  return {
    tenants,
    selectedTenant,
    selectedTenantId,
    isLoading,
    error,
    lastLoadedAt,
    loadTenants,
    clearError,
    startAutoRefresh,
    stopAutoRefresh
  }
}