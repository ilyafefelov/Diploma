import type { TenantSummary } from '~/types/control-plane'

export default defineEventHandler(async (): Promise<TenantSummary[]> => {
  const runtimeConfig = useRuntimeConfig()
  const apiBase = String(runtimeConfig.apiBase || 'http://127.0.0.1:8010')

  try {
    return await $fetch<TenantSummary[]>(`${apiBase}/tenants`)
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to load tenants from the control plane.',
      data: error
    })
  }
})