import type { OperatorStatus } from '~/types/control-plane'

export default defineEventHandler(async (event): Promise<OperatorStatus> => {
  const runtimeConfig = useRuntimeConfig()
  const apiBase = String(runtimeConfig.apiBase || 'http://127.0.0.1:8000')
  const query = getQuery(event)

  try {
    return await $fetch<OperatorStatus>(`${apiBase}/dashboard/operator-status`, {
      query
    })
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to load operator flow status from the control plane.',
      data: error
    })
  }
})