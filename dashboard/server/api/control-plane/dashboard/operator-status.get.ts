import type { OperatorStatus } from '~/types/control-plane'

export default defineEventHandler(async (event): Promise<OperatorStatus> => {
  const runtimeConfig = useRuntimeConfig()
  const apiBase = String(runtimeConfig.apiBase || 'http://127.0.0.1:8010')
  const query = getQuery(event)

  try {
    return await $fetch<OperatorStatus>(`${apiBase}/dashboard/operator-status`, {
      query
    })
  } catch (error) {
    const fetchError = error as {
      statusCode?: number
      statusMessage?: string
      data?: { detail?: string }
    }

    if (fetchError.statusCode === 404) {
      throw createError({
        statusCode: 404,
        statusMessage: fetchError.data?.detail || fetchError.statusMessage || 'Operator flow status not found.',
        data: fetchError.data
      })
    }

    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to load operator flow status from the control plane.',
      data: error
    })
  }
})
