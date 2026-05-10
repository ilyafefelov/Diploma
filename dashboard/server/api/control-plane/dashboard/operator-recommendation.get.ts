import type { OperatorRecommendationResponse } from '~/types/control-plane'

export default defineEventHandler(async (event): Promise<OperatorRecommendationResponse> => {
  const runtimeConfig = useRuntimeConfig()
  const apiBase = String(runtimeConfig.apiBase || 'http://127.0.0.1:8010')
  const query = getQuery(event)

  try {
    return await $fetch<OperatorRecommendationResponse>(`${apiBase}/dashboard/operator-recommendation`, {
      query
    })
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to load operator recommendation from the control plane.',
      data: error
    })
  }
})
