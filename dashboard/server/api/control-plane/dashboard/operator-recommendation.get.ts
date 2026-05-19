import type { OperatorRecommendationResponse } from '~/types/control-plane'
import { fetchControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<OperatorRecommendationResponse> => {
  const query = getQuery(event)

  try {
    return await fetchControlPlane<OperatorRecommendationResponse>('/dashboard/operator-recommendation', { query })
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to load operator recommendation from the control plane.',
      data: error
    })
  }
})
