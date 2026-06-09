import type { ShadowRecommendationPreviewResponse } from '~/types/control-plane'
import { fetchControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<ShadowRecommendationPreviewResponse> => {
  const query = getQuery(event)

  try {
    return await fetchControlPlane<ShadowRecommendationPreviewResponse>(
      '/dashboard/shadow-recommendation-preview',
      { query }
    )
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to load shadow recommendation preview from the control plane.',
      data: error
    })
  }
})
