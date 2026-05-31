import type { OperatorRecommendationResponse } from '~/types/control-plane'
import { proxyControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<OperatorRecommendationResponse> => {
  return proxyControlPlane<OperatorRecommendationResponse>(
    event,
    '/dashboard/operator-recommendation',
    'Failed to load operator recommendation from the control plane.'
  )
})
