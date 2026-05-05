import type { DecisionTransformerTrajectoryResponse } from '~/types/control-plane'
import { proxyControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<DecisionTransformerTrajectoryResponse> => {
  return proxyControlPlane<DecisionTransformerTrajectoryResponse>(
    event,
    '/dashboard/decision-transformer-trajectories',
    'Failed to load Decision Transformer trajectories from the control plane.'
  )
})
