import type { DecisionTransformerTrajectoryResponse } from '~/types/control-plane'
import { proxyOptionalControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<DecisionTransformerTrajectoryResponse | null> => {
  return proxyOptionalControlPlane<DecisionTransformerTrajectoryResponse>(
    event,
    '/dashboard/decision-transformer-trajectories',
    'Failed to load Decision Transformer trajectories from the control plane.'
  )
})
