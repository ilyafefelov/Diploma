import type { OperatorStatus } from '~/types/control-plane'
import { proxyOptionalControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<OperatorStatus | null> => {
  return proxyOptionalControlPlane<OperatorStatus>(
    event,
    '/dashboard/operator-status',
    'Failed to load operator flow status from the control plane.'
  )
})
