import type { OperatorPreviewEnsureResponse } from '~/types/control-plane'
import { proxyControlPlane } from '../../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<OperatorPreviewEnsureResponse> => {
  return proxyControlPlane<OperatorPreviewEnsureResponse>(
    event,
    '/dashboard/operator-preview/readiness',
    'Failed to inspect operator preview readiness from the control plane.'
  )
})
