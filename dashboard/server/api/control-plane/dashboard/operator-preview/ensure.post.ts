import type { OperatorPreviewEnsureResponse } from '~/types/control-plane'
import { proxyControlPlanePost } from '../../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<OperatorPreviewEnsureResponse> => {
  return proxyControlPlanePost<OperatorPreviewEnsureResponse>(
    event,
    '/dashboard/operator-preview/ensure',
    'Failed to ensure operator preview rows from the control plane.'
  )
})
