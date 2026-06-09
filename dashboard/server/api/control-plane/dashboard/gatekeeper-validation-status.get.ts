import type { GatekeeperValidationStatusResponse } from '~/types/control-plane'
import { fetchControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<GatekeeperValidationStatusResponse> => {
  const query = getQuery(event)

  try {
    return await fetchControlPlane<GatekeeperValidationStatusResponse>('/dashboard/gatekeeper-validation-status', { query })
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to load Bid Gatekeeper validation status from the control plane.',
      data: error
    })
  }
})
