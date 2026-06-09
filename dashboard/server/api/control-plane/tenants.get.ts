import type { TenantSummary } from '~/types/control-plane'
import { fetchControlPlane } from '../../utils/controlPlaneProxy'

export default defineEventHandler(async (): Promise<TenantSummary[]> => {
  try {
    return await fetchControlPlane<TenantSummary[]>('/tenants')
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to load tenants from the control plane.',
      data: error
    })
  }
})
