import type { AcademicMvpReadinessResponse } from '~/types/control-plane'
import { fetchControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (): Promise<AcademicMvpReadinessResponse> => {
  try {
    return await fetchControlPlane<AcademicMvpReadinessResponse>('/dashboard/academic-mvp-readiness')
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to load academic MVP readiness from the control plane.',
      data: error
    })
  }
})
