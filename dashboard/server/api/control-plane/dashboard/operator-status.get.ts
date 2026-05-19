import type { OperatorStatus } from '~/types/control-plane'
import { fetchControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<OperatorStatus> => {
  const query = getQuery(event)

  try {
    return await fetchControlPlane<OperatorStatus>('/dashboard/operator-status', { query })
  } catch (error) {
    const fetchError = error as {
      statusCode?: number
      statusMessage?: string
      data?: { detail?: string }
    }

    if (fetchError.statusCode === 404) {
      throw createError({
        statusCode: 404,
        statusMessage: fetchError.data?.detail || fetchError.statusMessage || 'Operator flow status not found.',
        data: fetchError.data
      })
    }

    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to load operator flow status from the control plane.',
      data: error
    })
  }
})
