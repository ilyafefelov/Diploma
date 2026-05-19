import type { BaselineLpPreview } from '~/types/control-plane'
import { fetchControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<BaselineLpPreview> => {
  const query = getQuery(event)

  try {
    return await fetchControlPlane<BaselineLpPreview>('/dashboard/baseline-lp-preview', { query })
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to load baseline LP preview from the control plane.',
      data: error
    })
  }
})
