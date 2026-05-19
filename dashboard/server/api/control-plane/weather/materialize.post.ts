import { fetchControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event) => {
  const body = await readBody<Record<string, unknown>>(event)

  try {
    return await fetchControlPlane('/weather/materialize', { method: 'POST', body })
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to materialize weather assets from the control plane.',
      data: error
    })
  }
})
