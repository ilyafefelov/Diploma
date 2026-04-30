import type { SignalPreview } from '~/types/control-plane'

export default defineEventHandler(async (event): Promise<SignalPreview> => {
  const runtimeConfig = useRuntimeConfig()
  const apiBase = String(runtimeConfig.apiBase || 'http://127.0.0.1:8000')
  const query = getQuery(event)

  try {
    return await $fetch<SignalPreview>(`${apiBase}/dashboard/signal-preview`, {
      query
    })
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to load dashboard signal preview from the control plane.',
      data: error
    })
  }
})