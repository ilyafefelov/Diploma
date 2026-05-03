import type { BaselineLpPreview } from '~/types/control-plane'

export default defineEventHandler(async (event): Promise<BaselineLpPreview> => {
  const runtimeConfig = useRuntimeConfig()
  const apiBase = String(runtimeConfig.apiBase || 'http://127.0.0.1:8010')
  const query = getQuery(event)

  try {
    return await $fetch<BaselineLpPreview>(`${apiBase}/dashboard/baseline-lp-preview`, {
      query
    })
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to load baseline LP preview from the control plane.',
      data: error
    })
  }
})
