import { $fetch as fetchExternal } from 'ofetch'

export default defineEventHandler(async (event) => {
  const runtimeConfig = useRuntimeConfig()
  const apiBase = String(runtimeConfig.apiBase || 'http://127.0.0.1:8010')
  const body = await readBody<Record<string, unknown>>(event)

  try {
    return await fetchExternal(`${apiBase}/weather/materialize`, {
      method: 'POST',
      body
    })
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to materialize weather assets from the control plane.',
      data: error
    })
  }
})
