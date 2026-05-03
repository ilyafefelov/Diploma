export default defineEventHandler(async (event) => {
  const runtimeConfig = useRuntimeConfig()
  const apiBase = String(runtimeConfig.apiBase || 'http://127.0.0.1:8010')
  const body = await readBody<Record<string, unknown>>(event)

  try {
    return await $fetch(`${apiBase}/weather/run-config`, {
      method: 'POST',
      body
    })
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: 'Failed to prepare weather run config from the control plane.',
      data: error
    })
  }
})