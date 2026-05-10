import { createError, getQuery, type H3Event } from 'h3'
import { $fetch } from 'ofetch'
import { useRuntimeConfig } from '#imports'

export const proxyControlPlane = async <T>(
  event: H3Event,
  endpoint: string,
  failureMessage: string
): Promise<T> => {
  const runtimeConfig = useRuntimeConfig()
  const apiBase = String(runtimeConfig.apiBase || 'http://127.0.0.1:8010')
  const query = getQuery(event)

  try {
    return await $fetch<T>(`${apiBase}${endpoint}`, {
      query
    })
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: failureMessage,
      data: error
    })
  }
}
