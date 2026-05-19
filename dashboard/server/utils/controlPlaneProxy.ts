import { createError, getQuery, type H3Event } from 'h3'
import { $fetch } from 'ofetch'
import { useRuntimeConfig } from '#imports'

type ControlPlaneQuery = Record<string, unknown>

interface ControlPlaneFetchOptions {
  query?: ControlPlaneQuery
  body?: BodyInit | Record<string, unknown> | null
  method?: 'GET' | 'POST'
  timeoutMs?: number
}

export const proxyControlPlane = async <T>(
  event: H3Event,
  endpoint: string,
  failureMessage: string
): Promise<T> => {
  const query = getQuery(event) as ControlPlaneQuery

  try {
    return await fetchControlPlane<T>(endpoint, { query })
  } catch (error) {
    throw createError({
      statusCode: 502,
      statusMessage: failureMessage,
      data: error
    })
  }
}

export const fetchControlPlane = async <T>(
  endpoint: string,
  options: ControlPlaneFetchOptions = {}
): Promise<T> => {
  const bases = resolveControlPlaneApiBases()
  let lastError: unknown = null

  for (const apiBase of bases) {
    try {
      return await fetchFromControlPlaneBase<T>(apiBase, endpoint, options)
    } catch (error) {
      lastError = error
      if (!shouldTryNextControlPlaneBase(error)) {
        throw error
      }
    }
  }

  throw lastError
}

export const resolveControlPlaneApiBases = (): string[] => {
  const runtimeConfig = useRuntimeConfig()
  const configuredBase = String(runtimeConfig.apiBase || 'http://127.0.0.1:8000')

  return [...new Set([
    configuredBase,
    'http://127.0.0.1:8000',
    'http://127.0.0.1:8010'
  ])]
}

const fetchFromControlPlaneBase = async <T>(
  apiBase: string,
  endpoint: string,
  options: ControlPlaneFetchOptions
): Promise<T> => {
  const controller = new AbortController()
  const timeout = options.timeoutMs
    ? setTimeout(() => {
        controller.abort()
      }, options.timeoutMs)
    : null

  try {
    return await $fetch<T>(`${apiBase}${endpoint}`, {
      method: options.method,
      query: options.query,
      body: options.body,
      signal: controller.signal
    })
  } finally {
    if (timeout) {
      clearTimeout(timeout)
    }
  }
}

const shouldTryNextControlPlaneBase = (error: unknown): boolean => {
  const statusCode = typeof error === 'object' && error !== null && 'statusCode' in error
    ? Number((error as { statusCode?: number }).statusCode)
    : null

  return statusCode === null || Number.isNaN(statusCode) || statusCode >= 500
}
