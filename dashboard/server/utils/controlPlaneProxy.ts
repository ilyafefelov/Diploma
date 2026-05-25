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

export const proxyOptionalControlPlane = async <T>(
  event: H3Event,
  endpoint: string,
  failureMessage: string
): Promise<T | null> => {
  const query = getQuery(event) as ControlPlaneQuery

  try {
    return await fetchControlPlane<T>(endpoint, { query })
  } catch (error) {
    if (isControlPlaneNotFound(error)) {
      return null
    }

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

export const isControlPlaneNotFound = (error: unknown): boolean => {
  const statusCode = controlPlaneStatusCode(error)
  return statusCode === 404
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
  const statusCode = controlPlaneStatusCode(error)

  return statusCode === null || Number.isNaN(statusCode) || statusCode >= 500
}

const controlPlaneStatusCode = (error: unknown): number | null => {
  if (typeof error !== 'object' || error === null) {
    return null
  }

  if ('statusCode' in error) {
    return Number((error as { statusCode?: number }).statusCode)
  }

  if ('response' in error) {
    const response = (error as { response?: { status?: number } }).response
    return typeof response?.status === 'number' ? response.status : null
  }

  return null
}
