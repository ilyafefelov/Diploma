const DEFAULT_PUBLIC_BESS_DATA_BASE_URL =
  'https://raw.githubusercontent.com/ilyafefelov/Diploma/main/dashboard/public/data/bess-arbitrage-index'

function publicBessDataBaseUrl(): string {
  const runtimeConfig = useRuntimeConfig()
  return String(runtimeConfig.public.bessDataBaseUrl || DEFAULT_PUBLIC_BESS_DATA_BASE_URL).replace(/\/$/, '')
}

function normalizedPublicBessPath(path: string): string {
  return path.replace(/^\/+/, '')
}

export function publicBessDataContentUrl(path: string): string {
  return `${publicBessDataBaseUrl()}/${normalizedPublicBessPath(path)}`
}

export function publicBessDataUrl(path: string): string {
  const cacheBucket = Math.floor(Date.now() / (5 * 60 * 1000))
  return `${publicBessDataContentUrl(path)}?v=${cacheBucket}`
}

export function parsePublicBessPayload<T>(payload: T | string): T {
  if (typeof payload === 'string') {
    return JSON.parse(payload) as T
  }
  return payload
}
