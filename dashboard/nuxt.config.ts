// https://nuxt.com/docs/api/configuration/nuxt-config
type NodeRuntimeGlobal = typeof globalThis & {
  process?: {
    env?: Record<string, string | undefined>
  }
}

const env = (globalThis as NodeRuntimeGlobal).process?.env ?? {}
const siteUrl = env.NUXT_PUBLIC_SITE_URL || (env.VERCEL_URL ? `https://${env.VERCEL_URL}` : 'http://localhost:64163')

export default defineNuxtConfig({
  modules: [
    '@nuxt/eslint',
    '@nuxt/ui'
  ],

  devtools: {
    enabled: true
  },

  css: ['~/assets/css/main.css'],

  runtimeConfig: {
    apiBase: env.NUXT_API_BASE || 'http://127.0.0.1:8010',
    public: {
      siteUrl,
      siteName: 'Smart Arbitrage Operator',
      siteDescription: 'Operator dashboard for tenant-aware DAM baseline monitoring.'
    }
  },

  compatibilityDate: '2025-01-15',

  eslint: {
    config: {
      stylistic: {
        commaDangle: 'never',
        braceStyle: '1tbs'
      }
    }
  }
})
