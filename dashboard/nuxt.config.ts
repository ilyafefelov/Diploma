// https://nuxt.com/docs/api/configuration/nuxt-config
type NodeRuntimeGlobal = typeof globalThis & {
  process?: {
    env?: Record<string, string | undefined>
  }
}

const env = (globalThis as NodeRuntimeGlobal).process?.env ?? {}
const siteUrl = env.NUXT_PUBLIC_SITE_URL || (env.VERCEL_URL ? `https://${env.VERCEL_URL}` : 'http://localhost:64163')
const baseURL = env.NUXT_APP_BASE_URL || '/'

export default defineNuxtConfig({
  app: {
    baseURL,
    head: {
      link: [
        { rel: 'preconnect', href: 'https://fonts.googleapis.com' },
        { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' },
        {
          rel: 'stylesheet',
          href: 'https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;700;800;900&display=swap'
        }
      ]
    }
  },

  modules: [
    '@nuxt/eslint',
    '@nuxt/ui'
  ],

  devtools: {
    enabled: false
  },

  css: ['~/assets/css/main.css'],

  vite: {
    optimizeDeps: {
      include: ['three']
    }
  },

  runtimeConfig: {
    apiBase: env.NUXT_API_BASE || 'http://127.0.0.1:8000',
    public: {
      siteUrl,
      siteName: 'Ukraine BESS Arbitrage Index',
      siteDescription: 'Source-backed public BESS arbitrage index for Ukrainian DAM prices.'
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
