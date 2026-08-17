// https://nuxt.com/docs/api/configuration/nuxt-config
type NodeRuntimeGlobal = typeof globalThis & {
  process?: {
    env?: Record<string, string | undefined>
  }
}

const env = (globalThis as NodeRuntimeGlobal).process?.env ?? {}
const siteUrl = env.NUXT_PUBLIC_SITE_URL || 'https://energy-index.full-iron.com'
const baseURL = env.NUXT_APP_BASE_URL || '/'
const bessDataBaseUrl
  = env.NUXT_PUBLIC_BESS_DATA_BASE_URL
    || 'https://raw.githubusercontent.com/ilyafefelov/Diploma/main/dashboard/public/data/bess-arbitrage-index'

export default defineNuxtConfig({

  modules: [
    '@nuxt/eslint',
    '@nuxt/ui'
  ],
  devtools: {
    enabled: false
  }, app: {
    baseURL,
    head: {
      link: [
        { rel: 'preconnect', href: 'https://fonts.googleapis.com' },
        { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' },
        {
          rel: 'stylesheet',
          href: 'https://fonts.googleapis.com/css2?family=Advent+Pro:wght@500;600;700;800&family=Alumni+Sans:wght@500;600;700;800;900&family=Anonymous+Pro:wght@400;700&family=Noto+Sans:wght@400;500;600;700;800;900&family=Noto+Sans+Mono:wght@400;500;600;700&family=Noto+Serif:wght@600;700;800&subset=latin,cyrillic&display=swap'
        }
      ]
    }
  },

  css: ['~/assets/css/main.css'],

  ui: {
    fonts: false
  },

  runtimeConfig: {
    apiBase: env.NUXT_API_BASE || 'http://127.0.0.1:8000',
    public: {
      siteUrl,
      siteName: 'Ukraine BESS Arbitrage Index',
      siteDescription: 'Source-backed public BESS arbitrage index for Ukrainian DAM prices.',
      bessDataBaseUrl
    }
  },

  compatibilityDate: '2025-01-15',

  vite: {
    optimizeDeps: {
      include: ['three']
    }
  },

  eslint: {
    config: {
      stylistic: {
        commaDangle: 'never',
        braceStyle: '1tbs'
      }
    }
  },
  icon: {
    serverBundle: {
      collections: ['lucide', 'simple-icons']
    },
    clientBundle: {
      scan: true
    }
  }

})
