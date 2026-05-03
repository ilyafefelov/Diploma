<script setup lang="ts">
const runtimeConfig = useRuntimeConfig()

useHead({
  meta: [
    { name: 'viewport', content: 'width=device-width, initial-scale=1' },
    { name: 'theme-color', content: '#0079c1' }
  ],
  link: [
    { rel: 'icon', type: 'image/svg+xml', href: '/favicon.svg' },
    { rel: 'alternate icon', href: '/favicon.ico' }
  ],
  htmlAttrs: {
    lang: 'en'
  }
})

const title = runtimeConfig.public.siteName
const description = runtimeConfig.public.siteDescription
const siteUrl = String(runtimeConfig.public.siteUrl || 'http://localhost:64163').replace(/\/$/, '')

useSeoMeta({
  title,
  description,
  ogTitle: title,
  ogDescription: description,
  ogSiteName: title,
  ogUrl: siteUrl,
  ogImage: `${siteUrl}/social/week1-interactive-report.svg`,
  twitterTitle: title,
  twitterDescription: description,
  twitterImage: `${siteUrl}/social/week1-interactive-report.svg`,
  twitterCard: 'summary_large_image'
})
</script>

<template>
  <UApp>
    <UMain class="min-h-screen">
      <NuxtPage />
    </UMain>
  </UApp>
</template>
