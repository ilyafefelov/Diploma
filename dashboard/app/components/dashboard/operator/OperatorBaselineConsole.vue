<script setup lang="ts">
import HudBaselinePreview from '~/components/dashboard/HudBaselinePreview.vue'
import type { BaselineLpPreview } from '~/types/control-plane'
import type { OperatorExplanationMode } from '~/types/operator-dashboard'

defineProps<{
  baselinePreview: BaselineLpPreview | null
  isLoading: boolean
  lastLoadedLabel: string
  explanationMode: OperatorExplanationMode
}>()
</script>

<template>
  <ClientOnly>
    <HudBaselinePreview
      :baseline-preview="baselinePreview"
      :is-loading="isLoading"
      :last-loaded-label="lastLoadedLabel"
      :explanation-mode="explanationMode"
    />

    <template #fallback>
      <div class="chart-fallback chart-fallback-compact">
        Preparing baseline LP surface...
      </div>
    </template>
  </ClientOnly>
</template>
