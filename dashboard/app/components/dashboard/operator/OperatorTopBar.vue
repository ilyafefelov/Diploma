<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  clockLabel: string
  isLoading: boolean
  activeAlertCount: number
  timezoneLabel: string
}>()

const emit = defineEmits<{
  refresh: []
}>()

const previewStatusLabel = computed(() => {
  if (props.isLoading) {
    return 'Refreshing preview'
  }

  return props.activeAlertCount > 0 ? 'Preview gaps' : 'Preview ready'
})
</script>

<template>
  <header
    id="operator-overview"
    class="operator-topbar"
  >
    <div class="brand-cluster">
      <div
        class="brand-orb"
        aria-hidden="true"
      >
        <UIcon name="i-lucide-zap" />
      </div>
      <div>
        <p class="brand-kicker">
          BESS read model
        </p>
        <h1 class="brand-title">
          Operator Preview
        </h1>
      </div>
    </div>

    <div class="topbar-status">
      <span class="topbar-chip topbar-chip-clock">{{ clockLabel }}</span>
      <span
        class="topbar-chip topbar-chip-live"
        :class="{ 'topbar-chip-warning': activeAlertCount > 0 && !isLoading }"
      >
        <span class="status-dot" />
        {{ previewStatusLabel }}
      </span>
      <span class="topbar-chip">
        <UIcon name="i-lucide-map-pin" />
        {{ timezoneLabel }}
      </span>
      <UButton
        class="icon-button"
        icon="i-lucide-refresh-cw"
        :loading="isLoading"
        :disabled="isLoading"
        color="info"
        variant="ghost"
        square
        aria-label="Refresh registry"
        @click="emit('refresh')"
      />
    </div>
  </header>
</template>
