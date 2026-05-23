<script setup lang="ts">
defineProps<{
  clockLabel: string
  isLoading: boolean
  timezoneLabel: string
}>()

const emit = defineEmits<{
  refresh: []
}>()
</script>

<template>
  <header class="operator-topbar">
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
      <span class="topbar-chip topbar-chip-live">
        <span class="status-dot" />
        {{ isLoading ? 'Refreshing preview' : 'Preview ready' }}
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
