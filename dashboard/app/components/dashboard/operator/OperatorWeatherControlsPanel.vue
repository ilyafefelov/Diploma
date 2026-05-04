<script setup lang="ts">
const includePriceHistory = defineModel<boolean>('includePriceHistory', {
  required: true
})

defineProps<{
  statusLabel: string
  isPreparing: boolean
  isMaterializing: boolean
  hasSelectedTenant: boolean
  lastActionLabel: string
  weatherLocationLabel: string
}>()

const emit = defineEmits<{
  prepare: []
  materialize: []
}>()
</script>

<template>
  <section class="surface-panel control-actions-panel">
    <div class="rail-heading">
      <div>
        <p class="eyebrow">
          Control actions
        </p>
        <h2 class="rail-title">
          Weather slice
        </h2>
      </div>
      <UBadge
        class="status-badge"
        :label="statusLabel"
        color="success"
        variant="soft"
      />
    </div>

    <label class="check-toggle">
      <input
        v-model="includePriceHistory"
        type="checkbox"
      >
      <span>Include DAM price history</span>
    </label>

    <div class="action-button-grid">
      <UButton
        class="control-button control-button-primary"
        icon="i-lucide-settings-2"
        :label="isPreparing ? 'Preparing' : 'Prepare'"
        :loading="isPreparing"
        :disabled="isPreparing || !hasSelectedTenant"
        color="info"
        variant="solid"
        @click="emit('prepare')"
      />
      <UButton
        class="control-button control-button-green"
        icon="i-lucide-cloud-cog"
        :label="isMaterializing ? 'Running' : 'Materialize'"
        :loading="isMaterializing"
        :disabled="isMaterializing || !hasSelectedTenant"
        color="success"
        variant="solid"
        @click="emit('materialize')"
      />
    </div>

    <p class="control-meta">
      Last action {{ lastActionLabel }}
    </p>
    <p class="control-meta">
      {{ weatherLocationLabel }}
    </p>
  </section>
</template>
