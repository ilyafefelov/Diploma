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
      <span
        class="control-help-tooltip"
        role="tooltip"
      >
        When enabled, materialization refreshes weather context together with DAM price history so later Silver features can join market and weather context.
      </span>
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

    <div class="control-step-grid">
      <article>
        <span>Prepare</span>
        <p>Builds Dagster run config for selected tenant and resolved location. No assets run yet.</p>
      </article>
      <article>
        <span>Materialize</span>
        <p>Runs selected Bronze/Silver assets, persists backend status, and refreshes dashboard read models.</p>
      </article>
    </div>
  </section>
</template>

<style scoped>
.check-toggle {
  position: relative;
  cursor: help;
}

.control-help-tooltip {
  position: absolute;
  right: 0;
  bottom: calc(100% + 0.45rem);
  z-index: 120;
  width: min(18rem, calc(100vw - 2rem));
  border: 1px solid rgba(202, 249, 255, 0.9);
  border-radius: 0.72rem;
  background: linear-gradient(180deg, rgba(0, 129, 204, 0.98), rgba(0, 56, 112, 0.98));
  padding: 0.65rem;
  color: rgba(238, 250, 255, 0.92);
  font-size: 0.72rem;
  font-weight: 750;
  line-height: 1.35;
  box-shadow: 0 18px 32px rgba(0, 39, 82, 0.32);
  opacity: 0;
  pointer-events: none;
  transform: translateY(0.25rem) scale(0.98);
  transition: opacity 150ms ease, transform 150ms ease;
}

.check-toggle:hover .control-help-tooltip,
.check-toggle:focus-within .control-help-tooltip {
  opacity: 1;
  transform: translateY(0) scale(1);
}

.control-step-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.45rem;
}

.control-step-grid article {
  display: grid;
  gap: 0.2rem;
  border: 1px solid rgba(255, 255, 255, 0.24);
  border-radius: 0.62rem;
  background: rgba(0, 61, 119, 0.24);
  padding: 0.55rem;
}

.control-step-grid span {
  color: #d7ff4f;
  font-size: 0.68rem;
  font-weight: 900;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.control-step-grid p {
  margin: 0;
  color: rgba(229, 249, 255, 0.82);
  font-size: 0.72rem;
  font-weight: 750;
  line-height: 1.35;
}
</style>
