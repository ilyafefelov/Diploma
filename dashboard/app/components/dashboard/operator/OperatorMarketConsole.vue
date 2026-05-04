<script setup lang="ts">
import HudSignalCharts from '~/components/dashboard/HudSignalCharts.vue'
import OperatorMarketSignalHero from '~/components/dashboard/operator/OperatorMarketSignalHero.vue'
import type { SignalPreview, TenantSummary } from '~/types/control-plane'
import type { OperatorExplanationMode, OperatorMarketRegimeChip } from '~/types/operator-dashboard'

defineProps<{
  tenants: TenantSummary[]
  selectedTenantId: string
  registryEnvelope: string
  explanationMode: OperatorExplanationMode
  explanationModeLabel: string
  marketRegimeChips: OperatorMarketRegimeChip[]
  signalPreview: SignalPreview | null
  isRegistryLoading: boolean
  isSignalPreviewLoading: boolean
  signalPreviewLastLoadedLabel: string
}>()

const emit = defineEmits<{
  'update:explanationMode': [value: OperatorExplanationMode]
}>()
</script>

<template>
  <section class="surface-panel market-console">
    <div class="console-heading">
      <div>
        <p class="eyebrow">
          Market signals
        </p>
        <h2 class="section-title">
          DAM / IDM arbitrage surface
        </h2>
      </div>

      <div class="console-controls">
        <div
          class="segmented-control"
          role="tablist"
          aria-label="Explanation mode"
        >
          <UButton
            label="MVP"
            color="info"
            variant="ghost"
            :class="{ 'segmented-control__button-active': explanationMode === 'mvp' }"
            @click="emit('update:explanationMode', 'mvp')"
          />
          <UButton
            label="Future"
            color="info"
            variant="ghost"
            :class="{ 'segmented-control__button-active': explanationMode === 'future' }"
            @click="emit('update:explanationMode', 'future')"
          />
        </div>

        <UBadge
          class="console-badge"
          :label="explanationModeLabel"
          color="success"
          variant="soft"
        />
      </div>
    </div>

    <div class="market-signal-layout">
      <div class="market-signal-panel">
        <OperatorMarketSignalHero
          :signal-preview="signalPreview"
          :is-loading="isSignalPreviewLoading"
          :last-loaded-label="signalPreviewLastLoadedLabel"
        />
      </div>

      <div class="market-regime">
        <div class="market-regime__heading">
          <p class="market-regime__label">
            Market regime
          </p>
        </div>
        <div class="market-regime__chips">
          <span
            v-for="chip in marketRegimeChips"
            :key="chip.label"
            class="market-chip"
            :class="{ 'market-chip-active': chip.active }"
            tabindex="0"
          >
            <UIcon :name="chip.icon" />
            <span>{{ chip.label }}</span>
            <span
              class="market-chip-tooltip"
              role="tooltip"
            >
              <strong>{{ chip.tooltipTitle }}</strong>
              <span>{{ chip.tooltipBody }}</span>
            </span>
          </span>
        </div>
      </div>
    </div>

    <ClientOnly>
      <HudSignalCharts
        :signal-preview="signalPreview"
        :is-loading="isSignalPreviewLoading"
        :last-loaded-label="signalPreviewLastLoadedLabel"
        :explanation-mode="explanationMode"
      />

      <template #fallback>
        <div class="chart-fallback chart-fallback-compact">
          Preparing signal charts...
        </div>
      </template>
    </ClientOnly>
  </section>
</template>
