<script setup lang="ts">
import { computed } from 'vue'
import HudSignalCharts from '~/components/dashboard/HudSignalCharts.vue'
import OperatorMarketSignalHero from '~/components/dashboard/operator/OperatorMarketSignalHero.vue'
import type { OperatorRecommendationResponse, SignalPreview, TenantSummary } from '~/types/control-plane'
import type { OperatorExplanationMode, OperatorMarketRegimeChip } from '~/types/operator-dashboard'

const props = defineProps<{
  tenants: TenantSummary[]
  selectedTenantId: string
  registryEnvelope: string
  explanationMode: OperatorExplanationMode
  explanationModeLabel: string
  marketRegimeChips: OperatorMarketRegimeChip[]
  signalPreview: SignalPreview | null
  operatorRecommendation: OperatorRecommendationResponse | null
  isRegistryLoading: boolean
  isSignalPreviewLoading: boolean
  signalPreviewLastLoadedLabel: string
}>()

const emit = defineEmits<{
  'update:explanationMode': [value: OperatorExplanationMode]
}>()

const formatBoundaryTimestamp = (value: string | null | undefined): string => {
  if (!value) {
    return 'not available'
  }

  return new Date(value).toLocaleString('en-GB', {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  })
}

const formatBoundaryStatus = (value: string | null | undefined): string => {
  if (!value) {
    return 'not available'
  }

  return value
    .split('_')
    .filter(Boolean)
    .map(part => part.length <= 3 ? part.toUpperCase() : `${part[0]?.toUpperCase() ?? ''}${part.slice(1)}`)
    .join(' ')
}

const marketBoundaryItems = computed(() => {
  const recommendation = props.operatorRecommendation

  return [
    {
      label: 'Scope',
      value: recommendation?.market_scope === 'dam_hourly_planning_preview'
        ? 'DAM hourly preview'
        : formatBoundaryStatus(recommendation?.market_scope)
    },
    {
      label: 'Delivery',
      value: recommendation?.target_delivery_window_start && recommendation.target_delivery_window_end
        ? `${formatBoundaryTimestamp(recommendation.target_delivery_window_start)} -> ${formatBoundaryTimestamp(recommendation.target_delivery_window_end)}`
        : 'loading'
    },
    {
      label: 'Gate',
      value: formatBoundaryStatus(recommendation?.market_gate_status ?? 'not_evaluated_preview_only')
    },
    {
      label: 'Bid',
      value: formatBoundaryStatus(recommendation?.proposed_bid_status ?? 'not_emitted_operator_preview')
    }
  ]
})
</script>

<template>
  <section
    id="operator-market"
    class="surface-panel market-console"
  >
    <div class="console-heading">
      <div>
        <p class="eyebrow">
          Market scope
        </p>
        <h2 class="section-title">
          DAM hourly planning preview
        </h2>
        <p class="console-subcopy">
          No ProposedBid, no market submission, no IDM recommendation mode.
        </p>
        <div
          class="console-boundary-strip"
          aria-label="Operator preview boundary"
        >
          <span
            v-for="item in marketBoundaryItems"
            :key="item.label"
            class="console-boundary-pill"
          >
            <strong>{{ item.label }}</strong>
            {{ item.value }}
          </span>
        </div>
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
        :operator-recommendation="operatorRecommendation"
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
