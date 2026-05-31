<script setup lang="ts">
import ClientVChart from '~/components/dashboard/ClientVChart.vue'
import { dashboardChartTokens } from '~/lib/charts/dashboardChartCore'
import type { PolicyValueMode } from '~/utils/operatorFutureStackPresentation'
import type {
  FutureChartGuideItem,
  FutureChartSummaryItem
} from './operatorFutureChartCardTypes'

const policyLegendItems = [
  {
    label: 'Battery MW',
    detail: '+ discharge/sell, - charge/buy',
    color: dashboardChartTokens.secondaryStrongOnDark,
    swatch: 'bar'
  },
  {
    label: 'Shortfall',
    detail: 'max(0, strict LP/reference value - selected preview value), UAH',
    color: dashboardChartTokens.warning,
    swatch: 'line'
  },
  {
    label: 'Price context',
    detail: 'DAM/IDM read-model UAH/MWh; not a value metric',
    color: dashboardChartTokens.highlightOnDark,
    swatch: 'line'
  }
] as const

defineProps<{
  policyChartTitle: string
  policyChartDescription: string
  policyChartSummary: FutureChartSummaryItem[]
  selectedRecommendationGuideItems: FutureChartGuideItem[]
  policyOption: Record<string, unknown>
  isOfficialPolicyMode: boolean
  hasOfficialPolicyRows: boolean
  isShadowRecommendationMode: boolean
}>()

const emit = defineEmits<{
  'update:policyValueMode': [mode: PolicyValueMode]
}>()
</script>

<template>
  <article class="future-chart-card">
    <div class="policy-chart-heading">
      <div>
        <p class="decision-chart-card__eyebrow">
          Policy value
        </p>
        <h3>{{ policyChartTitle }}</h3>
        <p>{{ policyChartDescription }}</p>
      </div>
      <div
        class="policy-chart-toggle"
        role="group"
        aria-label="Policy value source"
      >
        <button
          type="button"
          :aria-pressed="!isOfficialPolicyMode"
          :class="{ 'policy-chart-toggle__button--active': !isOfficialPolicyMode }"
          @click="emit('update:policyValueMode', 'selected')"
        >
          <UIcon name="i-lucide-brain" />
          <span>{{ isShadowRecommendationMode ? 'Shadow preview' : 'Selected schedule' }}</span>
        </button>
        <button
          type="button"
          :aria-pressed="isOfficialPolicyMode"
          :disabled="!hasOfficialPolicyRows"
          :class="{ 'policy-chart-toggle__button--active': isOfficialPolicyMode }"
          @click="emit('update:policyValueMode', 'official')"
        >
          <UIcon name="i-lucide-database" />
          <span>Forecast rows</span>
        </button>
      </div>
    </div>
    <div>
      <div
        v-if="policyChartSummary.length"
        class="forecast-quality-strip"
      >
        <span
          v-for="item in policyChartSummary"
          :key="item.label"
        >
          {{ item.label }}: {{ item.value }} / {{ item.meta }}
        </span>
      </div>
      <div
        v-if="selectedRecommendationGuideItems.length"
        class="future-chart-legend future-chart-legend--policy"
        aria-label="Policy value chart legend"
      >
        <span
          v-for="item in policyLegendItems"
          :key="item.label"
          class="future-chart-legend__item"
        >
          <span
            class="future-chart-legend__swatch"
            :class="`future-chart-legend__swatch--${item.swatch}`"
            :style="{ '--future-chart-legend-color': item.color }"
            aria-hidden="true"
          />
          <span class="future-chart-legend__text">
            <strong>{{ item.label }}</strong>
            <small>{{ item.detail }}</small>
          </span>
        </span>
      </div>
    </div>
    <ClientVChart
      :option="policyOption"
      autoresize
      class="future-chart"
    />
  </article>
</template>
