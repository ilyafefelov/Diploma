<script setup lang="ts">
import ClientVChart from '~/components/dashboard/ClientVChart.vue'
import type {
  FutureChartShadowStoryItem,
  FutureChartSummaryItem
} from './operatorFutureChartCardTypes'

defineProps<{
  selectedScheduleWindowLabel: string
  shadowModelStoryItems: FutureChartShadowStoryItem[]
  strategyComparisonSummary: FutureChartSummaryItem[]
  strategyComparisonOption: Record<string, unknown>
}>()
</script>

<template>
  <article class="future-chart-card future-chart-card--wide">
    <div>
      <p class="decision-chart-card__eyebrow">
        Strategy comparison
      </p>
      <h3>Delivery-day strategy comparison</h3>
      <p>
        Charge/discharge totals and regret metrics are shown for the selected market delivery window
        {{ selectedScheduleWindowLabel }}. Shadow and diagnostic strategies stay preview-only; blocked V13/DT/LAVA
        remains visible as gate evidence with no schedule rows.
      </p>
      <div class="shadow-model-story-strip">
        <span
          v-for="item in shadowModelStoryItems"
          :key="item.label"
        >
          <strong>{{ item.label }}</strong>
          {{ item.value }}
          <small>{{ item.meta }}</small>
        </span>
      </div>
      <div
        v-if="strategyComparisonSummary.length"
        class="forecast-quality-strip"
      >
        <span
          v-for="item in strategyComparisonSummary"
          :key="item.label"
        >
          {{ item.label }}: {{ item.value }} / {{ item.meta }}
        </span>
      </div>
    </div>
    <ClientVChart
      :option="strategyComparisonOption"
      autoresize
      class="future-chart"
    />
  </article>
</template>
