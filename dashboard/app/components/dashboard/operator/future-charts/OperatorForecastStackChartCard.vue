<script setup lang="ts">
import ClientVChart from '~/components/dashboard/ClientVChart.vue'
import { dashboardChartTokens } from '~/lib/charts/dashboardChartCore'
import type {
  FutureChartHiddenForecastItem,
  FutureChartQualityItem
} from './operatorFutureChartCardTypes'

const forecastLegendItems = [
  {
    label: 'Price context',
    detail: 'DAM/IDM read-model input, UAH/MWh',
    color: dashboardChartTokens.highlightOnDark,
    swatch: 'line'
  },
  {
    label: 'Battery MW',
    detail: '+ discharge/sell, - charge/buy',
    color: dashboardChartTokens.secondarySoftOnDark,
    swatch: 'bar'
  },
  {
    label: 'SOC',
    detail: 'Projected state of charge, %',
    color: dashboardChartTokens.rose,
    swatch: 'line'
  },
  {
    label: 'Site load',
    detail: 'Configured/proxy estimate, MW when available',
    color: dashboardChartTokens.warning,
    swatch: 'line'
  }
] as const

defineProps<{
  forecastChartTitle: string
  forecastStackDescription: string
  recommendationInputSummaryItems: FutureChartQualityItem[]
  hiddenUnsafeForecastItems: FutureChartHiddenForecastItem[]
  forecastOption: Record<string, unknown>
}>()
</script>

<template>
  <article class="future-chart-card">
    <div>
      <p class="decision-chart-card__eyebrow">
        Forecast stack
      </p>
      <h3>{{ forecastChartTitle }}</h3>
      <p>{{ forecastStackDescription }}</p>
      <div class="forecast-quality-strip">
        <span
          v-for="item in recommendationInputSummaryItems"
          :key="item.modelName"
          :class="{ 'forecast-quality-strip__item--warn': item.needsCalibration }"
        >
          {{ item.label }}
        </span>
        <span
          v-for="item in hiddenUnsafeForecastItems"
          :key="`hidden-${item.modelName}`"
          class="forecast-quality-strip__item--warn"
        >
          hidden {{ item.modelName }}: {{ item.label }}
        </span>
      </div>
      <div
        class="future-chart-legend"
        aria-label="Forecast chart legend"
      >
        <span
          v-for="item in forecastLegendItems"
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
      <p class="future-chart-legend-note">
        Compact fallback NBEATSx/TFT rows are not independent model evidence in this chart.
      </p>
    </div>
    <ClientVChart
      :option="forecastOption"
      autoresize
      class="future-chart"
    />
  </article>
</template>
