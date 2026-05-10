<script setup lang="ts">
import { computed } from 'vue'
import { LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

import type { SignalPreview } from '~/types/control-plane'
import { buildMarketSignalHeroChartOption } from '~/utils/dashboardChartTheme'

use([CanvasRenderer, LineChart, GridComponent, TooltipComponent, LegendComponent])

const props = defineProps<{
  signalPreview: SignalPreview | null
  isLoading: boolean
  lastLoadedLabel: string
}>()

const chartOption = computed(() => buildMarketSignalHeroChartOption(props.signalPreview))

const hasSignalData = computed(() => {
  return !!props.signalPreview && props.signalPreview.market_price.length > 0
})

const formatNumber = (value: number): string => `${value.toFixed(2)}`
const formatPowerLabel = (value: number): string => `${value >= 0 ? '+' : ''}${value.toFixed(1)} UAH/MWh`
const formatPeriodDateTime = (value: string | null | undefined): string => {
  if (!value) {
    return 'date pending'
  }

  const parsedDate = new Date(value)

  if (Number.isNaN(parsedDate.getTime())) {
    return 'date pending'
  }

  return new Intl.DateTimeFormat('en-GB', {
    day: '2-digit',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
    timeZone: props.signalPreview?.timezone || props.signalPreview?.resolved_location.timezone || 'Europe/Kyiv'
  }).format(parsedDate)
}

const latestPricePeriodLabel = computed(() => {
  return `Hour: ${formatPeriodDateTime(props.signalPreview?.latest_price_timestamp || props.signalPreview?.forecast_window_end)}`
})

const forecastWindowPeriodLabel = computed(() => {
  const startLabel = formatPeriodDateTime(props.signalPreview?.forecast_window_start)
  const endLabel = formatPeriodDateTime(props.signalPreview?.forecast_window_end)

  if (startLabel === 'date pending' || endLabel === 'date pending') {
    return 'Forecast window pending'
  }

  return `${startLabel} → ${endLabel}`
})

const latestMarketPrice = computed(() => {
  if (!props.signalPreview?.market_price.length) {
    return null
  }

  return props.signalPreview.market_price.at(-1) ?? null
})

const maxMarketPrice = computed(() => {
  if (!props.signalPreview?.market_price.length) {
    return null
  }

  return Math.max(...props.signalPreview.market_price)
})

const minMarketPrice = computed(() => {
  if (!props.signalPreview?.market_price.length) {
    return null
  }

  return Math.min(...props.signalPreview.market_price)
})

const avgBias = computed(() => {
  if (!props.signalPreview?.weather_bias.length) {
    return null
  }

  const avg = props.signalPreview.weather_bias.reduce((acc, value) => acc + value, 0) / props.signalPreview.weather_bias.length
  return avg
})

const forecastSpread = computed(() => {
  if (maxMarketPrice.value == null || minMarketPrice.value == null) {
    return null
  }

  return maxMarketPrice.value - minMarketPrice.value
})
</script>

<template>
  <div class="market-signal-hero">
    <div class="market-signal-hero__toolbar">
      <div class="market-signal-hero__tabs">
        <UButton
          label="DAM"
          color="info"
          variant="ghost"
          class="market-signal-hero__tab market-signal-hero__tab-active"
        />
        <UButton
          label="IDM"
          color="info"
          variant="ghost"
          class="market-signal-hero__tab"
        />
        <UButton
          label="Both"
          color="info"
          variant="ghost"
          class="market-signal-hero__tab"
        />
      </div>

      <div class="market-signal-hero__range">
        <span>24H</span>
        <span>7D</span>
        <span>30D</span>
      </div>

      <UBadge
        class="market-signal-hero__updated"
        :label="lastLoadedLabel"
        color="success"
        variant="soft"
      />
    </div>

    <div
      v-if="!isLoading"
      class="market-signal-hero__metrics"
    >
      <article
        class="hud-mini-stat"
        tabindex="0"
      >
        <p class="hud-mini-stat__label">
          DAM spot
        </p>
        <strong>{{ latestMarketPrice == null ? '—' : formatPowerLabel(latestMarketPrice) }}</strong>
        <p class="hud-mini-stat__meta">
          {{ latestPricePeriodLabel }}
        </p>
        <span
          class="hud-mini-stat__tooltip"
          role="tooltip"
        >
          <span class="hud-mini-stat__tooltip-title">Latest DAM price</span>
          <span>Formula: P_t = market_price[t]</span>
          <span>Period: {{ latestPricePeriodLabel }}.</span>
          <span>Definition: expected settlement level used in the current MVP preview.</span>
        </span>
      </article>
      <article
        class="hud-mini-stat"
        tabindex="0"
      >
        <p class="hud-mini-stat__label">
          Window spread
        </p>
        <strong>{{ forecastSpread == null ? '—' : `${formatNumber(forecastSpread)} UAH/MWh` }}</strong>
        <p class="hud-mini-stat__meta">
          {{ forecastWindowPeriodLabel }}
        </p>
        <span
          class="hud-mini-stat__tooltip"
          role="tooltip"
        >
          <span class="hud-mini-stat__tooltip-title">Forecast band</span>
          <span>Formula: spread = max(price_i) − min(price_i)</span>
          <span>Period: {{ forecastWindowPeriodLabel }}.</span>
          <span>Interpretation: higher spread usually gives higher arbitrage opportunity.</span>
        </span>
      </article>
      <article
        class="hud-mini-stat"
        tabindex="0"
      >
        <p class="hud-mini-stat__label">
          Weather uplift
        </p>
        <strong>{{ avgBias == null ? '—' : `${avgBias >= 0 ? '+' : ''}${formatNumber(avgBias)} UAH/MWh` }}</strong>
        <p class="hud-mini-stat__meta">
          {{ forecastWindowPeriodLabel }}
        </p>
        <span
          class="hud-mini-stat__tooltip"
          role="tooltip"
        >
          <span class="hud-mini-stat__tooltip-title">Weather term</span>
          <span>Formula: price_adj = market_price + weather_bias</span>
          <span>Period: {{ forecastWindowPeriodLabel }}.</span>
          <span>Source: calibrated uplift from cloud, precipitation, humidity, solar, and wind.</span>
        </span>
      </article>
      <article
        class="hud-mini-stat"
        tabindex="0"
      >
        <p class="hud-mini-stat__label">
          Signal count
        </p>
        <strong>{{ hasSignalData ? props.signalPreview?.labels.length : '—' }}</strong>
        <p class="hud-mini-stat__meta">
          {{ forecastWindowPeriodLabel }}
        </p>
        <span
          class="hud-mini-stat__tooltip"
          role="tooltip"
        >
          <span class="hud-mini-stat__tooltip-title">Signal density</span>
          <span>Formula: point_count = len(labels)</span>
          <span>Period: {{ forecastWindowPeriodLabel }}.</span>
          <span>Interpretation: longer horizon gives smoother visual trend and confidence for dispatch alignment.</span>
        </span>
      </article>
    </div>

    <ClientOnly>
      <div
        v-if="isLoading"
        class="chart-fallback"
      >
        Preparing market signals...
      </div>
      <VChart
        v-else
        class="market-signal-hero__chart"
        :option="chartOption"
        autoresize
      />

      <template #fallback>
        <div class="chart-fallback">
          Preparing market signals...
        </div>
      </template>
    </ClientOnly>
  </div>
</template>
