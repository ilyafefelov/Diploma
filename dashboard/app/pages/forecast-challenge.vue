<script setup lang="ts">
import { computed } from 'vue'
import type {
  PublicBessForecastArtifact as PublicPayload,
  PublicBessForecastModel as ForecastModel
} from '~/utils/publicBessArtifactTypes'
import { parsePublicBessPayload, publicBessDataUrl } from '~/utils/publicBessData'

const SVG_WIDTH = 760
const SVG_HEIGHT = 300
const SVG_MARGIN = { top: 24, right: 26, bottom: 34, left: 58 }
const SERIES_COLORS = ['#0c7eb3', '#76b82a', '#164260', '#b25e09']

const { data: forecastData } = await useFetch<PublicPayload>(publicBessDataUrl('forecast/latest.json'), {
  key: 'public-bess-forecast-latest',
  server: false,
  transform: parsePublicBessPayload,
  default: () => ({ models: [] })
})

const models = computed<ForecastModel[]>(() => (
  Array.isArray(forecastData.value?.models) ? forecastData.value.models : []
))
const forecastPointModels = computed(() => (
  models.value.filter(model => Array.isArray(model.points) && model.points.length > 0)
))
const primaryModel = computed(() => forecastPointModels.value[0] || models.value[0] || null)
const hasForecastPoints = computed(() => forecastPointModels.value.length > 0)

const forecastSvg = computed(() => {
  const allValues = forecastPointModels.value.flatMap(model => (
    (model.points || []).map(point => numberValue(point.forecast_price_uah_mwh))
  ))
  const domain = domainFor(allValues)
  const maxPointCount = Math.max(0, ...forecastPointModels.value.map(model => model.points?.length || 0))
  const xFor = xScale(maxPointCount, SVG_WIDTH)
  const yFor = yScale(domain, SVG_HEIGHT)
  const referencePoints = forecastPointModels.value[0]?.points || []
  return {
    width: SVG_WIDTH,
    height: SVG_HEIGHT,
    series: forecastPointModels.value.map((model, modelIndex) => ({
      label: String(model.label || model.model_name),
      color: SERIES_COLORS[modelIndex % SERIES_COLORS.length],
      line: pointsAttr((model.points || []).map((point, pointIndex) => ({
        x: xFor(pointIndex),
        y: yFor(numberValue(point.forecast_price_uah_mwh))
      })))
    })),
    yTicks: ticksFor(domain, 4).map(value => ({
      label: formatNumber(value, 0),
      y: yFor(value)
    })),
    xTicks: tickIndexes(maxPointCount).map(index => ({
      label: hourLabel(referencePoints[index]?.timestamp),
      x: xFor(index)
    }))
  }
})

function hourLabel(value: string | undefined): string {
  if (!value) {
    return ''
  }
  return value.slice(11, 16)
}

function numberValue(value: unknown): number {
  const numeric = Number(value || 0)
  return Number.isFinite(numeric) ? numeric : 0
}

function formatNumber(value: unknown, digits = 2) {
  const numeric = Number(value || 0)
  const fixed = Number.isFinite(numeric) ? numeric.toFixed(digits) : (0).toFixed(digits)
  const [integerPart = '0', decimalPart] = fixed.split('.')
  const sign = integerPart.startsWith('-') ? '-' : ''
  const unsignedInteger = integerPart.replace('-', '')
  const groupedInteger = unsignedInteger.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  return decimalPart ? `${sign}${groupedInteger}.${decimalPart}` : `${sign}${groupedInteger}`
}

function plotWidth(width: number): number {
  return width - SVG_MARGIN.left - SVG_MARGIN.right
}

function plotHeight(height: number): number {
  return height - SVG_MARGIN.top - SVG_MARGIN.bottom
}

function xScale(count: number, width: number) {
  const usableWidth = plotWidth(width)
  return (index: number) => SVG_MARGIN.left + (count <= 1 ? usableWidth / 2 : index / (count - 1) * usableWidth)
}

function yScale(domain: { min: number, max: number }, height: number) {
  const usableHeight = plotHeight(height)
  const range = Math.max(0.000001, domain.max - domain.min)
  return (value: number) => SVG_MARGIN.top + (domain.max - value) / range * usableHeight
}

function domainFor(values: number[]) {
  const validValues = values.filter(Number.isFinite)
  if (validValues.length === 0) {
    return { min: 0, max: 1 }
  }
  let min = Math.min(...validValues)
  let max = Math.max(...validValues)
  if (min === max) {
    const pad = Math.max(1, Math.abs(max) * 0.1)
    min -= pad
    max += pad
  }
  return { min, max }
}

function ticksFor(domain: { min: number, max: number }, count: number) {
  if (count <= 1) {
    return [domain.max]
  }
  return Array.from({ length: count }, (_, index) => domain.min + (domain.max - domain.min) * index / (count - 1))
}

function tickIndexes(count: number) {
  if (count <= 1) {
    return [0]
  }
  return Array.from(new Set([0, Math.floor(count * 0.25), Math.floor(count * 0.5), Math.floor(count * 0.75), count - 1]))
}

function pointsAttr(points: { x: number, y: number }[]) {
  return points.map(point => `${point.x.toFixed(2)},${point.y.toFixed(2)}`).join(' ')
}
</script>

<template>
  <main class="bess-public-shell">
    <div class="bess-public-frame">
      <header class="bess-public-topbar">
        <div class="bess-public-brand">
          <div
            class="bess-public-mark"
            aria-hidden="true"
          >
            <UIcon name="i-lucide-line-chart" />
          </div>
          <div>
            <p class="bess-public-subtitle">
              Forward public forecast
            </p>
            <p class="bess-public-title">
              Forecast Challenge
            </p>
          </div>
        </div>
        <nav
          class="bess-public-nav"
          aria-label="Public BESS views"
        >
          <NuxtLink to="/ukraine-bess-arbitrage-index">Index</NuxtLink>
          <NuxtLink to="/forecast-challenge">Forecast Challenge</NuxtLink>
          <NuxtLink to="/model-scoreboard">Model Scoreboard</NuxtLink>
        </nav>
      </header>

      <section class="bess-public-hero">
        <div class="bess-panel bess-panel--inset bess-hero-copy">
          <div>
            <h1>Forecasts are published before they are scored.</h1>
            <p>
              This layer separates forward NBEATSx/TFT experiments from the realized index.
              Each forecast file is versioned through GitHub commits and scored only after official OREE rows exist.
            </p>
          </div>
          <div class="bess-hero-meta">
            <span class="bess-chip">Target {{ forecastData?.target_delivery_date || 'pending' }}</span>
            <span class="bess-chip">Generated {{ forecastData?.generated_at || 'pending' }}</span>
            <span class="bess-chip">No market execution</span>
          </div>
        </div>

        <aside class="bess-score-stack">
          <div class="bess-score-primary">
            <p class="bess-score-label">
              Active forecast
            </p>
            <p class="bess-score-value">
              {{ primaryModel?.label || 'Pending source-backed history' }}
            </p>
            <p class="bess-score-meta">
              {{ primaryModel?.point_count || 0 }} hourly points · {{ primaryModel?.quality_boundary || 'not_ranked' }}
            </p>
          </div>
          <div class="bess-metric-grid">
            <div class="bess-metric">
              <span>History rows</span>
              <strong>{{ forecastData?.source?.history_row_count || 0 }}</strong>
            </div>
            <div class="bess-metric">
              <span>Models</span>
              <strong>{{ models.length }}</strong>
            </div>
            <div class="bess-metric">
              <span>Boundary</span>
              <strong>{{ forecastData?.market_execution_enabled === false ? 'Read model' : 'Unknown' }}</strong>
            </div>
          </div>
        </aside>
      </section>

      <section class="bess-section-grid">
        <div class="bess-panel bess-chart-panel">
          <div class="bess-section-header">
            <div>
              <h2>Tomorrow price forecast</h2>
              <p>Visible model rows are point-in-time snapshots, not dispatch or market bids.</p>
            </div>
          </div>
          <div
            v-if="!hasForecastPoints"
            class="bess-chart bess-empty-chart"
          >
            <strong>No materialized forecast points in this snapshot.</strong>
            <span>Source-backed history and blocked model rows stay visible until NBEATSx/TFT outputs are committed.</span>
          </div>
          <div
            v-else
            class="bess-chart-wrap"
          >
            <div class="bess-chart-legend">
              <span
                v-for="series in forecastSvg.series"
                :key="series.label"
              >
                <i
                  class="bess-legend-line"
                  :style="{ background: series.color }"
                />{{ series.label }}
              </span>
            </div>
            <svg
              class="bess-chart bess-svg-chart"
              :viewBox="`0 0 ${forecastSvg.width} ${forecastSvg.height}`"
              role="img"
              aria-label="Published price forecasts by model"
            >
              <g
                v-for="tick in forecastSvg.yTicks"
                :key="`forecast-y-${tick.label}`"
              >
                <line
                  class="bess-svg-grid"
                  :x1="58"
                  :x2="forecastSvg.width - 26"
                  :y1="tick.y"
                  :y2="tick.y"
                />
                <text
                  class="bess-svg-label"
                  :x="48"
                  :y="tick.y + 4"
                  text-anchor="end"
                >{{ tick.label }}</text>
              </g>
              <polyline
                v-for="series in forecastSvg.series"
                :key="series.label"
                class="bess-svg-line"
                :points="series.line"
                :style="{ stroke: series.color }"
              />
              <g
                v-for="tick in forecastSvg.xTicks"
                :key="`forecast-x-${tick.label}`"
              >
                <text
                  class="bess-svg-label"
                  :x="tick.x"
                  :y="forecastSvg.height - 10"
                  text-anchor="middle"
                >{{ tick.label }}</text>
              </g>
            </svg>
          </div>
        </div>

        <aside class="bess-panel bess-panel--inset">
          <div class="bess-section-header">
            <div>
              <h2>Model readiness</h2>
              <p>Questionable or unavailable model rows stay visible but unranked.</p>
            </div>
          </div>
          <ul class="bess-detail-list">
            <li
              v-for="model in models"
              :key="model.model_name"
            >
              <span>{{ model.label || model.model_name }}</span>
              <strong>{{ model.backend_status }} · {{ model.quality_boundary }}</strong>
            </li>
          </ul>
        </aside>
      </section>

      <section class="bess-panel bess-footer-note">
        Forecast Challenge rules: temporal history only, explicit `training_cutoff`, generated-before-publication
        status, rolling/temporal scoring after OREE rows appear, and no claims of guaranteed forecasts,
        external EMS integration, market bids, or deployed DT controllers.
      </section>
    </div>
  </main>
</template>
