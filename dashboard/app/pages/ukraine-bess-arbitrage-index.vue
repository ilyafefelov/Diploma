<script setup lang="ts">
import { computed, ref, watchEffect } from 'vue'
import BessDispatchField from '~/components/public/BessDispatchField.vue'

type PublicPayload = Record<string, any>
type ChartPoint = { x: number, y: number }

const SVG_WIDTH = 760
const SVG_HEIGHT = 320
const SVG_SHORT_HEIGHT = 260
const SVG_MARGIN = { top: 24, right: 28, bottom: 36, left: 64 }
const runtimeConfig = useRuntimeConfig()
const appBaseURL = String(runtimeConfig.app.baseURL || '/')

const { data: latestData } = await useFetch<PublicPayload>('/data/bess-arbitrage-index/latest.json', {
  baseURL: appBaseURL,
  key: 'public-bess-index-latest-narrative',
  server: false,
  default: () => ({ presets: [], source: {}, summary: {}, methodology: {} })
})

const { data: historyData } = await useFetch<PublicPayload>('/data/bess-arbitrage-index/history.json', {
  baseURL: appBaseURL,
  key: 'public-bess-index-history-narrative',
  server: false,
  default: () => ({ rows: [] })
})

const { data: forecastData } = await useFetch<PublicPayload>('/data/bess-arbitrage-index/forecast/latest.json', {
  baseURL: appBaseURL,
  key: 'public-bess-forecast-latest-narrative',
  server: false,
  default: () => ({ models: [], source: {} })
})

const { data: scoreboardData } = await useFetch<PublicPayload>('/data/bess-arbitrage-index/forecast_scoreboard.json', {
  baseURL: appBaseURL,
  key: 'public-bess-forecast-scoreboard-narrative',
  server: false,
  default: () => ({ rows: [], metrics: [] })
})

const { data: publicationStatusData } = await useFetch<PublicPayload>('/data/bess-arbitrage-index/publication_status.json', {
  baseURL: appBaseURL,
  key: 'public-bess-publication-status-narrative',
  server: false,
  default: () => ({ realized: {}, forecast: {}, autonomy: {}, artifacts: {} })
})

useHead({
  title: 'Ukraine BESS Arbitrage Index',
  meta: [
    {
      name: 'description',
      content: 'Source-backed public BESS arbitrage index for Ukrainian DAM prices.'
    }
  ]
})

const selectedPresetId = ref('')
const threeFallbackReason = ref('')

const presets = computed<Record<string, any>[]>(() => (
  Array.isArray(latestData.value?.presets) ? latestData.value.presets : []
))

watchEffect(() => {
  if (presets.value.length === 0) {
    return
  }
  if (!selectedPresetId.value || !presets.value.some(preset => preset.preset_id === selectedPresetId.value)) {
    const firstPreset = presets.value[0]
    if (firstPreset) {
      selectedPresetId.value = String(firstPreset.preset_id)
    }
  }
})

const selectedPreset = computed<Record<string, any> | null>(() => (
  presets.value.find(preset => preset.preset_id === selectedPresetId.value) || presets.value[0] || null
))

const selectedSchedule = computed<Record<string, any>[]>(() => (
  Array.isArray(selectedPreset.value?.hourly_schedule) ? selectedPreset.value.hourly_schedule : []
))

const selectedMetrics = computed<Record<string, any>>(() => (
  selectedPreset.value?.metrics || {}
))

const selectedBattery = computed<Record<string, any>>(() => (
  selectedPreset.value?.battery || {}
))

const source = computed<Record<string, any>>(() => (
  latestData.value?.source || {}
))

const sourceStatus = computed(() => String(source.value.source_status || 'pending_source'))
const isBlocked = computed(() => sourceStatus.value.startsWith('blocked'))
const latestGeneratedAt = computed(() => compactIso(latestData.value?.generated_at))
const forecastGeneratedAt = computed(() => compactIso(forecastData.value?.generated_at))
const deliveryDate = computed(() => String(source.value.delivery_date || 'pending'))
const rowCount = computed(() => Number(source.value.row_count || selectedSchedule.value.length || 0))

const historyRowsForPreset = computed<Record<string, any>[]>(() => (
  Array.isArray(historyData.value?.rows)
    ? historyData.value.rows.filter((row: Record<string, any>) => row.preset_id === selectedPreset.value?.preset_id)
    : []
))

const models = computed<Record<string, any>[]>(() => (
  Array.isArray(forecastData.value?.models) ? forecastData.value.models : []
))

const primaryForecast = computed<Record<string, any> | null>(() => (
  models.value.find(model => Array.isArray(model.points) && model.points.length > 0) || models.value[0] || null
))

const scoreboardRows = computed<Record<string, any>[]>(() => (
  Array.isArray(scoreboardData.value?.rows) ? scoreboardData.value.rows : []
))

const scoreboardMetrics = computed<string[]>(() => (
  Array.isArray(scoreboardData.value?.metrics) ? scoreboardData.value.metrics : []
))

const realizedPublication = computed<Record<string, any>>(() => (
  publicationStatusData.value?.realized || {}
))

const forecastPublication = computed<Record<string, any>>(() => (
  publicationStatusData.value?.forecast || {}
))

const autonomyPublication = computed<Record<string, any>>(() => (
  publicationStatusData.value?.autonomy || {}
))

const publicationGeneratedAt = computed(() => compactIso(publicationStatusData.value?.generated_at))
const realizedFreshStatus = computed(() => (
  realizedPublication.value.is_current_for_kyiv_schedule ? 'current_for_kyiv_schedule' : 'stale_or_pending'
))
const forecastFreshStatus = computed(() => (
  forecastPublication.value.is_current_for_kyiv_schedule ? 'current_for_kyiv_schedule' : 'stale_or_pending'
))

const indexMethodologyRows = computed(() => [
  {
    label: 'Observed source',
    value: source.value.source_name || 'OREE DAM hourly prices'
  },
  {
    label: 'Optimization grain',
    value: latestData.value?.methodology?.optimization_grain || 'hourly'
  },
  {
    label: 'Objective',
    value: latestData.value?.methodology?.objective || 'maximize realized arbitrage value'
  },
  {
    label: 'Terminal SoC',
    value: latestData.value?.methodology?.terminal_soc || 'final_soc_equals_initial_soc'
  },
  {
    label: 'Degradation proxy',
    value: latestData.value?.methodology?.degradation_proxy || 'pending source-backed assumption'
  },
  {
    label: 'Execution boundary',
    value: latestData.value?.claim_boundary || 'public_bess_arbitrage_index_not_market_execution'
  }
])

const promotionStages = [
  {
    stage: 'Stage 0',
    title: 'Realized deterministic index',
    body: 'Perfect-hindsight LP on official hourly DAM rows. This page is the public default.'
  },
  {
    stage: 'Stage 1',
    title: 'Forecast Challenge',
    body: 'NBEATSx, TFT and strict similar-day baselines can publish timestamped forecasts before realized rows arrive.'
  },
  {
    stage: 'Stage 2',
    title: 'Public ranking',
    body: 'Models become ranked only after 30+ realized forecast days and source-backed leakage checks.'
  },
  {
    stage: 'Stage 3',
    title: 'Schedule selection',
    body: 'Forecasts feed a read-only schedule-selection backtest with dispatch regret and value capture.'
  },
  {
    stage: 'Stage 4',
    title: 'V2+ optimizer candidate',
    body: 'V2+ can challenge the deterministic selector after rolling robustness evidence is published.'
  },
  {
    stage: 'Stage 5',
    title: 'DT / HF DT challenger',
    body: 'Decision Transformer and HF lanes stay gated research challengers, never default market execution.'
  }
]

const dispatchSvg = computed(() => {
  const rows = selectedSchedule.value
  if (rows.length === 0) {
    return null
  }
  const prices = rows.map(row => numberValue(row.price_uah_mwh))
  const powers = rows.map(row => numberValue(row.net_power_mw))
  const priceDomain = domainFor(prices)
  const maxPower = Math.max(0.001, ...powers.map(value => Math.abs(value)))
  const powerDomain = { min: -maxPower, max: maxPower }
  const xFor = xScale(rows.length, SVG_WIDTH)
  const yPrice = yScale(priceDomain, SVG_HEIGHT)
  const yPower = yScale(powerDomain, SVG_HEIGHT)
  const zeroY = yPower(0)
  const barWidth = Math.max(6, plotWidth(SVG_WIDTH) / Math.max(rows.length, 1) * 0.58)
  const priceLine = pointsAttr(rows.map((row, index) => ({
    x: xFor(index),
    y: yPrice(numberValue(row.price_uah_mwh))
  })))
  return {
    width: SVG_WIDTH,
    height: SVG_HEIGHT,
    zeroY,
    priceLine,
    bars: rows.map((row, index) => {
      const value = numberValue(row.net_power_mw)
      const y = yPower(value)
      return {
        x: xFor(index) - barWidth / 2,
        y: value >= 0 ? y : zeroY,
        width: barWidth,
        height: Math.max(1, Math.abs(zeroY - y)),
        kind: value >= 0 ? 'discharge' : 'charge'
      }
    }),
    priceTicks: ticksFor(priceDomain, 4).map(value => ({
      label: `${formatNumber(value, 0)}`,
      y: yPrice(value)
    })),
    xTicks: tickIndexes(rows.length).map(index => ({
      label: hourLabel(rows[index]?.timestamp),
      x: xFor(index)
    }))
  }
})

const socSvg = computed(() => {
  const rows = selectedSchedule.value
  if (rows.length === 0) {
    return null
  }
  const socValues = rows.map(row => numberValue(row.soc_after_mwh))
  const domain = domainFor(socValues)
  const xFor = xScale(rows.length, SVG_WIDTH)
  const yFor = yScale(domain, SVG_SHORT_HEIGHT)
  const line = pointsAttr(rows.map((row, index) => ({
    x: xFor(index),
    y: yFor(numberValue(row.soc_after_mwh))
  })))
  return {
    width: SVG_WIDTH,
    height: SVG_SHORT_HEIGHT,
    line,
    yTicks: ticksFor(domain, 4).map(value => ({
      label: `${formatNumber(value, 3)} MWh`,
      y: yFor(value)
    })),
    xTicks: tickIndexes(rows.length).map(index => ({
      label: hourLabel(rows[index]?.timestamp),
      x: xFor(index)
    }))
  }
})

const historySvg = computed(() => {
  const rows = historyRowsForPreset.value.slice(-14)
  if (rows.length === 0) {
    return null
  }
  const values = rows.map(row => numberValue(row.net_value_uah))
  const domain = domainFor([0, ...values])
  const xFor = xScale(rows.length, SVG_WIDTH)
  const yFor = yScale(domain, SVG_SHORT_HEIGHT)
  const zeroY = yFor(0)
  const barWidth = Math.max(12, plotWidth(SVG_WIDTH) / Math.max(rows.length, 1) * 0.48)
  return {
    width: SVG_WIDTH,
    height: SVG_SHORT_HEIGHT,
    zeroY,
    bars: rows.map((row, index) => {
      const value = numberValue(row.net_value_uah)
      const y = yFor(value)
      return {
        x: xFor(index) - barWidth / 2,
        y: value >= 0 ? y : zeroY,
        width: barWidth,
        height: Math.max(1, Math.abs(zeroY - y)),
        label: shortDate(row.delivery_date)
      }
    }),
    yTicks: ticksFor(domain, 4).map(value => ({
      label: formatNumber(value, 0),
      y: yFor(value)
    })),
    xTicks: tickIndexes(rows.length).map(index => ({
      label: shortDate(rows[index]?.delivery_date),
      x: xFor(index)
    }))
  }
})

function handleFieldFallback(reason: string) {
  threeFallbackReason.value = reason
}

function hourLabel(value: string | undefined): string {
  return value ? value.slice(11, 16) : ''
}

function shortDate(value: string | undefined): string {
  return value ? value.slice(5, 10) : ''
}

function compactIso(value: unknown): string {
  const text = String(value || '')
  return text ? text.replace('+00:00', 'Z').replace('+03:00', '+03').slice(0, 22).replace('T', ' ') : 'pending'
}

function numberValue(value: unknown): number {
  const numeric = Number(value || 0)
  return Number.isFinite(numeric) ? numeric : 0
}

function formatNumber(value: unknown, digits = 2): string {
  const numeric = Number(value || 0)
  const fixed = Number.isFinite(numeric) ? numeric.toFixed(digits) : (0).toFixed(digits)
  const [integerPart = '0', decimalPart] = fixed.split('.')
  const sign = integerPart.startsWith('-') ? '-' : ''
  const unsignedInteger = integerPart.replace('-', '')
  const groupedInteger = unsignedInteger.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  return decimalPart ? `${sign}${groupedInteger}.${decimalPart}` : `${sign}${groupedInteger}`
}

function formatUah(value: unknown): string {
  return `${formatNumber(value, 0)} UAH`
}

function formatMw(value: unknown): string {
  return `${formatNumber(value, 3)} MW`
}

function formatMwh(value: unknown): string {
  return `${formatNumber(value, 3)} MWh`
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
  const padding = Math.max(1, (max - min) * 0.08)
  return { min: min - padding, max: max + padding }
}

function ticksFor(domain: { min: number, max: number }, count: number) {
  if (count <= 1) {
    return [domain.max]
  }
  return Array.from({ length: count }, (_, index) => domain.min + (domain.max - domain.min) * index / (count - 1))
}

function tickIndexes(count: number) {
  if (count <= 1) {
    return count === 1 ? [0] : []
  }
  return Array.from(new Set([0, Math.floor(count / 4), Math.floor(count / 2), Math.floor(count * 3 / 4), count - 1]))
}

function pointsAttr(points: ChartPoint[]): string {
  return points.map(point => `${point.x.toFixed(2)},${point.y.toFixed(2)}`).join(' ')
}
</script>

<template>
  <main class="bess-public-shell bess-public-shell--narrative">
    <div class="bess-public-frame">
      <header class="bess-public-topbar">
        <a class="bess-public-brand" href="#index" aria-label="Ukraine BESS Arbitrage Index">
          <span class="bess-public-mark" aria-hidden="true">
            <UIcon name="i-lucide-chart-no-axes-combined" />
          </span>
          <span>
            <span class="bess-public-title">Ukraine BESS Arbitrage Index</span>
            <span class="bess-public-subtitle">Source-backed DAM dispatch research</span>
          </span>
        </a>
        <nav class="bess-public-nav" aria-label="Public index sections">
          <a href="#index">Index</a>
          <a href="#forecast">Forecast Challenge</a>
          <a href="#scoreboard">Model Scoreboard</a>
          <a href="#methodology">Methodology</a>
        </nav>
      </header>

      <section id="index" class="bess-narrative-hero" aria-labelledby="bess-index-title">
        <div class="bess-panel bess-panel--inset bess-hero-copy bess-hero-copy--narrative">
          <div>
            <div class="bess-hero-rule" aria-hidden="true">
              <span />
              <span />
              <span />
            </div>
            <h1 id="bess-index-title">
              How much value could a standard BESS capture on Ukrainian DAM prices?
            </h1>
            <p>
              A daily public receipt for Ukrainian C&I storage economics: official observed
              OREE day-ahead prices go in, deterministic battery constraints go in, and the
              published result is a no-execution arbitrage value and dispatch trace.
            </p>
          </div>

          <div class="bess-hero-meta">
            <span class="bess-chip">Delivery {{ deliveryDate }}</span>
            <span class="bess-chip">{{ rowCount }} hourly rows</span>
            <span class="bess-chip">Generated {{ latestGeneratedAt }}</span>
            <span class="bess-chip">No market execution</span>
          </div>
          <div class="bess-hero-flow" aria-label="Autonomous publication flow">
            <span>OREE rows</span>
            <span>LP dispatch</span>
            <span>GitHub JSON</span>
            <span>Static page</span>
          </div>
        </div>

        <div class="bess-hero-stage" aria-label="Animated BESS dispatch field">
          <ClientOnly>
            <BessDispatchField
              :schedule="selectedSchedule"
              :source-status="sourceStatus"
              :preset-label="selectedPreset?.label || ''"
              @fallback="handleFieldFallback"
            />
            <template #fallback>
              <div class="bess-field-fallback-shell">
                <strong>Dispatch field loading</strong>
                <span>SVG evidence charts below remain the analytical source of truth.</span>
              </div>
            </template>
          </ClientOnly>
          <p v-if="threeFallbackReason && threeFallbackReason !== 'reduced_motion'" class="bess-field-note">
            Dispatch field fallback: {{ threeFallbackReason }}. Audit charts remain available below.
          </p>
        </div>

        <aside class="bess-kpi-rail" aria-label="Headline index metrics">
          <div class="bess-score-primary bess-score-primary--light">
            <p class="bess-score-label">Headline net value</p>
            <p class="bess-score-value">{{ formatUah(selectedMetrics.net_value_uah) }}</p>
            <p class="bess-score-meta">{{ selectedPreset?.label || 'Battery preset pending' }}</p>
          </div>
          <div class="bess-metric-grid bess-metric-grid--rail">
            <div class="bess-metric">
              <span>Normalized</span>
              <strong>{{ formatNumber(selectedMetrics.normalized_uah_per_mwh_capacity, 0) }}</strong>
            </div>
            <div class="bess-metric">
              <span>Equivalent cycles</span>
              <strong>{{ formatNumber(selectedMetrics.equivalent_full_cycles, 3) }}</strong>
            </div>
            <div class="bess-metric">
              <span>Throughput</span>
              <strong>{{ formatMwh(selectedMetrics.throughput_mwh) }}</strong>
            </div>
            <div class="bess-metric">
              <span>Charge hours</span>
              <strong>{{ formatNumber(selectedMetrics.charge_hours, 0) }}</strong>
            </div>
            <div class="bess-metric">
              <span>Discharge hours</span>
              <strong>{{ formatNumber(selectedMetrics.discharge_hours, 0) }}</strong>
            </div>
            <div class="bess-metric">
              <span>Degradation</span>
              <strong>{{ formatUah(selectedMetrics.degradation_penalty_uah) }}</strong>
            </div>
          </div>
        </aside>
      </section>

      <section class="bess-source-ledger" aria-label="Source and claim boundary">
        <div class="bess-ledger-item">
          <span>Official source</span>
          <a v-if="source.source_url" :href="source.source_url" target="_blank" rel="noreferrer">
            {{ source.source_name || 'OREE DAM hourly prices' }}
          </a>
          <strong v-else>{{ source.source_name || 'OREE DAM hourly prices' }}</strong>
        </div>
        <div class="bess-ledger-item">
          <span>Status</span>
          <strong :class="{ 'bess-text-warn': isBlocked }">{{ sourceStatus }}</strong>
        </div>
        <div class="bess-ledger-item">
          <span>Claim boundary</span>
          <strong>{{ latestData?.claim_boundary || 'public_bess_arbitrage_index_not_market_execution' }}</strong>
        </div>
        <div class="bess-ledger-item">
          <span>Bid status</span>
          <strong>{{ latestData?.proposed_bid_status || 'not_emitted' }}</strong>
        </div>
      </section>

      <section class="bess-autonomy-receipt" aria-label="Autonomous publication status">
        <div class="bess-autonomy-stamp" :class="{ 'bess-autonomy-stamp--warn': realizedFreshStatus !== 'current_for_kyiv_schedule' || forecastFreshStatus !== 'current_for_kyiv_schedule' }">
          <span>Autonomous lane</span>
          <strong>{{ realizedFreshStatus === 'current_for_kyiv_schedule' && forecastFreshStatus === 'current_for_kyiv_schedule' ? 'current' : 'watch freshness' }}</strong>
        </div>
        <div class="bess-receipt-strip">
          <span>Realized expected</span>
          <strong>{{ realizedPublication.expected_delivery_date || 'pending' }}</strong>
        </div>
        <div class="bess-receipt-strip">
          <span>Realized artifact</span>
          <strong>{{ realizedPublication.actual_delivery_date || deliveryDate }}</strong>
        </div>
        <div class="bess-receipt-strip">
          <span>Forecast expected</span>
          <strong>{{ forecastPublication.expected_target_delivery_date || forecastData?.target_delivery_date || 'pending' }}</strong>
        </div>
        <div class="bess-receipt-strip">
          <span>Forecast artifact</span>
          <strong>{{ forecastPublication.actual_target_delivery_date || forecastData?.target_delivery_date || 'pending' }}</strong>
        </div>
        <div class="bess-receipt-strip">
          <span>Publisher</span>
          <strong>{{ autonomyPublication.compute_layer || 'github_actions_scheduled_static_json' }}</strong>
        </div>
        <div class="bess-receipt-strip">
          <span>Last status JSON</span>
          <strong>{{ publicationGeneratedAt }}</strong>
        </div>
      </section>

      <section class="bess-section-grid bess-section-grid--evidence" aria-label="Dispatch evidence">
        <div class="bess-panel bess-chart-panel">
          <div class="bess-section-header">
            <div>
              <p class="bess-kicker">Dispatch and price receipt</p>
              <h2>24-hour schedule evidence</h2>
              <p>
                Bars show charge and discharge power. The blue line shows observed DAM price.
                This chart is source-backed evidence, not a proposed bid.
              </p>
            </div>
            <div class="bess-segmented" aria-label="Battery preset selector">
              <button
                v-for="preset in presets"
                :key="preset.preset_id"
                type="button"
                :aria-pressed="selectedPresetId === preset.preset_id"
                @click="selectedPresetId = preset.preset_id"
              >
                {{ preset.label }}
              </button>
            </div>
          </div>

          <div v-if="dispatchSvg" class="bess-chart-wrap">
            <svg
              class="bess-chart bess-svg-chart"
              :viewBox="`0 0 ${dispatchSvg.width} ${dispatchSvg.height}`"
              role="img"
              aria-label="Dispatch power bars and DAM price line"
              preserveAspectRatio="none"
            >
              <line
                v-for="tick in dispatchSvg.priceTicks"
                :key="`price-${tick.label}`"
                class="bess-svg-grid"
                :x1="SVG_MARGIN.left"
                :x2="dispatchSvg.width - SVG_MARGIN.right"
                :y1="tick.y"
                :y2="tick.y"
              />
              <line
                class="bess-svg-axis"
                :x1="SVG_MARGIN.left"
                :x2="dispatchSvg.width - SVG_MARGIN.right"
                :y1="dispatchSvg.zeroY"
                :y2="dispatchSvg.zeroY"
              />
              <text
                v-for="tick in dispatchSvg.priceTicks"
                :key="`price-label-${tick.label}`"
                class="bess-svg-label"
                x="8"
                :y="tick.y + 4"
              >
                {{ tick.label }}
              </text>
              <rect
                v-for="(bar, index) in dispatchSvg.bars"
                :key="`bar-${index}`"
                class="bess-svg-bar"
                :class="bar.kind === 'charge' ? 'bess-svg-bar--charge' : 'bess-svg-bar--discharge'"
                :x="bar.x"
                :y="bar.y"
                :width="bar.width"
                :height="bar.height"
                rx="4"
              />
              <polyline class="bess-svg-line bess-svg-line--price" :points="dispatchSvg.priceLine" />
              <text
                v-for="tick in dispatchSvg.xTicks"
                :key="`hour-${tick.label}`"
                class="bess-svg-label"
                text-anchor="middle"
                :x="tick.x"
                :y="dispatchSvg.height - 10"
              >
                {{ tick.label }}
              </text>
            </svg>
            <div class="bess-chart-legend">
              <span><i class="bess-legend-bar bess-legend-bar--green" /> Discharge</span>
              <span><i class="bess-legend-bar bess-legend-bar--yellow" /> Charge</span>
              <span><i class="bess-legend-line" /> DAM price</span>
            </div>
          </div>
          <div v-else class="bess-empty-chart">
            <strong>No complete dispatch rows yet.</strong>
            <span>The page will populate when the daily GitHub publisher commits a complete source-backed JSON file.</span>
          </div>
        </div>

        <aside class="bess-panel bess-panel--inset">
          <div class="bess-section-header">
            <div>
              <p class="bess-kicker">Battery assumptions</p>
              <h2>Preset receipt</h2>
            </div>
          </div>
          <dl class="bess-detail-list">
            <li>
              <span>Capacity</span>
              <strong>{{ formatMwh(selectedBattery.capacity_mwh) }}</strong>
            </li>
            <li>
              <span>Power limit</span>
              <strong>{{ formatMw(selectedBattery.max_power_mw) }}</strong>
            </li>
            <li>
              <span>Duration</span>
              <strong>{{ formatNumber(selectedBattery.duration_hours, 2) }} h</strong>
            </li>
            <li>
              <span>Round-trip efficiency</span>
              <strong>{{ formatNumber(numberValue(selectedBattery.round_trip_efficiency) * 100, 1) }}%</strong>
            </li>
            <li>
              <span>SoC range</span>
              <strong>{{ formatNumber(numberValue(selectedBattery.soc_min_fraction) * 100, 0) }}-{{ formatNumber(numberValue(selectedBattery.soc_max_fraction) * 100, 0) }}%</strong>
            </li>
            <li>
              <span>Market execution</span>
              <strong>{{ selectedPreset?.market_execution_enabled ? 'enabled' : 'false' }}</strong>
            </li>
          </dl>
        </aside>
      </section>

      <section class="bess-section-grid" aria-label="State of charge and history">
        <div class="bess-panel bess-chart-panel">
          <div class="bess-section-header">
            <div>
              <p class="bess-kicker">SOC trace</p>
              <h2>State of charge after each hour</h2>
              <p>Terminal SoC is constrained to equal the initial SoC for the realized daily receipt.</p>
            </div>
          </div>
          <svg
            v-if="socSvg"
            class="bess-chart bess-chart--short bess-svg-chart"
            :viewBox="`0 0 ${socSvg.width} ${socSvg.height}`"
            role="img"
            aria-label="Battery state of charge trace"
            preserveAspectRatio="none"
          >
            <line
              v-for="tick in socSvg.yTicks"
              :key="`soc-grid-${tick.label}`"
              class="bess-svg-grid"
              :x1="SVG_MARGIN.left"
              :x2="socSvg.width - SVG_MARGIN.right"
              :y1="tick.y"
              :y2="tick.y"
            />
            <text
              v-for="tick in socSvg.yTicks"
              :key="`soc-label-${tick.label}`"
              class="bess-svg-label"
              x="8"
              :y="tick.y + 4"
            >
              {{ tick.label }}
            </text>
            <polyline class="bess-svg-line bess-svg-line--soc" :points="socSvg.line" />
            <text
              v-for="tick in socSvg.xTicks"
              :key="`soc-hour-${tick.label}`"
              class="bess-svg-label"
              text-anchor="middle"
              :x="tick.x"
              :y="socSvg.height - 10"
            >
              {{ tick.label }}
            </text>
          </svg>
          <div v-else class="bess-empty-chart">
            <strong>No SOC trace yet.</strong>
            <span>Waiting for source-backed dispatch rows.</span>
          </div>
        </div>

        <div class="bess-panel bess-chart-panel">
          <div class="bess-section-header">
            <div>
              <p class="bess-kicker">Rolling receipt</p>
              <h2>Recent realized value</h2>
              <p>History only uses committed public index rows for the selected preset.</p>
            </div>
          </div>
          <svg
            v-if="historySvg"
            class="bess-chart bess-chart--short bess-svg-chart"
            :viewBox="`0 0 ${historySvg.width} ${historySvg.height}`"
            role="img"
            aria-label="Recent realized net value bars"
            preserveAspectRatio="none"
          >
            <line
              v-for="tick in historySvg.yTicks"
              :key="`history-grid-${tick.label}`"
              class="bess-svg-grid"
              :x1="SVG_MARGIN.left"
              :x2="historySvg.width - SVG_MARGIN.right"
              :y1="tick.y"
              :y2="tick.y"
            />
            <line
              class="bess-svg-axis"
              :x1="SVG_MARGIN.left"
              :x2="historySvg.width - SVG_MARGIN.right"
              :y1="historySvg.zeroY"
              :y2="historySvg.zeroY"
            />
            <text
              v-for="tick in historySvg.yTicks"
              :key="`history-label-${tick.label}`"
              class="bess-svg-label"
              x="8"
              :y="tick.y + 4"
            >
              {{ tick.label }}
            </text>
            <rect
              v-for="(bar, index) in historySvg.bars"
              :key="`history-bar-${index}`"
              class="bess-svg-bar bess-svg-bar--discharge"
              :x="bar.x"
              :y="bar.y"
              :width="bar.width"
              :height="bar.height"
              rx="5"
            />
            <text
              v-for="tick in historySvg.xTicks"
              :key="`history-tick-${tick.label}`"
              class="bess-svg-label"
              text-anchor="middle"
              :x="tick.x"
              :y="historySvg.height - 10"
            >
              {{ tick.label }}
            </text>
          </svg>
          <div v-else class="bess-empty-chart">
            <strong>History is not populated yet.</strong>
            <span>The rolling strip appears after the first public history artifact is committed.</span>
          </div>
        </div>
      </section>

      <section id="forecast" class="bess-section-grid bess-section-grid--forecast" aria-label="Forecast challenge preview">
        <div class="bess-panel bess-panel--inset bess-forecast-panel">
          <div class="bess-section-header">
            <div>
              <p class="bess-kicker">Forecast Challenge</p>
              <h2>Forecasts stay separate from the realized index</h2>
              <p>
                Public forecasts are committed before realized rows are scored. The realized
                deterministic index above is not blended with forecast model output.
              </p>
            </div>
            <NuxtLink class="bess-technical-link" to="/forecast-challenge">
              Open technical page
            </NuxtLink>
          </div>

          <div class="bess-methodology-grid">
            <div class="bess-receipt-strip">
              <span>Target delivery</span>
              <strong>{{ forecastData?.target_delivery_date || primaryForecast?.target_delivery_date || 'pending' }}</strong>
            </div>
            <div class="bess-receipt-strip">
              <span>Generated before realization</span>
              <strong>{{ forecastGeneratedAt }}</strong>
            </div>
            <div class="bess-receipt-strip">
              <span>Training cutoff</span>
              <strong>{{ primaryForecast?.training_cutoff || forecastData?.source?.training_cutoff || 'pending' }}</strong>
            </div>
            <div class="bess-receipt-strip">
              <span>History rows</span>
              <strong>{{ formatNumber(forecastData?.source?.history_row_count, 0) }}</strong>
            </div>
          </div>
        </div>

        <aside class="bess-panel bess-panel--inset">
          <div class="bess-section-header">
            <div>
              <p class="bess-kicker">Readiness</p>
              <h2>Visible model lanes</h2>
            </div>
          </div>
          <ul class="bess-model-list">
            <li v-for="model in models" :key="model.model_name || model.label">
              <div>
                <strong>{{ model.label || model.model_name }}</strong>
                <span>{{ model.quality_boundary || 'quality_boundary_pending' }}</span>
              </div>
              <span class="bess-status" :class="{ 'bess-status--blocked': model.backend_status === 'blocked' }">
                {{ model.backend_status || model.point_in_time_status || 'pending' }}
              </span>
            </li>
            <li v-if="models.length === 0">
              <div>
                <strong>No forecast artifact yet.</strong>
                <span>The daily publisher has not committed model rows for this snapshot.</span>
              </div>
              <span class="bess-status bess-status--blocked">pending</span>
            </li>
          </ul>
        </aside>
      </section>

      <section id="scoreboard" class="bess-panel bess-chart-panel" aria-label="Model scoreboard preview">
        <div class="bess-section-header">
          <div>
            <p class="bess-kicker">Model Scoreboard</p>
            <h2>Rolling realized performance</h2>
            <p>
              Rows appear only after a forecast committed before realization can be scored
              against official OREE rows.
            </p>
          </div>
          <NuxtLink class="bess-technical-link" to="/model-scoreboard">
            Open scoreboard
          </NuxtLink>
        </div>

        <div class="bess-source-ledger bess-source-ledger--compact">
          <div class="bess-ledger-item">
            <span>Score status</span>
            <strong>{{ scoreboardData?.score_status || 'pending_realized_forecast_pairs' }}</strong>
          </div>
          <div class="bess-ledger-item">
            <span>Metrics</span>
            <strong>{{ scoreboardMetrics.join(', ') || 'MAE, RMSE, dispatch regret, value capture' }}</strong>
          </div>
          <div class="bess-ledger-item">
            <span>Rows</span>
            <strong>{{ formatNumber(scoreboardData?.row_count || scoreboardRows.length, 0) }}</strong>
          </div>
        </div>

        <div v-if="scoreboardRows.length > 0" class="bess-table-wrap">
          <table class="bess-table">
            <thead>
              <tr>
                <th>Model</th>
                <th>Window</th>
                <th>MAE</th>
                <th>RMSE</th>
                <th>Dispatch regret</th>
                <th>Value capture</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in scoreboardRows" :key="`${row.model_name}-${row.window_start}-${row.window_end}`">
                <td>{{ row.model_name }}</td>
                <td>{{ row.window_start }} to {{ row.window_end }}</td>
                <td>{{ formatNumber(row.mae_uah_mwh, 2) }}</td>
                <td>{{ formatNumber(row.rmse_uah_mwh, 2) }}</td>
                <td>{{ formatUah(row.dispatch_regret_uah) }}</td>
                <td>{{ formatNumber(row.value_capture_ratio, 3) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div v-else class="bess-empty-chart">
          <strong>No scored forecast pairs yet.</strong>
          <span>That is a useful public state: it means the page refuses to rank models before source-backed realized rows exist.</span>
        </div>
      </section>

      <section id="methodology" class="bess-section-grid bess-section-grid--methodology" aria-label="Methodology and promotion ladder">
        <div class="bess-panel bess-panel--inset">
          <div class="bess-section-header">
            <div>
              <p class="bess-kicker">Methodology receipt</p>
              <h2>What this page can and cannot claim</h2>
              <p>
                The public MVP is an autonomous GitHub Actions to GitHub Pages publication lane.
                GitHub Actions scrapes and computes JSON; the static host serves the committed artifact.
              </p>
            </div>
          </div>
          <dl class="bess-detail-list bess-detail-list--receipt">
            <li v-for="row in indexMethodologyRows" :key="row.label">
              <span>{{ row.label }}</span>
              <strong>{{ row.value }}</strong>
            </li>
            <li>
              <span>Proposed bid status</span>
              <strong>{{ latestData?.proposed_bid_status || 'not_emitted' }}</strong>
            </li>
            <li>
              <span>Utility integration claim</span>
              <strong>none</strong>
            </li>
          </dl>
        </div>

        <div class="bess-panel bess-panel--inset">
          <div class="bess-section-header">
            <div>
              <p class="bess-kicker">Promotion ladder</p>
              <h2>How ML earns visibility</h2>
              <p>Forecast models, schedule selection, V2+ and DT/HF DT move up only after rolling public evidence.</p>
            </div>
          </div>
          <ol class="bess-stage-list">
            <li v-for="stage in promotionStages" :key="stage.stage">
              <span>{{ stage.stage }}</span>
              <div>
                <strong>{{ stage.title }}</strong>
                <p>{{ stage.body }}</p>
              </div>
            </li>
          </ol>
        </div>
      </section>

      <footer class="bess-footer-note bess-panel">
        <strong>Autonomous publication path:</strong>
        GitHub Actions publishes source-backed JSON under
        <code>dashboard/public/data/bess-arbitrage-index</code>; GitHub Pages redeploys the
        generated static dashboard after JSON updates. The current workflow is scheduled for
        05:35 UTC daily and supports manual dispatch for same-day recovery. Vercel can be added
        later as a connected mirror, but runtime writes are not required for v1.
      </footer>
    </div>
  </main>
</template>
