<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watchEffect } from 'vue'
import BessDispatchField from '~/components/public/BessDispatchField.vue'

type PublicPayload = Record<string, any>
type ChartPoint = { x: number, y: number }
type BessIndexWindow = Window & {
  __bessIndexPreviousScrollRestoration?: ScrollRestoration
}

const SVG_WIDTH = 900
const SVG_HEIGHT = 340
const SVG_SHORT_HEIGHT = 260
const SVG_MARGIN = { top: 24, right: 28, bottom: 36, left: 64 }
const runtimeConfig = useRuntimeConfig()
const appBaseURL = String(runtimeConfig.app.baseURL || '/')
const siteUrl = String(runtimeConfig.public.siteUrl || 'http://localhost:64163').replace(/\/$/, '')
const canonicalUrl = `${siteUrl}/ukraine-bess-arbitrage-index`
const ogImageUrl = `${siteUrl}/og/ukraine-bess-arbitrage-index.png`
const repoUrl = 'https://github.com/ilyafefelov/Diploma'
const contactEmail = 'ilyafefelov@gmail.com'
const contactHref = `mailto:${contactEmail}?subject=${encodeURIComponent('Ukraine BESS Arbitrage Index / BESS analytics')}&body=${encodeURIComponent('Hi Illya, I saw the Ukraine BESS Arbitrage Index and would like to discuss BESS analytics / forecasting / energy optimization.')}`
const initialScrollResetScript = `(function () {
  try {
    var isBessIndex = window.location.pathname.replace(/\\/$/, '') === '/ukraine-bess-arbitrage-index';
    if (!isBessIndex) return;
    if ('scrollRestoration' in window.history) {
      if (!window.__bessIndexPreviousScrollRestoration) {
        window.__bessIndexPreviousScrollRestoration = window.history.scrollRestoration;
      }
      window.history.scrollRestoration = 'manual';
    }
    var navigation = performance.getEntriesByType('navigation')[0];
    var shouldReset = window.location.hash || (navigation && navigation.type === 'reload');
    if (!shouldReset) return;
    if (window.location.hash) {
      window.history.replaceState(null, '', window.location.pathname + window.location.search);
    }
    var reset = function () { window.scrollTo(0, 0); };
    reset();
    window.addEventListener('DOMContentLoaded', reset, { once: true });
  } catch (error) {}
}());`

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

useSeoMeta({
  title: 'Ukraine BESS Arbitrage Index | Ukrainian DAM BESS Analytics',
  description: 'Daily source-backed BESS dispatch and arbitrage index for Ukrainian OREE day-ahead prices, with forecast challenge artifacts and no market execution claims.',
  robots: 'index, follow, max-image-preview:large',
  ogTitle: 'Ukraine BESS Arbitrage Index',
  ogDescription: 'Source-backed public BESS arbitrage analytics for Ukrainian DAM prices, built from committed JSON artifacts.',
  ogUrl: canonicalUrl,
  ogType: 'website',
  ogImage: ogImageUrl,
  ogImageAlt: 'Ukraine BESS Arbitrage Index public dashboard preview with BESS dispatch field and OREE DAM evidence.',
  twitterTitle: 'Ukraine BESS Arbitrage Index',
  twitterDescription: 'Daily source-backed BESS dispatch index and forecast challenge for Ukrainian DAM prices.',
  twitterCard: 'summary_large_image',
  twitterImage: ogImageUrl,
  twitterImageAlt: 'Ukraine BESS Arbitrage Index public dashboard preview.'
})

useHead({
  link: [
    { rel: 'canonical', href: canonicalUrl },
    { rel: 'sitemap', type: 'application/xml', href: `${siteUrl}/sitemap.xml` },
    { rel: 'alternate', type: 'text/plain', href: `${siteUrl}/llms.txt`, title: 'LLMs and agent summary' }
  ],
  meta: [
    {
      name: 'keywords',
      content: 'Ukraine BESS, battery energy storage, arbitrage, OREE, DAM prices, energy optimization, forecast challenge, C&I energy analytics'
    },
    {
      name: 'application-name',
      content: 'Ukraine BESS Arbitrage Index'
    },
    {
      name: 'author',
      content: 'Illya Fefelov'
    },
    {
      property: 'og:image:width',
      content: '1200'
    },
    {
      property: 'og:image:height',
      content: '630'
    }
  ],
  script: [
    {
      key: 'bess-index-initial-scroll-reset',
      innerHTML: initialScrollResetScript
    },
    {
      type: 'application/ld+json',
      innerHTML: JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'SoftwareSourceCode',
        name: 'Ukraine BESS Arbitrage Index',
        description: 'Source-backed public BESS dispatch and arbitrage index for Ukrainian day-ahead electricity prices.',
        codeRepository: repoUrl,
        url: canonicalUrl,
        programmingLanguage: ['TypeScript', 'Vue', 'Python'],
        applicationCategory: 'Energy analytics'
      })
    },
    {
      type: 'application/ld+json',
      innerHTML: JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'Dataset',
        name: 'Ukraine BESS Arbitrage Index public JSON artifacts',
        description: 'Daily committed JSON artifacts for realized public BESS arbitrage index, forecast challenge rows, and publication status.',
        url: canonicalUrl,
        license: 'https://opensource.org/license/mit',
        creator: {
          '@type': 'Person',
          name: 'Illya Fefelov',
          email: contactEmail
        },
        distribution: [
          {
            '@type': 'DataDownload',
            encodingFormat: 'application/json',
            contentUrl: `${siteUrl}/data/bess-arbitrage-index/latest.json`
          },
          {
            '@type': 'DataDownload',
            encodingFormat: 'application/json',
            contentUrl: `${siteUrl}/data/bess-arbitrage-index/forecast/latest.json`
          },
          {
            '@type': 'DataDownload',
            encodingFormat: 'application/json',
            contentUrl: `${siteUrl}/data/bess-arbitrage-index/publication_status.json`
          }
        ],
        isBasedOn: {
          '@type': 'Dataset',
          name: 'OREE Day-Ahead Market public price rows',
          url: 'https://www.oree.com.ua/'
        }
      })
    },
    {
      type: 'application/ld+json',
      innerHTML: JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'WebSite',
        name: 'Ukraine BESS Arbitrage Index',
        url: siteUrl,
        description: 'Public source-backed BESS arbitrage index and forecast challenge for Ukrainian DAM prices.',
        inLanguage: 'en',
        publisher: {
          '@type': 'Person',
          name: 'Illya Fefelov'
        },
        potentialAction: {
          '@type': 'ContactAction',
          target: contactHref,
          name: 'Request BESS analytics demo'
        }
      })
    }
  ]
})

const selectedPresetId = ref('')
const threeFallbackReason = ref('')
const previousScrollRestoration = ref<ScrollRestoration | null>(null)
const activeSection = ref('index')
const dispatchHoverIndex = ref<number | null>(null)
const observedSectionIds = ['index', 'forecast', 'scoreboard', 'methodology', 'contact'] as const
let sectionObserver: IntersectionObserver | null = null

const presets = computed<Record<string, any>[]>(() => (
  Array.isArray(latestData.value?.presets) ? latestData.value.presets : []
))

watchEffect(() => {
  if (presets.value.length === 0) {
    return
  }
  if (!selectedPresetId.value || !presets.value.some(preset => String(preset.preset_id) === selectedPresetId.value)) {
    const firstPreset = presets.value[0]
    if (firstPreset) {
      selectedPresetId.value = String(firstPreset.preset_id)
    }
  }
})

onMounted(() => {
  if ('scrollRestoration' in window.history) {
    const bessWindow = window as BessIndexWindow
    previousScrollRestoration.value = bessWindow.__bessIndexPreviousScrollRestoration || window.history.scrollRestoration
    bessWindow.__bessIndexPreviousScrollRestoration = previousScrollRestoration.value
    window.history.scrollRestoration = 'manual'
  }

  startSectionObserver()

  const navigation = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming | undefined
  const shouldReset = Boolean(window.location.hash) || navigation?.type === 'reload'
  if (shouldReset) {
    activeSection.value = 'index'
    clearSectionHash()
    scheduleInitialTopScroll()
  }
})

onBeforeUnmount(() => {
  sectionObserver?.disconnect()
  sectionObserver = null

  if (previousScrollRestoration.value && 'scrollRestoration' in window.history) {
    window.history.scrollRestoration = previousScrollRestoration.value
    delete (window as BessIndexWindow).__bessIndexPreviousScrollRestoration
  }
})

const selectedPreset = computed<Record<string, any> | null>(() => (
  presets.value.find(preset => String(preset.preset_id) === selectedPresetId.value) || presets.value[0] || null
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

const finalSocPercent = computed(() => {
  const capacity = numberValue(selectedBattery.value.capacity_mwh)
  const lastScheduleRow = selectedSchedule.value.length > 0
    ? selectedSchedule.value[selectedSchedule.value.length - 1]
    : null
  const finalSoc = numberValue(selectedMetrics.value.final_soc_mwh ?? lastScheduleRow?.soc_after_mwh)
  if (!Number.isFinite(capacity) || capacity <= 0 || !Number.isFinite(finalSoc)) {
    return null
  }
  return Math.max(0, Math.min(100, finalSoc / capacity * 100))
})

const finalSocGaugeStyle = computed(() => ({
  '--bess-soc-percent': `${finalSocPercent.value ?? 0}%`
}))

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
const claimBoundaryRaw = computed(() => String(latestData.value?.claim_boundary || 'public_bess_arbitrage_index_not_market_execution'))
const proposedBidStatusRaw = computed(() => String(latestData.value?.proposed_bid_status || 'not_emitted'))
const publisherRaw = computed(() => String(autonomyPublication.value.compute_layer || 'github_actions_scheduled_static_json'))
const freshnessLabel = computed(() => (
  realizedFreshStatus.value === 'current_for_kyiv_schedule' && forecastFreshStatus.value === 'current_for_kyiv_schedule'
    ? 'LIVE'
    : 'WATCH'
))

const heroReceiptRows = computed(() => [
  {
    label: 'Delivery Date (EET)',
    value: deliveryDate.value,
    meta: '00:00 - 24:00'
  },
  {
    label: 'OREE DAM Rows',
    value: `${formatNumber(rowCount.value, 0)} / 24`,
    meta: sourceStatus.value.includes('complete') ? 'Complete' : sourceStatus.value
  },
  {
    label: 'Index Generated (UTC)',
    value: latestGeneratedAt.value,
    meta: 'latest.json'
  },
  {
    label: 'Point-in-Time Status',
    value: sourceStatus.value.includes('complete') ? 'As of generation' : 'Watch source',
    meta: 'No look-ahead'
  },
  {
    label: 'Market Execution',
    value: latestData.value?.market_execution_enabled ? 'Enabled' : 'Disabled',
    meta: 'No bids. No execution.'
  }
])

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
    value: receiptLabel(latestData.value?.methodology?.terminal_soc || 'final_soc_equals_initial_soc')
  },
  {
    label: 'Degradation proxy',
    value: latestData.value?.methodology?.degradation_proxy || 'pending source-backed assumption'
  },
  {
    label: 'Execution boundary',
    value: receiptLabel(latestData.value?.claim_boundary || 'public_bess_arbitrage_index_not_market_execution')
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

const connectRoutes = [
  {
    icon: 'i-lucide-factory',
    title: 'For C&I integrators',
    body: 'A public proof point for pre-sales sizing, daily dispatch economics, and transparent savings conversations.'
  },
  {
    icon: 'i-lucide-chart-no-axes-combined',
    title: 'For energy teams',
    body: 'A reproducible way to separate realized arbitrage evidence from forecast experiments and model claims.'
  },
  {
    icon: 'i-lucide-briefcase-business',
    title: 'For recruiters and investors',
    body: 'A compact portfolio artifact showing data ingestion, optimization, ML-readiness, deployment, and visual product polish.'
  }
]

const workbenchModels = computed<Record<string, any>[]>(() => models.value.slice(0, 3))
const workbenchStages = promotionStages.slice(0, 4)

const dispatchSvg = computed(() => {
  const rows = selectedSchedule.value
  if (rows.length === 0) {
    return null
  }
  const prices = rows.map(row => numberValue(row.price_uah_mwh))
  const powers = rows.map(row => numberValue(row.net_power_mw))
  const socValues = rows.map(row => numberValue(row.soc_after_mwh))
  const capacityMwh = numberValue(selectedBattery.value.capacity_mwh)
  const priceDomain = domainFor(prices)
  const maxPower = Math.max(0.001, ...powers.map(value => Math.abs(value)))
  const powerDomain = { min: -maxPower, max: maxPower }
  const socDomain = capacityMwh > 0
    ? { min: 0, max: capacityMwh }
    : domainFor(socValues)
  const xFor = xScale(rows.length, SVG_WIDTH)
  const yPrice = yScale(priceDomain, SVG_HEIGHT)
  const yPower = yScale(powerDomain, SVG_HEIGHT)
  const ySoc = yScale(socDomain, SVG_HEIGHT)
  const zeroY = yPower(0)
  const barWidth = Math.max(6, plotWidth(SVG_WIDTH) / Math.max(rows.length, 1) * 0.58)
  const points = rows.map((row, index) => ({
    x: xFor(index),
    priceY: yPrice(numberValue(row.price_uah_mwh)),
    socY: ySoc(numberValue(row.soc_after_mwh)),
    zeroY,
    hour: hourLabel(row.timestamp),
    price: numberValue(row.price_uah_mwh),
    power: numberValue(row.net_power_mw),
    soc: numberValue(row.soc_after_mwh),
    socPercent: capacityMwh > 0 ? numberValue(row.soc_after_mwh) / capacityMwh * 100 : null,
    value: numberValue(row.net_value_uah),
    tooltipX: Math.min(SVG_WIDTH - 188, Math.max(SVG_MARGIN.left + 8, xFor(index) - 86)),
    tooltipY: Math.max(SVG_MARGIN.top + 4, yPrice(numberValue(row.price_uah_mwh)) - 72),
    hitX: Math.max(SVG_MARGIN.left, xFor(index) - plotWidth(SVG_WIDTH) / Math.max(rows.length, 1) / 2),
    hitWidth: plotWidth(SVG_WIDTH) / Math.max(rows.length, 1)
  }))
  const priceLine = pointsAttr(points.map(point => ({
    x: point.x,
    y: point.priceY
  })))
  const socLine = pointsAttr(points.map(point => ({
    x: point.x,
    y: point.socY
  })))
  return {
    width: SVG_WIDTH,
    height: SVG_HEIGHT,
    zeroY,
    priceLine,
    socLine,
    points,
    bars: rows.map((row, index) => {
      const value = numberValue(row.net_power_mw)
      const y = yPower(value)
      return {
        x: xFor(index) - barWidth / 2,
        y: value >= 0 ? y : zeroY,
        width: barWidth,
        height: Math.max(1, Math.abs(zeroY - y)),
        kind: value >= 0 ? 'discharge' : 'charge',
        hour: hourLabel(row.timestamp)
      }
    }),
    priceTicks: ticksFor(priceDomain, 4).map(value => ({
      label: `${formatNumber(value, 0)}`,
      y: yPrice(value)
    })),
    socTicks: ticksFor(socDomain, 3).map(value => ({
      label: capacityMwh > 0 ? `${formatNumber(value / capacityMwh * 100, 0)}%` : formatMwh(value),
      y: ySoc(value)
    })),
    xTicks: tickIndexes(rows.length).map(index => ({
      label: hourLabel(rows[index]?.timestamp),
      x: xFor(index)
    }))
  }
})

const dispatchDefaultIndex = computed(() => {
  const rows = selectedSchedule.value
  if (rows.length === 0) {
    return null
  }
  let bestValueIndex = 0
  let bestValue = -Infinity
  rows.forEach((row, index) => {
    const value = numberValue(row.net_value_uah)
    if (value > bestValue) {
      bestValue = value
      bestValueIndex = index
    }
  })
  if (bestValue > 0.0001) {
    return bestValueIndex
  }

  let bestDispatchIndex = 0
  let bestDispatch = -1
  rows.forEach((row, index) => {
    const magnitude = Math.abs(numberValue(row.net_power_mw))
    if (magnitude > bestDispatch) {
      bestDispatch = magnitude
      bestDispatchIndex = index
    }
  })
  if (bestDispatch > 0.0001) {
    return bestDispatchIndex
  }

  let peakPriceIndex = 0
  let peakPrice = -Infinity
  rows.forEach((row, index) => {
    const price = numberValue(row.price_uah_mwh)
    if (price > peakPrice) {
      peakPrice = price
      peakPriceIndex = index
    }
  })
  return peakPriceIndex
})

const dispatchActiveIndex = computed(() => dispatchHoverIndex.value ?? dispatchDefaultIndex.value)

const dispatchActivePoint = computed(() => {
  const svg = dispatchSvg.value
  const index = dispatchActiveIndex.value
  if (!svg || index === null || index < 0 || index >= svg.points.length) {
    return null
  }
  return svg.points[index]
})

const dispatchChartMarkers = computed(() => {
  const svg = dispatchSvg.value
  if (!svg || svg.points.length === 0) {
    return []
  }
  const points = svg.points
  let peakIndex = 0
  let lowIndex = 0
  let moveIndex = 0
  let peakPrice = -Infinity
  let lowPrice = Infinity
  let largestMove = -Infinity

  points.forEach((point, index) => {
    if (point.price > peakPrice) {
      peakPrice = point.price
      peakIndex = index
    }
    if (point.price < lowPrice) {
      lowPrice = point.price
      lowIndex = index
    }
    const move = Math.abs(point.power)
    if (move > largestMove) {
      largestMove = move
      moveIndex = index
    }
  })

  const used = new Set<number>()
  const markers: Array<{
    key: string
    label: string
    value: string
    index: number
    tone: string
    style: Record<string, string>
  }> = []
  const addMarker = (key: string, label: string, index: number, value: string, tone: string) => {
    if (used.has(index)) {
      return
    }
    used.add(index)
    const point = points[index]
    if (!point) {
      return
    }
    const x = (point.x / svg.width) * 100
    const y = Math.max(8, Math.min(74, (point.priceY / svg.height) * 100 - 10))
    markers.push({
      key,
      label,
      value,
      index,
      tone,
      style: {
        '--bess-marker-left': `${x}%`,
        '--bess-marker-top': `${y}%`
      }
    })
  }

  const peakPoint = points[peakIndex]
  const lowPoint = points[lowIndex]
  const movePoint = points[moveIndex]
  addMarker('peak-price', 'Peak', peakIndex, `${formatNumber(peakPoint?.price, 0)} UAH`, 'peak')
  addMarker('low-price', 'Low', lowIndex, `${formatNumber(lowPoint?.price, 0)} UAH`, 'low')
  addMarker('largest-move', 'Move', moveIndex, formatMw(movePoint?.power), (movePoint?.power ?? 0) < 0 ? 'charge' : 'discharge')
  return markers
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

function selectDispatchPoint(index: number) {
  dispatchHoverIndex.value = index
}

function selectDispatchPointFromKeyboard(event: KeyboardEvent, index: number) {
  const points = dispatchSvg.value?.points || []
  if (points.length === 0) {
    return
  }
  let nextIndex = index
  if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
    nextIndex = Math.min(points.length - 1, index + 1)
  } else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
    nextIndex = Math.max(0, index - 1)
  } else if (event.key === 'Home') {
    nextIndex = 0
  } else if (event.key === 'End') {
    nextIndex = points.length - 1
  } else if (event.key === 'Enter' || event.key === ' ') {
    nextIndex = index
  } else {
    return
  }
  event.preventDefault()
  dispatchHoverIndex.value = nextIndex
  const target = event.currentTarget
  if (target instanceof SVGElement) {
    const hitZones = Array.from(target.parentElement?.querySelectorAll<SVGElement>('.bess-svg-hit') || [])
    hitZones[nextIndex]?.focus()
  }
}

function navigateToSection(sectionId: string) {
  activeSection.value = sectionId
  const target = document.getElementById(sectionId)
  clearSectionHash()
  if (sectionId === 'index' || !target) {
    window.scrollTo({ top: 0, left: 0, behavior: 'smooth' })
    return
  }
  target.scrollIntoView({ behavior: 'smooth', block: 'start' })
}

function startSectionObserver() {
  if (!('IntersectionObserver' in window)) {
    return
  }

  sectionObserver?.disconnect()
  const visibilityById = new Map<string, number>()
  const sections = observedSectionIds
    .map(sectionId => document.getElementById(sectionId))
    .filter((section): section is HTMLElement => Boolean(section))

  if (sections.length === 0) {
    return
  }

  sectionObserver = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      const sectionId = entry.target.id
      if (observedSectionIds.includes(sectionId as typeof observedSectionIds[number])) {
        visibilityById.set(sectionId, entry.isIntersecting ? entry.intersectionRatio : 0)
      }
    }

    const anchorY = Math.max(88, window.innerHeight * 0.22)
    let bestSection = activeSection.value
    let bestScore = Number.NEGATIVE_INFINITY

    for (const sectionId of observedSectionIds) {
      const section = document.getElementById(sectionId)
      if (!section) {
        continue
      }
      const rect = section.getBoundingClientRect()
      const proximity = Math.max(0, 1 - Math.abs(rect.top - anchorY) / Math.max(window.innerHeight, 1))
      const visibility = visibilityById.get(sectionId) || 0
      const score = proximity + visibility * 2
      if (score > bestScore) {
        bestScore = score
        bestSection = sectionId
      }
    }

    activeSection.value = bestSection
  }, {
    root: null,
    rootMargin: '-14% 0px -58% 0px',
    threshold: [0, 0.08, 0.16, 0.32, 0.5, 0.75]
  })

  for (const section of sections) {
    sectionObserver.observe(section)
  }
}

function clearSectionHash() {
  if (!window.location.hash) {
    return
  }
  window.history.replaceState(null, '', `${window.location.pathname}${window.location.search}`)
}

function scheduleInitialTopScroll() {
  resetToPageTop()
  requestAnimationFrame(() => {
    resetToPageTop()
    requestAnimationFrame(resetToPageTop)
  })
  window.setTimeout(resetToPageTop, 120)
  window.setTimeout(resetToPageTop, 320)
}

function resetToPageTop() {
  window.scrollTo({ top: 0, left: 0, behavior: 'auto' })
  document.documentElement.scrollTop = 0
  document.body.scrollTop = 0
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

function receiptLabel(value: unknown): string {
  const text = String(value || 'pending')
  const labels: Record<string, string> = {
    complete_24_hour_delivery_day: 'Complete 24-hour day',
    pending_source: 'Pending source',
    public_bess_arbitrage_index_not_market_execution: 'Public index; no market execution',
    not_emitted: 'Not emitted',
    github_actions_scheduled_static_json: 'GitHub Actions static JSON',
    current_for_kyiv_schedule: 'Current for Kyiv schedule',
    stale_or_pending: 'Stale or pending',
    final_soc_equals_initial_soc: 'Final SOC equals initial SOC'
  }
  return labels[text] || text
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, letter => letter.toUpperCase())
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
    <div class="bess-scroll-progress" aria-hidden="true" />
    <div class="bess-public-frame">
      <header class="bess-public-topbar">
        <a class="bess-public-brand" href="#index" aria-label="Ukraine BESS Arbitrage Index" @click.prevent="navigateToSection('index')">
          <span class="bess-public-mark" aria-hidden="true">
            <UIcon name="i-lucide-chart-no-axes-combined" />
          </span>
          <span>
            <span class="bess-public-title">UKRAINE BESS ARBITRAGE INDEX</span>
            <span class="bess-public-subtitle-row">
              <span class="bess-public-subtitle">Source-backed · Transparent · Reproducible</span>
              <span class="bess-research-chip">Research Lab</span>
            </span>
          </span>
        </a>
        <nav class="bess-public-nav" aria-label="Public index sections">
          <a href="#index" :class="{ 'is-active': activeSection === 'index' }" :aria-current="activeSection === 'index' ? 'page' : undefined" @click.prevent="navigateToSection('index')">
            <span>Index</span>
            <span aria-hidden="true">Index</span>
          </a>
          <a href="#forecast" :class="{ 'is-active': activeSection === 'forecast' }" :aria-current="activeSection === 'forecast' ? 'page' : undefined" @click.prevent="navigateToSection('forecast')">
            <span>Forecast Challenge</span>
            <span aria-hidden="true">Forecast</span>
          </a>
          <a href="#scoreboard" :class="{ 'is-active': activeSection === 'scoreboard' }" :aria-current="activeSection === 'scoreboard' ? 'page' : undefined" @click.prevent="navigateToSection('scoreboard')">
            <span>Model Scoreboard</span>
            <span aria-hidden="true">Models</span>
          </a>
          <a href="#methodology" :class="{ 'is-active': activeSection === 'methodology' }" :aria-current="activeSection === 'methodology' ? 'page' : undefined" @click.prevent="navigateToSection('methodology')">
            <span>Methodology</span>
            <span aria-hidden="true">Method</span>
          </a>
          <a href="#contact" :class="{ 'is-active': activeSection === 'contact' }" :aria-current="activeSection === 'contact' ? 'page' : undefined" @click.prevent="navigateToSection('contact')">
            <span>About</span>
            <span aria-hidden="true">About</span>
          </a>
        </nav>
        <div class="bess-public-actions" aria-label="Public index actions">
          <span class="bess-live-pill" :class="{ 'bess-live-pill--watch': freshnessLabel !== 'LIVE' }">
            <span aria-hidden="true" />
            {{ freshnessLabel === 'LIVE' ? 'Live Data' : 'Freshness Watch' }}
          </span>
          <a class="bess-action-link bess-action-icon" :href="repoUrl" target="_blank" rel="noreferrer" aria-label="Open GitHub source">
            <UIcon name="i-lucide-github" />
          </a>
          <a class="bess-action-link bess-action-icon" :href="contactHref" aria-label="Discuss BESS analytics">
            <UIcon name="i-lucide-send" />
          </a>
        </div>
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
              Official OREE DAM rows go in; deterministic BESS constraints come out as
              a no-execution arbitrage value, dispatch trace, and freshness receipt.
            </p>
          </div>

          <div class="bess-hero-evidence-stack" aria-label="Source and dispatch legend">
            <article class="bess-hero-source-card">
              <div>
                <span>Source</span>
                <strong>OREE (Оператор ринку)</strong>
                <em>Day-Ahead Market (DAM) · OES Ukraine</em>
              </div>
              <UIcon name="i-lucide-circle-check" />
            </article>
            <article class="bess-hero-dispatch-legend" aria-label="Dispatch legend">
              <strong>Dispatch legend</strong>
              <span><i class="bess-hero-legend-key bess-hero-legend-key--discharge" /> Discharge (to grid)</span>
              <span><i class="bess-hero-legend-key bess-hero-legend-key--charge" /> Charge (from grid)</span>
              <span><i class="bess-hero-legend-key bess-hero-legend-key--soc" /> SOC state</span>
              <span><i class="bess-hero-legend-line" /> DAM price ribbon</span>
            </article>
          </div>

          <div class="bess-hero-meta">
            <span class="bess-chip">Delivery {{ deliveryDate }}</span>
            <span class="bess-chip">{{ rowCount }} hourly rows</span>
            <span class="bess-chip">Generated {{ latestGeneratedAt }}</span>
            <span class="bess-chip">No market execution</span>
          </div>
          <div class="bess-hero-actions">
            <a class="bess-hero-cta" href="#contact" @click.prevent="navigateToSection('contact')">
              <UIcon name="i-lucide-sparkles" />
              Let's connect
            </a>
            <a class="bess-hero-cta bess-hero-cta--ghost" :href="repoUrl" target="_blank" rel="noreferrer">
              <UIcon name="i-lucide-file-json-2" />
              Audit JSON
            </a>
          </div>
          <div class="bess-hero-flow" aria-label="Autonomous publication flow">
            <span>OREE rows</span>
            <span>LP dispatch</span>
            <span>GitHub JSON</span>
            <span>Static page</span>
          </div>
        </div>

        <section class="bess-hero-receipt" aria-label="Index generation receipt">
          <article v-for="row in heroReceiptRows" :key="row.label">
            <span>{{ row.label }}</span>
            <strong>{{ row.value }}</strong>
            <em>{{ row.meta }}</em>
          </article>
        </section>

        <div class="bess-hero-stage" aria-label="Animated BESS dispatch field">
          <ClientOnly>
            <BessDispatchField
              :schedule="selectedSchedule"
              :source-status="sourceStatus"
              :preset-label="selectedPreset?.label || ''"
              :capacity-mwh="selectedBattery.capacity_mwh"
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
          <div v-if="presets.length > 1" class="bess-hero-preset-switcher" aria-label="Battery preset selector">
            <span>Selected pack</span>
            <div class="bess-hero-preset-switcher__buttons" role="group">
              <button
                v-for="preset in presets"
                :key="`hero-preset-${preset.preset_id}`"
                type="button"
                :aria-pressed="selectedPresetId === String(preset.preset_id)"
                @click="selectedPresetId = String(preset.preset_id)"
              >
                {{ preset.label }}
              </button>
            </div>
          </div>
        </div>

        <aside class="bess-hero-side" aria-label="Index side receipts">
          <article class="bess-side-card bess-side-card--soc">
            <span>End of Day SOC</span>
            <div class="bess-soc-gauge" :style="finalSocGaugeStyle">
              <strong>{{ finalSocPercent === null ? 'Pending' : `${formatNumber(finalSocPercent, 1)}%` }}</strong>
              <small>Target: {{ formatNumber(numberValue(selectedBattery.initial_soc_fraction) * 100, 0) }}%</small>
            </div>
          </article>

          <article class="bess-side-card">
            <div class="bess-side-title">
              <UIcon name="i-lucide-battery-charging" />
              <span>Selected Battery</span>
            </div>
            <dl>
              <div>
                <dt>Power</dt>
                <dd>{{ formatMw(selectedBattery.max_power_mw) }}</dd>
              </div>
              <div>
                <dt>Energy</dt>
                <dd>{{ formatMwh(selectedBattery.capacity_mwh) }}</dd>
              </div>
              <div>
                <dt>Duration</dt>
                <dd>{{ formatNumber(selectedBattery.duration_hours, 2) }} h</dd>
              </div>
              <div>
                <dt>RTE</dt>
                <dd>{{ formatNumber(numberValue(selectedBattery.round_trip_efficiency) * 100, 0) }}%</dd>
              </div>
            </dl>
            <div v-if="presets.length > 1" class="bess-battery-preset-switcher" aria-label="Battery preset selector">
              <span>Switch preset</span>
              <div class="bess-battery-preset-switcher__buttons" role="group">
                <button
                  v-for="preset in presets"
                  :key="`side-preset-${preset.preset_id}`"
                  type="button"
                  :aria-pressed="selectedPresetId === String(preset.preset_id)"
                  @click="selectedPresetId = String(preset.preset_id)"
                >
                  {{ preset.label }}
                </button>
              </div>
            </div>
          </article>

          <article class="bess-side-card">
            <div class="bess-side-title">
              <UIcon name="i-lucide-calendar-check-2" />
              <span>Data Freshness</span>
            </div>
            <p>
              <strong>{{ rowCount }}/24</strong>
              OREE DAM rows as of {{ latestGeneratedAt }}.
            </p>
          </article>
        </aside>

        <aside class="bess-kpi-rail" aria-label="Headline index metrics">
          <div class="bess-kpi-title">
            <span>Perfect-hindsight index receipt</span>
            <strong>{{ deliveryDate }}</strong>
          </div>
          <div class="bess-score-primary bess-score-primary--light">
            <UIcon name="i-lucide-coins" />
            <div>
              <p class="bess-score-label">Net value</p>
              <p class="bess-score-value">{{ formatUah(selectedMetrics.net_value_uah) }}</p>
              <p class="bess-score-meta">{{ selectedPreset?.label || 'Battery preset pending' }}</p>
            </div>
          </div>
          <div class="bess-metric-grid bess-metric-grid--rail">
            <div class="bess-metric">
              <UIcon name="i-lucide-chart-no-axes-combined" />
              <span>Normalized</span>
              <strong>{{ formatNumber(selectedMetrics.normalized_uah_per_mwh_capacity, 0) }}</strong>
            </div>
            <div class="bess-metric">
              <UIcon name="i-lucide-rotate-cw" />
              <span>Equivalent cycles</span>
              <strong>{{ formatNumber(selectedMetrics.equivalent_full_cycles, 3) }}</strong>
            </div>
            <div class="bess-metric">
              <UIcon name="i-lucide-waves" />
              <span>Throughput</span>
              <strong>{{ formatMwh(selectedMetrics.throughput_mwh) }}</strong>
            </div>
            <div class="bess-metric">
              <UIcon name="i-lucide-battery-charging" />
              <span>Charge hours</span>
              <strong>{{ formatNumber(selectedMetrics.charge_hours, 0) }}</strong>
            </div>
            <div class="bess-metric">
              <UIcon name="i-lucide-zap" />
              <span>Discharge hours</span>
              <strong>{{ formatNumber(selectedMetrics.discharge_hours, 0) }}</strong>
            </div>
            <div class="bess-metric">
              <UIcon name="i-lucide-leaf" />
              <span>Degradation</span>
              <strong>{{ formatUah(selectedMetrics.degradation_penalty_uah) }}</strong>
            </div>
          </div>
        </aside>
      </section>

      <section class="bess-first-screen-row bess-deferred-section" aria-label="First-screen evidence, forecast, and promotion summary">
        <article class="bess-panel bess-panel--inset bess-concept-chart-panel">
          <div class="bess-concept-panel-head">
            <div>
              <p class="bess-kicker">Price, Dispatch &amp; SoC</p>
              <h2>24-hour evidence</h2>
            </div>
            <span>Figure</span>
          </div>

          <div v-if="dispatchSvg" class="bess-concept-chart-wrap">
            <div class="bess-chart-marker-layer bess-chart-marker-layer--compact" aria-label="Key dispatch hour shortcuts">
              <button
                v-for="marker in dispatchChartMarkers"
                :key="`compact-marker-${marker.key}`"
                type="button"
                :class="['bess-chart-marker', `bess-chart-marker--${marker.tone}`, { 'is-active': dispatchActiveIndex === marker.index }]"
                :style="marker.style"
                :aria-pressed="dispatchActiveIndex === marker.index"
                :aria-label="`${marker.label} hour ${dispatchSvg.points[marker.index]?.hour || ''}: ${marker.value}`"
                @pointerenter="selectDispatchPoint(marker.index)"
                @focus="selectDispatchPoint(marker.index)"
                @click="selectDispatchPoint(marker.index)"
              >
                <span>{{ marker.label }}</span>
                <strong>{{ dispatchSvg.points[marker.index]?.hour }}</strong>
                <em>{{ marker.value }}</em>
              </button>
            </div>
            <svg
              class="bess-chart bess-svg-chart bess-svg-chart--compact"
              :viewBox="`0 0 ${dispatchSvg.width} ${dispatchSvg.height}`"
              role="group"
              aria-label="Compact dispatch power and DAM price evidence"
              preserveAspectRatio="none"
              @pointerleave="dispatchHoverIndex = null"
            >
              <title>Compact 24-hour dispatch and DAM price evidence</title>
              <desc>Interactive compact chart. Select an hour to inspect price, dispatch power, and net value.</desc>
              <line
                v-for="tick in dispatchSvg.priceTicks"
                :key="`compact-price-${tick.label}`"
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
                :key="`compact-price-label-${tick.label}`"
                class="bess-svg-label"
                x="8"
                :y="tick.y + 4"
              >
                {{ tick.label }}
              </text>
              <text
                v-for="tick in dispatchSvg.socTicks"
                :key="`compact-soc-label-${tick.label}`"
                class="bess-svg-label bess-svg-label--soc"
                :x="dispatchSvg.width - 8"
                :y="tick.y + 4"
                text-anchor="end"
              >
                {{ tick.label }}
              </text>
              <rect
                v-for="(bar, index) in dispatchSvg.bars"
                :key="`compact-bar-${index}`"
                class="bess-svg-bar"
                :class="bar.kind === 'charge' ? 'bess-svg-bar--charge' : 'bess-svg-bar--discharge'"
                :x="bar.x"
                :y="bar.y"
                :width="bar.width"
                :height="bar.height"
                rx="4"
                :aria-label="`${bar.hour} ${bar.kind}`"
                @pointerenter="selectDispatchPoint(index)"
              />
              <polyline class="bess-svg-line bess-svg-line--price" :points="dispatchSvg.priceLine" />
              <polyline class="bess-svg-line bess-svg-line--soc" :points="dispatchSvg.socLine" />
              <g v-if="dispatchActivePoint" class="bess-svg-active" aria-hidden="true">
                <line
                  class="bess-svg-active-line"
                  :x1="dispatchActivePoint.x"
                  :x2="dispatchActivePoint.x"
                  :y1="SVG_MARGIN.top"
                  :y2="dispatchSvg.height - SVG_MARGIN.bottom"
                />
                <circle class="bess-svg-active-ripple" :cx="dispatchActivePoint.x" :cy="dispatchActivePoint.priceY" r="18" />
                <circle class="bess-svg-active-ripple bess-svg-active-ripple--late" :cx="dispatchActivePoint.x" :cy="dispatchActivePoint.priceY" r="27" />
                <circle class="bess-svg-active-point" :cx="dispatchActivePoint.x" :cy="dispatchActivePoint.priceY" r="5" />
                <circle class="bess-svg-active-point bess-svg-active-point--soc" :cx="dispatchActivePoint.x" :cy="dispatchActivePoint.socY" r="4" />
                <g class="bess-svg-tooltip bess-svg-tooltip--compact" :transform="`translate(${dispatchActivePoint.tooltipX} ${dispatchActivePoint.tooltipY})`">
                  <rect width="170" height="64" rx="7" />
                  <text x="9" y="15">Hour {{ dispatchActivePoint.hour }}</text>
                  <text x="9" y="29">{{ formatNumber(dispatchActivePoint.price, 0) }} UAH/MWh</text>
                  <text x="9" y="43">{{ formatMw(dispatchActivePoint.power) }}</text>
                  <text x="9" y="57">SOC {{ dispatchActivePoint.socPercent === null ? formatMwh(dispatchActivePoint.soc) : `${formatNumber(dispatchActivePoint.socPercent, 1)}%` }}</text>
                  <text x="108" y="57">{{ formatUah(dispatchActivePoint.value) }}</text>
                </g>
              </g>
              <text
                v-for="tick in dispatchSvg.xTicks"
                :key="`compact-hour-${tick.label}`"
                class="bess-svg-label bess-svg-label--hour"
                text-anchor="middle"
                :x="tick.x"
                :y="dispatchSvg.height - 10"
              >
                {{ tick.label }}
              </text>
              <text
                v-for="tick in dispatchSvg.socTicks"
                :key="`soc-label-${tick.label}`"
                class="bess-svg-label bess-svg-label--soc"
                :x="dispatchSvg.width - 8"
                :y="tick.y + 4"
                text-anchor="end"
              >
                {{ tick.label }}
              </text>
              <rect
                v-for="(point, index) in dispatchSvg.points"
                :key="`compact-dispatch-hit-${index}`"
                class="bess-svg-hit"
                :x="point.hitX"
                :y="SVG_MARGIN.top"
                :width="point.hitWidth"
                :height="dispatchSvg.height - SVG_MARGIN.top - SVG_MARGIN.bottom"
                role="button"
                :tabindex="dispatchActiveIndex === index ? 0 : -1"
                :aria-label="`${point.hour}: ${formatNumber(point.price, 0)} UAH/MWh, ${formatMw(point.power)}, ${formatUah(point.value)}`"
                @pointerenter="selectDispatchPoint(index)"
                @pointerdown="selectDispatchPoint(index)"
                @click="selectDispatchPoint(index)"
                @focus="selectDispatchPoint(index)"
                @keydown="selectDispatchPointFromKeyboard($event, index)"
              />
            </svg>
            <div class="bess-chart-legend bess-chart-legend--compact">
              <span><i class="bess-legend-bar bess-legend-bar--green" /> Discharge</span>
              <span><i class="bess-legend-bar bess-legend-bar--yellow" /> Charge</span>
              <span><i class="bess-legend-line" /> DAM price</span>
              <span><i class="bess-legend-line bess-legend-line--soc" /> SOC</span>
            </div>
            <table class="bess-sr-only bess-chart-data-table">
              <caption>Compact 24-hour BESS dispatch schedule evidence</caption>
              <thead>
                <tr>
                  <th scope="col">Hour</th>
                  <th scope="col">DAM price</th>
                  <th scope="col">Dispatch power</th>
                  <th scope="col">Net value</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="point in dispatchSvg.points" :key="`compact-dispatch-row-${point.hour}`">
                  <td>{{ point.hour }}</td>
                  <td>{{ formatNumber(point.price, 0) }} UAH/MWh</td>
                  <td>{{ formatMw(point.power) }}</td>
                  <td>{{ formatUah(point.value) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div v-else class="bess-empty-chart bess-empty-chart--compact">
            <strong>No complete dispatch rows yet.</strong>
            <span>Daily JSON will populate this chart after publication.</span>
          </div>
        </article>

        <article class="bess-panel bess-panel--inset bess-concept-forecast-panel">
          <div class="bess-concept-panel-head">
            <div>
              <p class="bess-kicker">Forecast Challenge</p>
              <h2>Tomorrow lanes</h2>
            </div>
            <NuxtLink class="bess-technical-link" to="/forecast-challenge">
              Leaderboard
            </NuxtLink>
          </div>
          <div v-if="workbenchModels.length > 0" class="bess-concept-model-list">
            <button
              v-for="(model, index) in workbenchModels"
              :key="`${model.model_name || model.label || 'concept-model'}-${index}`"
              type="button"
              class="bess-concept-model-row"
              @click="navigateToSection('forecast')"
            >
              <span>
                <strong>{{ model.label || model.model_name || 'Unnamed model' }}</strong>
                <em>{{ compactIso(model.forecast_generated_at || model.generated_at || forecastGeneratedAt) }}</em>
              </span>
              <i :class="{ 'is-blocked': model.backend_status === 'blocked' }">
                {{ model.backend_status || model.point_in_time_status || 'pending' }}
              </i>
            </button>
          </div>
          <div v-else class="bess-empty-chart bess-empty-chart--compact">
            <strong>No forecast artifact yet.</strong>
            <span>Forecast rows will appear after the publisher commits a timestamped JSON artifact.</span>
          </div>
          <p class="bess-concept-footnote">Scored only after official OREE data is published.</p>
        </article>

        <article class="bess-panel bess-panel--inset bess-concept-ladder-panel">
          <div class="bess-concept-panel-head">
            <div>
              <p class="bess-kicker">Promotion Ladder</p>
              <h2>What earns trust next</h2>
            </div>
            <span>No execution</span>
          </div>
          <ol class="bess-concept-ladder">
            <li v-for="stage in workbenchStages" :key="`concept-${stage.stage}`">
              <span>{{ stage.stage.replace('Stage ', '') }}</span>
              <div>
                <strong>{{ stage.title }}</strong>
                <p>{{ stage.body }}</p>
              </div>
            </li>
          </ol>
          <a class="bess-workbench-link bess-workbench-link--primary bess-concept-cta" :href="contactHref">
            <span>Let's connect</span>
            <UIcon name="i-lucide-arrow-right" />
          </a>
        </article>
      </section>

      <section class="bess-section-grid bess-section-grid--evidence bess-deferred-section" aria-label="Dispatch evidence">
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
                :aria-pressed="selectedPresetId === String(preset.preset_id)"
                @click="selectedPresetId = String(preset.preset_id)"
              >
                {{ preset.label }}
              </button>
            </div>
          </div>

          <div v-if="dispatchSvg" class="bess-chart-wrap">
            <div class="bess-chart-marker-layer" aria-label="Key dispatch hour shortcuts">
              <button
                v-for="marker in dispatchChartMarkers"
                :key="`marker-${marker.key}`"
                type="button"
                :class="['bess-chart-marker', `bess-chart-marker--${marker.tone}`, { 'is-active': dispatchActiveIndex === marker.index }]"
                :style="marker.style"
                :aria-pressed="dispatchActiveIndex === marker.index"
                :aria-label="`${marker.label} hour ${dispatchSvg.points[marker.index]?.hour || ''}: ${marker.value}`"
                @pointerenter="selectDispatchPoint(marker.index)"
                @focus="selectDispatchPoint(marker.index)"
                @click="selectDispatchPoint(marker.index)"
              >
                <span>{{ marker.label }}</span>
                <strong>{{ dispatchSvg.points[marker.index]?.hour }}</strong>
                <em>{{ marker.value }}</em>
              </button>
            </div>
            <svg
              class="bess-chart bess-svg-chart"
              :viewBox="`0 0 ${dispatchSvg.width} ${dispatchSvg.height}`"
              role="group"
              aria-label="Dispatch power bars and DAM price line"
              preserveAspectRatio="none"
              @pointerleave="dispatchHoverIndex = null"
            >
              <title>24-hour dispatch power and DAM price evidence</title>
              <desc>Interactive source-backed chart. Select an hour to inspect price, dispatch power, and net value.</desc>
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
                :aria-label="`${bar.hour} ${bar.kind}`"
                @pointerenter="selectDispatchPoint(index)"
              />
              <polyline class="bess-svg-line bess-svg-line--price" :points="dispatchSvg.priceLine" />
              <polyline class="bess-svg-line bess-svg-line--soc" :points="dispatchSvg.socLine" />
              <g v-if="dispatchActivePoint" class="bess-svg-active" aria-hidden="true">
                <line
                  class="bess-svg-active-line"
                  :x1="dispatchActivePoint.x"
                  :x2="dispatchActivePoint.x"
                  :y1="SVG_MARGIN.top"
                  :y2="dispatchSvg.height - SVG_MARGIN.bottom"
                />
                <circle class="bess-svg-active-ripple" :cx="dispatchActivePoint.x" :cy="dispatchActivePoint.priceY" r="21" />
                <circle class="bess-svg-active-ripple bess-svg-active-ripple--late" :cx="dispatchActivePoint.x" :cy="dispatchActivePoint.priceY" r="30" />
                <circle class="bess-svg-active-point" :cx="dispatchActivePoint.x" :cy="dispatchActivePoint.priceY" r="5" />
                <circle class="bess-svg-active-point bess-svg-active-point--soc" :cx="dispatchActivePoint.x" :cy="dispatchActivePoint.socY" r="4" />
                <g class="bess-svg-tooltip" :transform="`translate(${dispatchActivePoint.tooltipX} ${dispatchActivePoint.tooltipY})`">
                  <rect width="184" height="72" rx="7" />
                  <text x="10" y="16">Hour {{ dispatchActivePoint.hour }}</text>
                  <text x="10" y="31">Price {{ formatNumber(dispatchActivePoint.price, 0) }} UAH/MWh</text>
                  <text x="10" y="46">Power {{ formatMw(dispatchActivePoint.power) }}</text>
                  <text x="10" y="61">SOC {{ dispatchActivePoint.socPercent === null ? formatMwh(dispatchActivePoint.soc) : `${formatNumber(dispatchActivePoint.socPercent, 1)}%` }}</text>
                  <text x="118" y="61">{{ formatUah(dispatchActivePoint.value) }}</text>
                </g>
              </g>
              <text
                v-for="tick in dispatchSvg.xTicks"
                :key="`hour-${tick.label}`"
                class="bess-svg-label bess-svg-label--hour"
                text-anchor="middle"
                :x="tick.x"
                :y="dispatchSvg.height - 10"
              >
                {{ tick.label }}
              </text>
              <rect
                v-for="(point, index) in dispatchSvg.points"
                :key="`dispatch-hit-${index}`"
                class="bess-svg-hit"
                :x="point.hitX"
                :y="SVG_MARGIN.top"
                :width="point.hitWidth"
                :height="dispatchSvg.height - SVG_MARGIN.top - SVG_MARGIN.bottom"
                role="button"
                :tabindex="dispatchActiveIndex === index ? 0 : -1"
                :aria-label="`${point.hour}: ${formatNumber(point.price, 0)} UAH/MWh, ${formatMw(point.power)}, ${formatUah(point.value)}`"
                @pointerenter="selectDispatchPoint(index)"
                @pointerdown="selectDispatchPoint(index)"
                @click="selectDispatchPoint(index)"
                @focus="selectDispatchPoint(index)"
                @keydown="selectDispatchPointFromKeyboard($event, index)"
              />
            </svg>
            <div class="bess-chart-legend">
              <span><i class="bess-legend-bar bess-legend-bar--green" /> Discharge</span>
              <span><i class="bess-legend-bar bess-legend-bar--yellow" /> Charge</span>
              <span><i class="bess-legend-line" /> DAM price</span>
              <span><i class="bess-legend-line bess-legend-line--soc" /> SOC</span>
            </div>
            <table class="bess-sr-only bess-chart-data-table">
              <caption>24-hour BESS dispatch schedule evidence</caption>
              <thead>
                <tr>
                  <th scope="col">Hour</th>
                  <th scope="col">DAM price</th>
                  <th scope="col">Dispatch power</th>
                  <th scope="col">Net value</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="point in dispatchSvg.points" :key="`dispatch-row-${point.hour}`">
                  <td>{{ point.hour }}</td>
                  <td>{{ formatNumber(point.price, 0) }} UAH/MWh</td>
                  <td>{{ formatMw(point.power) }}</td>
                  <td>{{ formatUah(point.value) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div v-else class="bess-empty-chart">
            <strong>No complete dispatch rows yet.</strong>
            <span>The page will populate when the daily GitHub publisher commits a complete source-backed JSON file.</span>
          </div>
        </div>

        <aside class="bess-panel bess-panel--inset bess-preset-panel">
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

      <section class="bess-proof-strip" aria-label="Source, claim, and publication proof">
        <div class="bess-source-ledger" aria-label="Source and claim boundary">
          <div class="bess-ledger-item">
            <span>Official source</span>
            <a v-if="source.source_url" :href="source.source_url" target="_blank" rel="noreferrer">
              {{ source.source_name || 'OREE DAM hourly prices' }}
            </a>
            <strong v-else>{{ source.source_name || 'OREE DAM hourly prices' }}</strong>
          </div>
          <div class="bess-ledger-item">
            <span>Status</span>
            <strong :class="{ 'bess-text-warn': isBlocked }" :title="sourceStatus">{{ receiptLabel(sourceStatus) }}</strong>
          </div>
          <div class="bess-ledger-item">
            <span>Claim boundary</span>
            <strong :title="claimBoundaryRaw">{{ receiptLabel(claimBoundaryRaw) }}</strong>
          </div>
          <div class="bess-ledger-item">
            <span>Bid status</span>
            <strong :title="proposedBidStatusRaw">{{ receiptLabel(proposedBidStatusRaw) }}</strong>
          </div>
        </div>

        <div class="bess-autonomy-receipt" aria-label="Autonomous publication status">
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
            <strong :title="publisherRaw">{{ receiptLabel(publisherRaw) }}</strong>
          </div>
          <div class="bess-receipt-strip">
            <span>Last status JSON</span>
            <strong>{{ publicationGeneratedAt }}</strong>
          </div>
        </div>
      </section>

      <section id="forecast" class="bess-section-grid bess-section-grid--forecast bess-deferred-section" aria-label="Forecast challenge preview">
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

        <aside class="bess-panel bess-panel--inset bess-model-lanes-panel">
          <div class="bess-section-header">
            <div>
              <p class="bess-kicker">Readiness</p>
              <h2>Visible model lanes</h2>
            </div>
          </div>
          <div v-if="models.length > 0" class="bess-model-table-wrap">
            <table class="bess-model-table">
              <thead>
                <tr>
                  <th>Model / strategy</th>
                  <th>Training cutoff</th>
                  <th>Generated</th>
                  <th>Status</th>
                  <th>Scored</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(model, index) in models" :key="`${model.model_name || model.label || 'model'}-${index}`">
                  <td>
                    <strong>{{ model.label || model.model_name || 'Unnamed model' }}</strong>
                    <span>{{ model.quality_boundary || model.point_in_time_status || 'quality_boundary_pending' }}</span>
                  </td>
                  <td>{{ compactIso(model.training_cutoff || forecastData?.source?.training_cutoff) }}</td>
                  <td>{{ compactIso(model.forecast_generated_at || model.generated_at || forecastGeneratedAt) }}</td>
                  <td>
                    <span class="bess-status" :class="{ 'bess-status--blocked': model.backend_status === 'blocked' }">
                      {{ model.backend_status || model.point_in_time_status || 'pending' }}
                    </span>
                  </td>
                  <td>{{ model.score_status || forecastData?.score_status || 'pending' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div v-else class="bess-empty-chart bess-empty-chart--compact">
            <strong>No forecast artifact yet.</strong>
            <span>The daily publisher has not committed model rows for this snapshot.</span>
          </div>
        </aside>
      </section>

      <section class="bess-index-workbench bess-deferred-section" aria-label="Index evidence, forecast, and lead overview">
        <article class="bess-workbench-panel bess-workbench-panel--evidence">
          <div class="bess-workbench-head">
            <div>
              <p class="bess-kicker">Evidence</p>
              <h2>Realized dispatch receipt</h2>
            </div>
            <span>{{ deliveryDate }}</span>
          </div>
          <div class="bess-workbench-number">
            <span>Net value</span>
            <strong>{{ formatUah(selectedMetrics.net_value_uah) }}</strong>
            <em>{{ selectedPreset?.label || 'selected BESS preset' }}</em>
          </div>
          <dl class="bess-workbench-stats">
            <div>
              <dt>Rows</dt>
              <dd>{{ formatNumber(rowCount, 0) }}/24</dd>
            </div>
            <div>
              <dt>Cycles</dt>
              <dd>{{ formatNumber(selectedMetrics.equivalent_full_cycles, 3) }}</dd>
            </div>
            <div>
              <dt>Throughput</dt>
              <dd>{{ formatMwh(selectedMetrics.throughput_mwh) }}</dd>
            </div>
          </dl>
          <a class="bess-workbench-link" href="#methodology" @click.prevent="navigateToSection('methodology')">
            <span>Read methodology</span>
            <UIcon name="i-lucide-arrow-right" />
          </a>
        </article>

        <article class="bess-workbench-panel bess-workbench-panel--forecast">
          <div class="bess-workbench-head">
            <div>
              <p class="bess-kicker">Forecast Challenge</p>
              <h2>Public model lanes</h2>
            </div>
            <span>{{ forecastData?.score_status || 'pending' }}</span>
          </div>
          <ul v-if="workbenchModels.length > 0" class="bess-workbench-models">
            <li v-for="(model, index) in workbenchModels" :key="`${model.model_name || model.label || 'workbench-model'}-${index}`">
              <div>
                <strong>{{ model.label || model.model_name || 'Unnamed model' }}</strong>
                <span>{{ compactIso(model.forecast_generated_at || model.generated_at || forecastGeneratedAt) }}</span>
              </div>
              <em :class="{ 'is-blocked': model.backend_status === 'blocked' }">
                {{ model.backend_status || model.point_in_time_status || 'pending' }}
              </em>
            </li>
          </ul>
          <div v-else class="bess-workbench-empty">
            <strong>No forecast artifact yet.</strong>
            <span>Forecast rows will appear after the publisher commits a timestamped JSON artifact.</span>
          </div>
          <a class="bess-workbench-link" href="#forecast" @click.prevent="navigateToSection('forecast')">
            <span>Open forecast receipt</span>
            <UIcon name="i-lucide-arrow-right" />
          </a>
        </article>

      </section>

      <section id="scoreboard" class="bess-panel bess-chart-panel bess-deferred-section" aria-label="Model scoreboard preview">
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

      <section id="methodology" class="bess-section-grid bess-section-grid--methodology bess-section-grid--methodology-single bess-deferred-section" aria-label="Methodology and claim boundary">
        <div class="bess-panel bess-panel--inset bess-claim-panel">
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
              <strong :title="proposedBidStatusRaw">{{ receiptLabel(proposedBidStatusRaw) }}</strong>
            </li>
            <li>
              <span>Utility integration claim</span>
              <strong>none</strong>
            </li>
          </dl>
        </div>
      </section>

      <section id="contact" class="bess-story-connect-panel bess-deferred-section" aria-label="Interested in the full story">
        <div class="bess-story-connect__copy">
          <p class="bess-kicker">Interested in the full story?</p>
          <h2>Connect on BESS analytics, recruiting, or research collaboration.</h2>
          <p>
            This is a public post-defense demo page: source-backed index, transparent JSON artifacts,
            forecast challenge preview, and portfolio-grade product evidence. It is not private operator
            functionality and it does not claim live market execution.
          </p>
          <dl class="bess-story-connect__receipt" aria-label="Public demo receipt">
            <div>
              <dt>Artifact type</dt>
              <dd>Public demo page</dd>
            </div>
            <div>
              <dt>Claim boundary</dt>
              <dd>No execution</dd>
            </div>
            <div>
              <dt>Best next step</dt>
              <dd>Demo / audit / PoC</dd>
            </div>
          </dl>
        </div>
        <div class="bess-story-connect__routes">
          <article v-for="route in connectRoutes" :key="route.title" class="bess-story-connect__route">
            <UIcon :name="route.icon" />
            <h3>{{ route.title }}</h3>
            <p>{{ route.body }}</p>
          </article>
        </div>
        <div class="bess-story-connect__actions">
          <div class="bess-story-connect__action-card">
            <span>Next routes</span>
            <strong>Short demo, technical deep-dive, PoC discussion, or hiring conversation.</strong>
            <ul class="bess-lead-action-list" aria-label="Collaboration routes">
              <li><UIcon name="i-lucide-check-square" /> Demo &amp; deep-dive</li>
              <li><UIcon name="i-lucide-check-square" /> Consulting / PoC</li>
              <li><UIcon name="i-lucide-check-square" /> Recruiting / collaboration</li>
            </ul>
          </div>
          <a class="bess-action-link bess-action-link--primary" :href="contactHref">
            <UIcon name="i-lucide-mail" />
            Let's connect
          </a>
          <a class="bess-action-link" :href="repoUrl" target="_blank" rel="noreferrer">
            <UIcon name="i-lucide-github" />
            Review source
          </a>
        </div>
        <div class="bess-story-connect__guard" aria-label="Public claim boundary">
          <UIcon name="i-lucide-shield-check" />
          <strong>No market execution.</strong>
          <span>No bids generated. No utility integration claim.</span>
        </div>
      </section>

      <section class="bess-section-grid bess-deferred-section bess-section-grid--trace" aria-label="State of charge and history">
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

      <footer class="bess-footer-note bess-panel">
        <span class="bess-footer-brand">Ukraine BESS Arbitrage Index</span>
        <a :href="source.source_url || 'https://www.oree.com.ua/'" target="_blank" rel="noreferrer">
          Data: OREE (oree.com.ua)
          <UIcon name="i-lucide-external-link" />
        </a>
        <a :href="repoUrl" target="_blank" rel="noreferrer">
          GitHub: transparent source of truth
          <UIcon name="i-lucide-external-link" />
        </a>
        <span>License: MIT</span>
        <span class="bess-footer-author">
          Built by a data engineer &amp; researcher
          <UIcon name="i-lucide-heart" />
        </span>
      </footer>
    </div>
  </main>
</template>
