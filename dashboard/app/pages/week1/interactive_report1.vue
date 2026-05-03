<script setup lang="ts">
import { computed, ref } from 'vue'

type MetricCard = {
  label: string
  value: string
  detail: string
}

type LayerCard = {
  label: string
  count: number
  detail: string
  color: string
}

type FlowStep = {
  phase: string
  label: string
  detail: string
}

type SourceCard = {
  label: string
  kind: string
  url?: string
  detail: string
}

type CitationCard = {
  title: string
  doi: string
  url: string
  implication: string
}

type QaItem = {
  question: string
  formula?: string
  answer: string[]
}

type FocusCard = {
  eyebrow: string
  title: string
  detail: string
}

const runtimeConfig = useRuntimeConfig()
const reportRoute = '/week1/interactive_report1'
const siteUrl = computed(() => String(runtimeConfig.public.siteUrl || 'http://localhost:64163').replace(/\/$/, ''))
const canonicalUrl = computed(() => `${siteUrl.value}${reportRoute}`)
const reportImageUrl = computed(() => `${siteUrl.value}/social/week1-interactive-report.svg`)

const formatInteger = (value: number): string => value.toLocaleString('en-US', { maximumFractionDigits: 0 })
const formatOneDecimal = (value: number): string => value.toLocaleString('en-US', {
  minimumFractionDigits: 1,
  maximumFractionDigits: 1
})

useSeoMeta({
  title: 'Week 1 Interactive Report',
  description: 'Supervisor-ready Week 1 interactive report for the Smart Arbitrage diploma MVP.',
  ogTitle: 'Week 1 Interactive Report',
  ogDescription: 'Bright supervisor-facing Nuxt report with verified MVP metrics, formulas, ELT flow, and DOI-backed rationale.',
  ogSiteName: 'Smart Arbitrage Operator',
  ogUrl: canonicalUrl,
  ogImage: reportImageUrl,
  twitterTitle: 'Week 1 Interactive Report',
  twitterDescription: 'Nuxt interactive report for tonight\'s diploma walkthrough: verified assets, API surface, formulas, and DOI-backed choices.',
  twitterImage: reportImageUrl,
  twitterCard: 'summary_large_image'
})

useHead({
  link: [
    { rel: 'canonical', href: canonicalUrl.value }
  ]
})

const focusMode = ref<'implemented' | 'planned'>('implemented')

const degradationProxyAssumptions = {
  capexUsdPerKwh: 210,
  lifetimeYears: 15,
  cyclesPerDay: 1,
  usdToUahRate: 43.9129,
  fxDate: '04.05.2026'
}

const batterySnapshot = {
  capacityMwh: 10,
  maxPowerMw: 2,
  roundTripEfficiencyPct: 95
}

const batteryCapacityKwh = batterySnapshot.capacityMwh * 1000
const lifetimeCycles = degradationProxyAssumptions.lifetimeYears * 365 * degradationProxyAssumptions.cyclesPerDay
const batteryReplacementCostUah = (
  degradationProxyAssumptions.capexUsdPerKwh
  * batteryCapacityKwh
  * degradationProxyAssumptions.usdToUahRate
)
const degradationCostPerCycleUah = batteryReplacementCostUah / lifetimeCycles
const degradationCostPerMwh = degradationCostPerCycleUah / (2 * batterySnapshot.capacityMwh)
const lifetimeCyclesLabel = formatInteger(lifetimeCycles)
const degradationCostPerCycleLabel = formatOneDecimal(degradationCostPerCycleUah)
const degradationCostPerMwhLabel = formatOneDecimal(degradationCostPerMwh)
const degradationCycleProxyWorkedExample = (
  `210 * ${formatInteger(batteryCapacityKwh)} * 43.9129 / (15 * 365) = ${degradationCostPerCycleLabel} UAH/cycle`
)
const degradationCostWorkedExample = (
  `${degradationCostPerCycleLabel} / (2 * ${batterySnapshot.capacityMwh}) = ${degradationCostPerMwhLabel} UAH/MWh`
)

const metricCards: MetricCard[] = [
  {
    label: 'Верифіковані assets',
    value: '10',
    detail: '2 bronze / 1 silver / 7 gold у MVP_DEMO_ASSETS'
  },
  {
    label: 'Control-plane API',
    value: '8',
    detail: '1 system / 1 tenants / 2 weather / 4 dashboard endpoint-и'
  },
  {
    label: 'Реальні джерела',
    value: '3 + fallback',
    detail: 'OREE, Open-Meteo, tenant registry і deterministic synthetic safety net'
  },
  {
    label: 'Research anchors',
    value: '5',
    detail: 'DOI-посилання, які прямо підтримують current scope і target architecture'
  }
]

const artifactLayers: LayerCard[] = [
  {
    label: 'Bronze',
    count: 2,
    detail: 'weather_forecast_bronze, dam_price_history',
    color: '#27a7ff'
  },
  {
    label: 'Silver',
    count: 1,
    detail: 'strict_similar_day_forecast',
    color: '#7ed321'
  },
  {
    label: 'Gold',
    count: 7,
    detail: 'metrics, telemetry, LP plan, gatekeeper, blocked demo, oracle metrics, MLflow tracking',
    color: '#0a4f8a'
  }
]

const endpointGroups: LayerCard[] = [
  {
    label: 'System',
    count: 1,
    detail: 'GET /health',
    color: '#7ed321'
  },
  {
    label: 'Tenants',
    count: 1,
    detail: 'GET /tenants',
    color: '#00b7c6'
  },
  {
    label: 'Weather',
    count: 2,
    detail: 'POST /weather/run-config, POST /weather/materialize',
    color: '#27a7ff'
  },
  {
    label: 'Dashboard',
    count: 4,
    detail: 'signal-preview, operator-status, projected-battery-state, baseline-lp-preview',
    color: '#0a4f8a'
  }
]

const flowSteps: FlowStep[] = [
  {
    phase: '01',
    label: 'Tenant registry',
    detail: 'Локація, timezone і tenant metadata приходять із YAML registry та API /tenants.'
  },
  {
    phase: '02',
    label: 'Bronze ingestion',
    detail: 'Open-Meteo дає погодні hourly features, OREE накладає live price rows поверх synthetic DAM base.'
  },
  {
    phase: '03',
    label: 'Silver + Gold',
    detail: 'strict similar-day forecast переходить у LP baseline, projected SOC preview і gatekeeper validation.'
  },
  {
    phase: '04',
    label: 'Operator surface',
    detail: 'FastAPI повертає signal preview, operator status, projected battery state і baseline LP preview.'
  }
]

const sourceCards: SourceCard[] = [
  {
    label: 'OREE market rows',
    kind: 'Live market source',
    url: 'https://www.oree.com.ua/index.php/pricectr/data_view',
    detail: 'Live DAM rows накладаються поверх deterministic synthetic base для demo stability.'
  },
  {
    label: 'Open-Meteo',
    kind: 'Live weather source',
    url: 'https://api.open-meteo.com/v1/forecast',
    detail: 'Hourly weather features і solar-derived fields для tenant-aware weather slice.'
  },
  {
    label: 'Tenant registry',
    kind: 'Configured local source',
    detail: 'Coordinates і timezone беруться з simulations/tenants.yml або fallback registry в самому репозиторії.'
  },
  {
    label: 'NREL ATB utility-scale storage',
    kind: 'Public assumption source',
    url: 'https://atb.nrel.gov/electricity/2024/utility-scale_battery_storage',
    detail: 'Дає utility-scale LIB/LFP framing, 15-year lifetime, приблизно 1 cycle/day і 85% RTE для capex-throughput proxy.'
  },
  {
    label: 'NBU FX API',
    kind: 'Official FX source',
    url: 'https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?valcode=USD&json',
    detail: 'Офіційний курс USD/UAH 43.9129 на 04.05.2026 використовується для переведення capex proxy у UAH.'
  }
]

const citationCards: CitationCard[] = [
  {
    title: 'Yi et al. — Decision-Focused Predict-then-Bid',
    doi: '10.48550/arXiv.2505.01551',
    url: 'https://doi.org/10.48550/arXiv.2505.01551',
    implication: 'Обґрунтовує target architecture для переходу від baseline PTO до DFL-парадигми в energy arbitrage.'
  },
  {
    title: 'Olivares et al. — NBEATSx for electricity prices',
    doi: '10.1016/j.ijforecast.2022.03.001',
    url: 'https://doi.org/10.1016/j.ijforecast.2022.03.001',
    implication: 'Підтримує вибір stronger forecast layer для наступного етапу після strict similar-day baseline.'
  },
  {
    title: 'Jiang et al. — Penalized TFT price forecasting',
    doi: '10.1002/for.3084',
    url: 'https://doi.org/10.1002/for.3084',
    implication: 'Пояснює, чому TFT варто тримати як кандидат для weather-aware probabilistic forecasting.'
  },
  {
    title: 'Elmachtoub and Grigas — Smart Predict, then Optimize',
    doi: '10.1287/mnsc.2020.3922',
    url: 'https://doi.org/10.1287/mnsc.2020.3922',
    implication: 'Дає канонічну точку відліку, чому baseline LP має лишатися контрольним контуром для майбутнього comparison.'
  },
  {
    title: 'Grimaldi et al. — Arbitrage with degradation-aware optimization',
    doi: '10.1016/j.est.2024.112380',
    url: 'https://doi.org/10.1016/j.est.2024.112380',
    implication: 'Підкріплює тезу, що деградацію слід вносити прямо в objective; visible paper metadata також дає capex anchor 210 USD/kWh, але не готову універсальну UAH/cycle константу.'
  }
]

const qaItems: QaItem[] = [
  {
    question: 'Чому зараз використовується проста economic penalty за деградацію?',
    formula: 'MC_deg = C_cycle / (2 * Capacity_mwh) ; Penalty_t = MC_deg * Throughput_t',
    answer: [
      'Це сумісно з LP-baseline, не руйнує контрольну постановку і вже дає оператору зрозумілий economic signal у UAH.',
      'Число тепер теж є прозорим: для demo battery береться capex anchor 210 USD/kWh, NREL ATB assumptions 15 years і ~1 cycle/day, а потім усе переводиться в UAH за офіційним курсом НБУ.',
      'Поточна модель свідомо не претендує на повний battery digital twin. Вона описує feasibility-and-economics preview model, достатню для MVP і першого walkthrough.'
    ]
  },
  {
    question: 'Як зараз реально працює data flow?',
    answer: [
      'Tenant metadata задає координати. Bronze layer збирає погоду і ринкові рядки. Silver формує strict similar-day forecast. Gold запускає LP baseline, projected SOC preview і gatekeeper validation.',
      'На виході система повертає не real market execution, а recommendation preview, projected SOC trace і operator-facing read models.'
    ]
  },
  {
    question: 'Що таке missed value у поточній demo-surface?',
    formula: 'missed_value = max(80, weather_bias * 2.4 + abs(adjusted_price - avg_adjusted_price) * 0.45)',
    answer: [
      'Це не settlement field і не LP objective. Це спрощений operator-facing opportunity score, який пояснює, чому певні години виглядають більш важливими у візуальному preview.',
      'У stronger stack ця евристика має поступитися regret, policy value gap або іншій decision-quality metric.'
    ]
  },
  {
    question: 'Що важливо не overclaim-ити сьогодні ввечері?',
    answer: [
      'Поточний MVP не виконує bidding, clearing і реальний dispatch. Також він не містить повний digital twin батареї та не реалізує DFL end-to-end.',
      'Правильне формулювання: є working baseline contour, control-plane API, operator-facing dashboard surface і чітка research trajectory до DFL.'
    ]
  }
]

const focusCards = computed<FocusCard[]>(() => focusMode.value === 'implemented'
  ? [
      {
        eyebrow: 'Current output',
        title: 'Recommendation preview замість execution',
        detail: 'Сьогоднішній deliverable — це projected SOC, baseline recommendation і control-plane explanation surface.'
      },
      {
        eyebrow: 'Battery layer',
        title: 'Feasibility-and-economics preview model',
        detail: 'Battery logic already tracks SOC bounds, power bounds, round-trip efficiency і public-source capex-throughput degradation proxy в UAH.'
      },
      {
        eyebrow: 'Operator shell',
        title: 'Nuxt + FastAPI demo contour',
        detail: 'Сторінка поєднує verified metrics, formulas, source provenance і current architecture boundary в єдину operator-facing narrative surface.'
      }
    ]
  : [
      {
        eyebrow: 'Target output',
        title: 'Proposed Bid -> Cleared Trade -> Dispatch',
        detail: 'Final version має перейти від preview semantics до канонічного market contract flow.'
      },
      {
        eyebrow: 'Forecast layer',
        title: 'NBEATSx + TFT замість strict similar-day only',
        detail: 'Більш сильний learned forecast stack має підняти якість decision inputs і пояснюваність forecast evidence.'
      },
      {
        eyebrow: 'Learning layer',
        title: 'DFL / DT як research slice',
        detail: 'LP baseline залишається control group, а DFL logic стає науковою новизною наступного етапу, а не overclaim у Week 1.'
      }
    ]
)

const maxArtifactCount = Math.max(...artifactLayers.map((item) => item.count))
const maxEndpointCount = Math.max(...endpointGroups.map((item) => item.count))
const totalEndpointCount = endpointGroups.reduce((sum, item) => sum + item.count, 0)
</script>

<template>
  <div class="report-shell">
    <div class="report-grid">
      <section class="report-hero">
        <div class="report-copy surface-panel surface-panel-strong">
          <div class="report-chip-row">
            <UBadge color="primary" variant="subtle">Week 1</UBadge>
            <UBadge color="info" variant="subtle">DAM / UAH / LP baseline</UBadge>
            <UBadge color="success" variant="subtle">Current MVP + target architecture</UBadge>
          </div>

          <p class="report-eyebrow">Interactive project report</p>
          <h1 class="report-title">Перший тиждень диплома, упакований як яскравий interactive report для керівника.</h1>
          <p class="report-summary">
            Це інтерактивний week 1 report, який збирає
            <strong>реальні repo-grounded метрики</strong>, <strong>чіткі формули</strong>,
            <strong>doi-посилання</strong> та <strong>цілісну narrative surface</strong> у світлому Sims-подібному стилі:
            багато синього, позитивний тон і чітке розділення між тим, що вже реалізовано, і тим, що лишається цільовою дослідницькою архітектурою.
          </p>

          <div class="report-action-row">
            <NuxtLink class="report-link report-link-primary" to="/operator">
              Open operator dashboard
            </NuxtLink>

            <a class="report-link report-link-secondary" href="#citations">
              Open DOI anchors
            </a>
          </div>

          <div class="report-metric-grid">
            <article v-for="card in metricCards" :key="card.label" class="report-metric-card">
              <p class="report-metric-card__label">{{ card.label }}</p>
              <p class="report-metric-card__value">{{ card.value }}</p>
              <p class="report-metric-card__detail">{{ card.detail }}</p>
            </article>
          </div>
        </div>

        <aside class="report-stage surface-panel surface-panel-blue">
          <div class="report-stage__halo"></div>
          <div class="report-stage__plumbob"></div>

          <div class="report-stage__content">
            <p class="report-eyebrow report-eyebrow-light">Current MVP status</p>
            <h2 class="report-stage__title">Поточний результат уже є демонстраційно придатним baseline contour</h2>

            <ol class="report-stage__list">
              <li>Scope свідомо обмежено: DAM-only, UAH, strict similar-day forecast і LP baseline.</li>
              <li>MVP уже має 10 verified assets і 8 endpoint-ів у current FastAPI surface.</li>
              <li>Battery layer описується як feasibility-and-economics preview model, а не як full digital twin.</li>
              <li>Наступний дослідницький крок веде до learned forecasting і DFL, але не ототожнюється з поточним deliverable.</li>
            </ol>
          </div>
        </aside>
      </section>

      <section class="focus-surface surface-panel surface-panel-blue">
        <div class="section-header">
          <div>
            <p class="report-eyebrow">Scope boundary</p>
            <h2 class="section-title">Поточний MVP і цільова дослідницька архітектура</h2>
          </div>

          <div class="mode-toggle">
            <button
              class="mode-toggle__button"
              :class="{ 'mode-toggle__button-active': focusMode === 'implemented' }"
              type="button"
              @click="focusMode = 'implemented'"
            >
              Поточний MVP
            </button>

            <button
              class="mode-toggle__button"
              :class="{ 'mode-toggle__button-active': focusMode === 'planned' }"
              type="button"
              @click="focusMode = 'planned'"
            >
              Target architecture
            </button>
          </div>
        </div>

        <div class="focus-grid">
          <article v-for="card in focusCards" :key="card.title" class="focus-card">
            <p class="focus-card__eyebrow">{{ card.eyebrow }}</p>
            <h3 class="focus-card__title">{{ card.title }}</h3>
            <p class="focus-card__detail">{{ card.detail }}</p>
          </article>
        </div>
      </section>

      <section class="chart-grid">
        <article class="report-panel surface-panel">
          <div class="section-header">
            <div>
              <p class="report-eyebrow">Dagster contour</p>
              <h2 class="section-title">MVP asset graph, який уже можна захищати словами й кодом</h2>
            </div>
            <span class="section-chip">10 assets</span>
          </div>

          <p class="section-copy">
            Це реальний Week 1 / Week 2 contour із `MVP_DEMO_ASSETS`: він уже має Bronze, Silver і Gold layers, а не лише
            намір їх створити пізніше.
          </p>

          <div class="metric-bar-list">
            <article v-for="item in artifactLayers" :key="item.label" class="metric-bar-card">
              <div class="metric-bar-card__topline">
                <span>{{ item.label }}</span>
                <strong>{{ item.count }}</strong>
              </div>
              <p class="metric-bar-card__detail">{{ item.detail }}</p>
              <div class="metric-bar-track">
                <span
                  class="metric-bar-fill"
                  :style="{
                    width: `${(item.count / maxArtifactCount) * 100}%`,
                    background: item.color
                  }"
                ></span>
              </div>
            </article>
          </div>

          <div class="mini-grid">
            <article v-for="item in artifactLayers" :key="item.label" class="mini-grid__card">
              <div class="mini-grid__topline">
                <span>{{ item.label }}</span>
                <strong>{{ item.count }}</strong>
              </div>
              <p>{{ item.detail }}</p>
            </article>
          </div>
        </article>

        <article class="report-panel surface-panel">
          <div class="section-header">
            <div>
              <p class="report-eyebrow">FastAPI surface</p>
              <h2 class="section-title">Control-plane API уже досить широкий для demo-stage walkthrough</h2>
            </div>
            <span class="section-chip">{{ totalEndpointCount }} endpoint-ів</span>
          </div>

          <p class="section-copy">
            Цей графік відокремлює service health, tenant lookup, weather actions і dashboard read models. Саме така
            декомпозиція зручна для supervisor explanation, бо вона показує, що operator shell має зрозумілі межі.
          </p>

          <div class="metric-bar-list metric-bar-list-endpoints">
            <article v-for="item in endpointGroups" :key="item.label" class="metric-bar-card metric-bar-card-endpoint">
              <div class="metric-bar-card__topline">
                <span>{{ item.label }}</span>
                <strong>{{ item.count }}</strong>
              </div>
              <p class="metric-bar-card__detail">{{ item.detail }}</p>
              <div class="metric-bar-track">
                <span
                  class="metric-bar-fill"
                  :style="{
                    width: `${(item.count / maxEndpointCount) * 100}%`,
                    background: item.color
                  }"
                ></span>
              </div>
            </article>
          </div>

          <div class="mini-grid mini-grid-endpoints">
            <article v-for="item in endpointGroups" :key="item.label" class="mini-grid__card">
              <div class="mini-grid__topline">
                <span>{{ item.label }}</span>
                <strong>{{ item.count }}</strong>
              </div>
              <p>{{ item.detail }}</p>
            </article>
          </div>
        </article>
      </section>

      <section class="report-lab-grid">
        <article class="report-panel surface-panel">
          <div class="section-header">
            <div>
              <p class="report-eyebrow">Battery economics</p>
              <h2 class="section-title">Чому current degradation proxy прозорий, а не довільний</h2>
            </div>
            <span class="section-chip">{{ degradationCostPerMwhLabel }} UAH/MWh proxy</span>
          </div>

          <p class="section-copy">
            Поточна логіка не приховує, що вона спрощена. Але число вже не є placeholder: для demo battery воно виводиться як
            capex-throughput proxy з публічних assumptions, який можна детерміновано включити в LP objective і потім пояснити прямо на demo-слайді.
          </p>

          <div class="formula-board">
            <p class="formula-board__line">MC_deg = C_cycle / (2 * Capacity_mwh)</p>
            <p class="formula-board__line">Penalty_t = MC_deg * Throughput_t</p>
            <p class="formula-board__line formula-board__line-subtle">{{ degradationCycleProxyWorkedExample }}</p>
            <p class="formula-board__line formula-board__line-subtle">{{ degradationCostWorkedExample }}</p>
          </div>

          <div class="mini-grid mini-grid-proxy">
            <article class="mini-grid__card">
              <div class="mini-grid__topline">
                <span>Capex anchor</span>
                <strong>{{ degradationProxyAssumptions.capexUsdPerKwh }} USD/kWh</strong>
              </div>
              <p>Visible battery-pack capex anchor from Grimaldi et al.</p>
            </article>

            <article class="mini-grid__card">
              <div class="mini-grid__topline">
                <span>Lifetime</span>
                <strong>{{ degradationProxyAssumptions.lifetimeYears }} years</strong>
              </div>
              <p>NREL ATB fixed-O&amp;M framing keeps rated capacity over a 15-year life.</p>
            </article>

            <article class="mini-grid__card">
              <div class="mini-grid__topline">
                <span>Cycle pace</span>
                <strong>~{{ degradationProxyAssumptions.cyclesPerDay }} / day</strong>
              </div>
              <p>Utility-scale default implies about {{ lifetimeCyclesLabel }} lifetime cycles.</p>
            </article>

            <article class="mini-grid__card">
              <div class="mini-grid__topline">
                <span>FX anchor</span>
                <strong>{{ degradationProxyAssumptions.usdToUahRate }} UAH/USD</strong>
              </div>
              <p>Official NBU rate dated {{ degradationProxyAssumptions.fxDate }}.</p>
            </article>
          </div>

          <div class="battery-grid">
            <article class="battery-grid__card">
              <span>Capacity</span>
              <strong>{{ batterySnapshot.capacityMwh }} MWh</strong>
            </article>
            <article class="battery-grid__card">
              <span>Max power</span>
              <strong>{{ batterySnapshot.maxPowerMw }} MW</strong>
            </article>
            <article class="battery-grid__card">
              <span>Round-trip efficiency</span>
              <strong>{{ batterySnapshot.roundTripEfficiencyPct }}%</strong>
            </article>
            <article class="battery-grid__card">
              <span>Cycle cost proxy</span>
              <strong>{{ degradationCostPerCycleLabel }} UAH</strong>
            </article>
            <article class="battery-grid__card battery-grid__card-accent">
              <span>Throughput proxy</span>
              <strong>{{ degradationCostPerMwhLabel }} UAH/MWh</strong>
            </article>
          </div>
        </article>

        <article class="report-panel surface-panel">
          <div class="section-header">
            <div>
              <p class="report-eyebrow">ELT walkthrough</p>
              <h2 class="section-title">Реальний data flow, який можна показати без натяжок</h2>
            </div>
            <span class="section-chip">Bronze -> Silver -> Gold</span>
          </div>

          <div class="flow-lane">
            <article v-for="step in flowSteps" :key="step.phase" class="flow-lane__card">
              <span class="flow-lane__phase">{{ step.phase }}</span>
              <h3>{{ step.label }}</h3>
              <p>{{ step.detail }}</p>
            </article>
          </div>
        </article>
      </section>

      <section class="sources-panel surface-panel">
        <div class="section-header">
          <div>
            <p class="report-eyebrow">Source provenance</p>
            <h2 class="section-title">Джерела, які можна назвати поіменно і з URL</h2>
          </div>
          <span class="section-chip">Repo-grounded</span>
        </div>

        <div class="source-grid">
          <component
            v-for="source in sourceCards"
            :key="source.label"
            :is="source.url ? 'a' : 'article'"
            :href="source.url"
            class="source-card"
            :target="source.url ? '_blank' : undefined"
            :rel="source.url ? 'noopener noreferrer' : undefined"
          >
            <p class="source-card__kind">{{ source.kind }}</p>
            <h3 class="source-card__title">{{ source.label }}</h3>
            <p class="source-card__detail">{{ source.detail }}</p>
            <span v-if="source.url" class="source-card__link">{{ source.url }}</span>
          </component>
        </div>
      </section>

      <section id="citations" class="citation-panel surface-panel surface-panel-blue">
        <div class="section-header">
          <div>
            <p class="report-eyebrow">Research anchors</p>
            <h2 class="section-title">DOI-посилання, які прямо підтримують обрані архітектурні рішення</h2>
          </div>
          <span class="section-chip">5 ключових джерел</span>
        </div>

        <div class="citation-grid">
          <a
            v-for="citation in citationCards"
            :key="citation.doi"
            :href="citation.url"
            class="citation-card"
            target="_blank"
            rel="noopener noreferrer"
          >
            <p class="citation-card__eyebrow">DOI</p>
            <h3 class="citation-card__title">{{ citation.title }}</h3>
            <p class="citation-card__detail">{{ citation.implication }}</p>
            <span class="citation-card__doi">{{ citation.doi }}</span>
          </a>
        </div>
      </section>

      <section class="qa-panel surface-panel">
        <div class="section-header">
          <div>
            <p class="report-eyebrow">Clarifying notes</p>
            <h2 class="section-title">Ключові уточнення до поточної версії системи</h2>
          </div>
          <span class="section-chip">4 відповіді</span>
        </div>

        <div class="qa-grid">
          <details v-for="item in qaItems" :key="item.question" class="qa-card">
            <summary>
              <span>{{ item.question }}</span>
              <span class="qa-card__summary-chip">{{ item.formula ? 'Formula' : 'Ready answer' }}</span>
            </summary>

            <div class="qa-card__body">
              <p v-if="item.formula" class="qa-card__formula">{{ item.formula }}</p>
              <p v-for="paragraph in item.answer" :key="paragraph">{{ paragraph }}</p>
            </div>
          </details>
        </div>
      </section>
    </div>
  </div>
</template>

<style scoped>
@import url('https://fonts.googleapis.com/css2?family=Chakra+Petch:wght@500;600;700&family=Maven+Pro:wght@400;500;700;800&display=swap');

.report-shell {
  --report-blue-strong: #0a5e9a;
  --report-blue-deep: #0d3d68;
  --report-blue-soft: #d8f2ff;
  --report-cyan: #27a7ff;
  --report-cyan-soft: rgba(39, 167, 255, 0.18);
  --report-ink: #24466d;
  --report-ink-soft: #5e7694;
  --report-green: #7ed321;
  --report-pink: #ff8fb8;
  --chart-grid-panel-max-height: min(46rem, 78vh);
  --chart-grid-visual-max-height: min(24rem, 42vh);
  position: relative;
  overflow: hidden;
  min-height: 100vh;
  padding: clamp(1.2rem, 2vw, 2rem);
  color: var(--report-ink);
}

.report-shell::before,
.report-shell::after {
  content: '';
  position: fixed;
  inset: auto;
  pointer-events: none;
  border-radius: 999px;
  filter: blur(12px);
  opacity: 0.5;
}

.report-shell::before {
  top: 3rem;
  right: 6vw;
  width: 18rem;
  height: 18rem;
  background: radial-gradient(circle, rgba(39, 167, 255, 0.28), transparent 68%);
}

.report-shell::after {
  bottom: 2rem;
  left: 4vw;
  width: 15rem;
  height: 15rem;
  background: radial-gradient(circle, rgba(126, 211, 33, 0.22), transparent 68%);
}

.report-grid {
  position: relative;
  z-index: 1;
  display: grid;
  gap: 1rem;
  max-width: 1420px;
  margin: 0 auto;
}

.report-hero {
  display: grid;
  grid-template-columns: minmax(0, 1.4fr) minmax(320px, 0.92fr);
  gap: 1rem;
  align-items: stretch;
}

.report-copy,
.report-stage,
.report-panel,
.sources-panel,
.citation-panel,
.qa-panel,
.focus-surface {
  position: relative;
  overflow: hidden;
}

.report-copy {
  display: grid;
  gap: 1rem;
  padding: clamp(1.3rem, 2vw, 2rem);
}

.report-chip-row {
  display: flex;
  flex-wrap: wrap;
  gap: 0.55rem;
}

.report-eyebrow,
.focus-card__eyebrow,
.citation-card__eyebrow,
.source-card__kind {
  margin: 0;
  font-size: 0.78rem;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--report-blue-strong);
  font-weight: 800;
}

.report-eyebrow-light {
  color: rgba(255, 255, 255, 0.78);
}

.report-title,
.report-stage__title,
.section-title,
.focus-card__title,
.citation-card__title,
.source-card__title {
  margin: 0;
  font-family: 'Chakra Petch', 'Segoe UI', sans-serif;
  line-height: 1.02;
}

.report-title {
  font-size: clamp(2.2rem, 4vw, 4.4rem);
  max-width: 14ch;
  color: var(--report-blue-deep);
  text-wrap: balance;
}

.report-summary,
.section-copy,
.focus-card__detail,
.citation-card__detail,
.source-card__detail,
.mini-grid__card p,
.metric-bar-card__detail,
.flow-lane__card p,
.qa-card__body p,
.report-metric-card__detail {
  margin: 0;
  font-family: 'Maven Pro', 'Segoe UI', sans-serif;
  color: var(--report-ink-soft);
  line-height: 1.55;
}

.report-metric-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 0.85rem;
}

.report-action-row {
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem;
}

.report-link {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 2.9rem;
  padding: 0.8rem 1.1rem;
  border-radius: 999px;
  text-decoration: none;
  font-weight: 800;
  transition: transform 140ms ease, background 140ms ease, color 140ms ease;
}

.report-link:hover {
  transform: translateY(-1px);
}

.report-link-primary {
  background: linear-gradient(135deg, var(--report-cyan), var(--report-blue-strong));
  color: #ffffff;
}

.report-link-secondary {
  background: rgba(39, 167, 255, 0.12);
  color: var(--report-blue-deep);
}

.report-metric-card,
.focus-card,
.mini-grid__card,
.battery-grid__card,
.flow-lane__card,
.artifact-stack__card,
.qa-card,
.source-card,
.citation-card,
.code-grid__card {
  border: 1px solid rgba(13, 91, 145, 0.11);
  border-radius: 1.35rem;
  background: rgba(255, 255, 255, 0.78);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.92);
}

.report-metric-card {
  position: relative;
  overflow: hidden;
  padding: 1rem;
}

.report-metric-card::before {
  content: '';
  position: absolute;
  inset: 0 auto 0 0;
  width: 0.45rem;
  border-radius: 999px;
  background: linear-gradient(180deg, var(--report-cyan), var(--report-green));
}

.report-metric-card__label,
.metric-bar-card__topline span {
  margin: 0;
  font-size: 0.82rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--report-blue-strong);
  font-weight: 800;
}

.report-metric-card__value {
  margin: 0.35rem 0 0.25rem;
  font-family: 'Chakra Petch', 'Segoe UI', sans-serif;
  font-size: clamp(1.55rem, 2.6vw, 2.35rem);
  color: var(--report-blue-deep);
}

.report-stage {
  display: grid;
  align-items: end;
  min-height: 100%;
  padding: 1.4rem;
  background:
    linear-gradient(180deg, rgba(14, 89, 145, 0.9) 0%, rgba(12, 67, 111, 0.96) 100%),
    radial-gradient(circle at top right, rgba(255, 255, 255, 0.22), transparent 36%);
  color: #eff9ff;
}

.report-stage__halo {
  position: absolute;
  inset: auto auto 2rem -2rem;
  width: 14rem;
  height: 14rem;
  border-radius: 50%;
  background: radial-gradient(circle, rgba(39, 167, 255, 0.35), transparent 68%);
}

.report-stage__plumbob {
  position: absolute;
  top: 2rem;
  right: 2rem;
  width: 7rem;
  height: 7rem;
  background: linear-gradient(135deg, #8ff16d 0%, #1dd7ff 100%);
  transform: rotate(45deg);
  border-radius: 1rem;
  box-shadow:
    0 0 0 0.4rem rgba(255, 255, 255, 0.1),
    0 24px 40px rgba(0, 0, 0, 0.22),
    inset 0 1px 0 rgba(255, 255, 255, 0.64);
}

.report-stage__content {
  position: relative;
  z-index: 1;
  display: grid;
  gap: 0.95rem;
}

.report-stage__title {
  font-size: clamp(1.9rem, 3vw, 3rem);
  color: #ffffff;
  max-width: 13ch;
}

.report-stage__list {
  display: grid;
  gap: 0.6rem;
  margin: 0;
  padding-left: 1.25rem;
  color: rgba(239, 249, 255, 0.88);
  line-height: 1.55;
}

.focus-surface {
  display: grid;
  gap: 1rem;
}

.section-header {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
}

.section-title {
  font-size: clamp(1.4rem, 2vw, 2.2rem);
  max-width: 22ch;
  color: var(--report-blue-deep);
}

.mode-toggle {
  display: inline-flex;
  flex-wrap: wrap;
  gap: 0.5rem;
}

.mode-toggle__button {
  min-height: 2.7rem;
  padding: 0.75rem 1rem;
  border: 0;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.7);
  color: var(--report-blue-deep);
  font-weight: 800;
  cursor: pointer;
  transition: transform 140ms ease, background 140ms ease, color 140ms ease;
}

.mode-toggle__button:hover {
  transform: translateY(-1px);
}

.mode-toggle__button-active {
  background: linear-gradient(135deg, var(--report-cyan), #0a5e9a);
  color: #ffffff;
}

.focus-grid,
.metric-bar-list,
.mini-grid,
.battery-grid,
.source-grid,
.citation-grid,
.qa-grid {
  display: grid;
  gap: 0.85rem;
}

.focus-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.focus-card {
  padding: 1rem;
}

.focus-card__title {
  margin-top: 0.35rem;
  font-size: 1.25rem;
  color: var(--report-blue-deep);
}

.chart-grid,
.report-lab-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 1rem;
}

.chart-grid {
  align-items: start;
  max-height: calc(var(--chart-grid-panel-max-height) + 2rem);
  overflow: hidden;
}

.chart-grid > * {
  min-height: 0;
}

.chart-grid > .report-panel {
  max-height: var(--chart-grid-panel-max-height);
  overflow: auto;
  overscroll-behavior: contain;
}

.chart-grid :deep(x-vue-echarts),
.chart-grid :deep(.echarts) {
  display: block;
  width: 100% !important;
  min-height: 0 !important;
  height: var(--chart-grid-visual-max-height) !important;
  max-height: var(--chart-grid-visual-max-height) !important;
}

.chart-grid :deep(x-vue-echarts > div),
.chart-grid :deep(.echarts > div) {
  height: var(--chart-grid-visual-max-height) !important;
  max-height: var(--chart-grid-visual-max-height) !important;
  overflow: hidden !important;
}

.chart-grid :deep(canvas),
.chart-grid :deep(svg) {
  max-height: var(--chart-grid-visual-max-height) !important;
}

.section-chip,
.qa-card__summary-chip {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 2rem;
  padding: 0.45rem 0.75rem;
  border-radius: 999px;
  background: rgba(39, 167, 255, 0.12);
  color: var(--report-blue-deep);
  font-size: 0.78rem;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: 0.08em;
}

.metric-bar-list {
  margin-top: 0.35rem;
}

.metric-bar-card {
  padding: 1rem;
  border: 1px solid rgba(13, 91, 145, 0.11);
  border-radius: 1.35rem;
  background: rgba(255, 255, 255, 0.78);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.92);
}

.metric-bar-card__topline {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
}

.metric-bar-card__topline strong {
  font-family: 'Chakra Petch', 'Segoe UI', sans-serif;
  font-size: 1.45rem;
  color: var(--report-blue-deep);
}

.metric-bar-track {
  margin-top: 0.8rem;
  height: 0.95rem;
  overflow: hidden;
  border-radius: 999px;
  background: rgba(13, 91, 145, 0.1);
}

.metric-bar-fill {
  display: block;
  height: 100%;
  min-width: 1.75rem;
  border-radius: inherit;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.35), 0 8px 20px rgba(13, 91, 145, 0.16);
}

.mini-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.mini-grid-endpoints {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.mini-grid-proxy {
  grid-template-columns: repeat(4, minmax(0, 1fr));
  margin-top: 0.9rem;
}

.mini-grid__card,
.battery-grid__card,
.metric-bar-card {
  padding: 0.9rem;
}

.mini-grid__topline,
.metric-bar-card__topline {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
}

.mini-grid__topline span,
.battery-grid__card span {
  font-size: 0.8rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--report-blue-strong);
  font-weight: 800;
}

.mini-grid__topline strong,
.battery-grid__card strong,
.metric-bar-card__topline strong {
  font-family: 'Chakra Petch', 'Segoe UI', sans-serif;
  color: var(--report-blue-deep);
}

.formula-board {
  display: grid;
  gap: 0.6rem;
  margin-top: 0.85rem;
  padding: 1rem;
  border-radius: 1.2rem;
  background: linear-gradient(145deg, rgba(39, 167, 255, 0.12), rgba(126, 211, 33, 0.12));
}

.formula-board__line,
.qa-card__formula {
  margin: 0;
  padding: 0.8rem 0.95rem;
  border-radius: 1rem;
  background: rgba(255, 255, 255, 0.86);
  color: var(--report-blue-deep);
  font-family: 'Chakra Petch', 'Segoe UI', sans-serif;
  font-weight: 700;
}

.formula-board__line-subtle {
  color: var(--report-blue-strong);
  background: rgba(255, 255, 255, 0.74);
}

.battery-grid {
  grid-template-columns: repeat(5, minmax(0, 1fr));
  margin-top: 0.9rem;
}

.battery-grid__card-accent {
  background: linear-gradient(135deg, rgba(39, 167, 255, 0.14), rgba(126, 211, 33, 0.16));
}

.flow-lane {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 0.8rem;
}

.flow-lane__card {
  position: relative;
  padding: 1rem;
}

.flow-lane__card::after {
  content: '';
  position: absolute;
  top: 50%;
  right: -0.45rem;
  width: 0.9rem;
  height: 0.9rem;
  transform: translateY(-50%) rotate(45deg);
  border-radius: 0.2rem;
  background: linear-gradient(135deg, rgba(39, 167, 255, 0.26), rgba(126, 211, 33, 0.28));
}

.flow-lane__card:last-child::after {
  display: none;
}

.flow-lane__phase {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 2.3rem;
  height: 2.3rem;
  border-radius: 0.9rem;
  background: linear-gradient(135deg, var(--report-cyan), #0a5e9a);
  color: #ffffff;
  font-family: 'Chakra Petch', 'Segoe UI', sans-serif;
  font-weight: 800;
}

.flow-lane__card h3 {
  margin: 0.75rem 0 0.35rem;
  color: var(--report-blue-deep);
  font-family: 'Chakra Petch', 'Segoe UI', sans-serif;
  font-size: 1.05rem;
}

.source-grid,
.citation-grid,
.qa-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.source-card,
.citation-card {
  display: grid;
  gap: 0.7rem;
  padding: 1rem;
  text-decoration: none;
  transition: transform 140ms ease, border-color 140ms ease, background 140ms ease;
}

.source-card:hover,
.citation-card:hover {
  transform: translateY(-2px);
  border-color: rgba(13, 91, 145, 0.24);
  background: rgba(255, 255, 255, 0.92);
}

.source-card__title,
.citation-card__title {
  font-size: 1.18rem;
  color: var(--report-blue-deep);
}

.source-card__link,
.citation-card__doi {
  font-size: 0.82rem;
  color: var(--report-blue-strong);
  word-break: break-word;
  font-weight: 700;
}

.qa-card {
  overflow: hidden;
}

.qa-card summary {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
  padding: 1rem 1.1rem;
  cursor: pointer;
  list-style: none;
  font-weight: 800;
  color: var(--report-blue-deep);
}

.qa-card summary::-webkit-details-marker {
  display: none;
}

.qa-card__body {
  display: grid;
  gap: 0.75rem;
  padding: 0 1.1rem 1.1rem;
}

@media (max-width: 1180px) {
  .report-hero,
  .chart-grid,
  .report-lab-grid {
    grid-template-columns: 1fr;
  }

  .report-metric-grid,
  .focus-grid,
  .metric-bar-list,
  .mini-grid,
  .mini-grid-endpoints,
  .battery-grid,
  .source-grid,
  .citation-grid,
  .qa-grid,
  .flow-lane {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 760px) {
  .report-shell {
    padding: 0.9rem;
  }

  .report-title {
    max-width: none;
  }

  .report-metric-grid,
  .focus-grid,
  .metric-bar-list,
  .mini-grid,
  .mini-grid-endpoints,
  .battery-grid,
  .source-grid,
  .citation-grid,
  .qa-grid,
  .flow-lane {
    grid-template-columns: 1fr;
  }

  .report-stage__plumbob {
    width: 5rem;
    height: 5rem;
  }

  .qa-card summary,
  .section-header,
  .report-chip-row,
  .mode-toggle,
  .mini-grid__topline,
  .metric-bar-card__topline {
    align-items: flex-start;
  }

  .flow-lane__card::after {
    top: auto;
    bottom: -0.45rem;
    right: 50%;
    transform: translateX(50%) rotate(45deg);
  }
}
</style>