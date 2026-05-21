<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  CURRENT_BILINGUAL_STRATEGY_EXPLAINER,
  CURRENT_DASHBOARD_EXPERIMENTS,
  CURRENT_DT_LAVA_NEXT_STEPS,
  CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE,
  CURRENT_REGRET_LADDER,
  CURRENT_TFT_SAFE_SELECTION_EXPLAINER,
  CURRENT_TFT_PORTFOLIO_DIAGNOSTICS,
  CURRENT_TFT_PORTFOLIO_CLOSURE,
  CURRENT_TFT_USE_DECISION,
  CURRENT_V2_PLUS_IMPROVEMENT_STORY,
  formatPercent,
  formatUah,
  summarizeScheduleValuePromotionReadModel
} from '~/utils/defenseDataset'
import { formatRuntimeAccelerationLabel } from '~/utils/operatorFutureStack'

const preferredTenantId = 'client_003_dnipro_factory'
const selectedTenantId = ref(preferredTenantId)
const registry = useControlPlaneRegistry()
const defense = useDefenseDashboard(selectedTenantId)
const pipelineInfographicUrl = '/design/v2-plus-pipeline-infographic.png'

const selectedTenant = computed(() => {
  return registry.tenants.value.find(tenant => tenant.tenant_id === selectedTenantId.value) || null
})

const futureForecastRows = computed(() => {
  return defense.futureStack.value?.forecast_series
    .filter(series => series.model_name.includes('nbeatsx') || series.model_name.includes('tft'))
    .map(series => ({
      modelName: series.model_name,
      modelFamily: series.model_family,
      sourceStatus: series.source_status,
      uncertaintyKind: series.uncertainty_kind,
      pointCount: series.points.length,
      firstForecast: series.points[0]?.forecast_price_uah_mwh ?? null,
      lastForecast: series.points.at(-1)?.forecast_price_uah_mwh ?? null,
      meanRegretUah: series.mean_regret_uah,
      winRate: series.win_rate
    })) ?? []
})

const futureBackendStatusText = computed(() => {
  const statusEntries = Object.entries(defense.futureStack.value?.backend_status ?? {})
  const runtimeText = formatRuntimeAccelerationLabel(defense.futureStack.value?.runtime_acceleration)
  if (statusEntries.length === 0) {
    return `official backend status not loaded / runtime ${runtimeText}`
  }

  return `${statusEntries.map(([name, status]) => `${name}: ${status}`).join(' / ')} / runtime ${runtimeText}`
})

const dtPolicySummary = computed(() => {
  const preview = defense.dtPolicyPreview.value
  if (!preview) {
    return null
  }

  return {
    readiness: preview.policy_readiness,
    rows: preview.row_count,
    violations: preview.constraint_violation_count,
    meanValueGap: preview.mean_value_gap_uah,
    valueVsHold: preview.total_value_vs_hold_uah,
    stateFeatures: preview.policy_state_features.join(', '),
    valueInterpretation: preview.policy_value_interpretation,
    operatorBoundary: preview.operator_boundary,
    boundary: preview.academic_scope
  }
})

const latestBatterySoc = computed(() => {
  const telemetrySoc = defense.batteryState.value?.latest_telemetry?.current_soc
  const hourlySoc = defense.batteryState.value?.hourly_snapshot?.soc_close

  if (typeof telemetrySoc === 'number') {
    return formatPercent(telemetrySoc)
  }

  if (typeof hourlySoc === 'number') {
    return formatPercent(hourlySoc)
  }

  return 'unavailable'
})

const thesisEvidence = computed(() => [
  {
    label: 'Offline V2+',
    value: formatUah(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.meanRegretUah),
    note: `${formatPercent(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.improvementVsStrict)} vs strict / ${CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.rollingPassCount}/${CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.rollingWindowCount} rolling`,
    tooltipTitle: 'Offline Strategy Promotion headline',
    tooltipBody: 'Current strongest thesis evidence: Ukrainian-only official global-panel NBEATSx Schedule/Value Learner V2+. This card is evidence/read-model language, not live dispatch.',
    tooltipFormula: 'promotion = strict LP/oracle regret gate, market_execution_enabled=false'
  },
  {
    label: 'Control baseline',
    value: formatUah(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.strictMeanRegretUah),
    note: 'strict_similar_day mean regret',
    tooltipTitle: 'Control baseline',
    tooltipBody: 'Frozen strict similar-day comparator from the current V2+ evidence packet. It remains the default fallback/control.',
    tooltipFormula: 'mean_regret = avg(oracle_value_uah - decision_value_uah)'
  },
  {
    label: 'TFT portfolio',
    value: `${CURRENT_TFT_PORTFOLIO_CLOSURE.rollingPassCount}/${CURRENT_TFT_PORTFOLIO_CLOSURE.rollingWindowCount}`,
    note: `${CURRENT_TFT_PORTFOLIO_CLOSURE.tftBetterCandidateCount}/${CURRENT_TFT_PORTFOLIO_CLOSURE.latestTenantAnchors} local opportunities`,
    tooltipTitle: 'Latest closed TFT portfolio test',
    tooltipBody: CURRENT_TFT_PORTFOLIO_CLOSURE.interpretation,
    tooltipFormula: `candidate_portfolio_rows=${CURRENT_TFT_PORTFOLIO_CLOSURE.candidatePortfolioRows}`
  },
  {
    label: 'Observed anchors',
    value: defense.benchmarkSummary.value ? `${defense.benchmarkSummary.value.anchorCount}` : 'unavailable',
    note: defense.benchmarkSummary.value?.dataQualityTier || 'not materialized',
    tooltipTitle: 'Observed anchors',
    tooltipBody: 'Count of rolling-origin evaluation timestamps with observed DAM and required exogenous coverage.',
    tooltipFormula: 'anchor_count = count(unique forecast origins with thesis-grade rows)'
  },
  {
    label: 'Battery truth',
    value: latestBatterySoc.value,
    note: defense.batteryState.value?.fallback_reason || defense.batteryState.value?.hourly_snapshot?.telemetry_freshness || 'live telemetry',
    tooltipTitle: 'Battery truth',
    tooltipBody: 'Physical battery state from telemetry when available, otherwise latest hourly Silver snapshot.',
    tooltipFormula: 'SOC = latest_telemetry.current_soc ?? hourly_snapshot.soc_close'
  }
])

const offlinePromotionRows = computed(() => defense.offlineStrategyPromotion.value?.rows ?? [])

const offlinePromotionReadModelLabel = computed(() => (
  summarizeScheduleValuePromotionReadModel(defense.offlineStrategyPromotion.value)
))

const regretLadderMax = computed(() => Math.max(
  ...CURRENT_REGRET_LADDER.map(point => point.meanRegretUah),
  1
))

const regretLadderRows = computed(() => CURRENT_REGRET_LADDER.map(point => ({
  ...point,
  barWidthPercent: Math.max(8, Math.round((point.meanRegretUah / regretLadderMax.value) * 100))
})))

const tftPortfolioRows = computed(() => CURRENT_TFT_PORTFOLIO_DIAGNOSTICS.map(point => ({
  ...point,
  percentLabel: point.denominator === 0 ? 'n/a' : formatPercent(point.numerator / point.denominator),
  barWidthPercent: point.denominator === 0 ? 0 : Math.max(4, Math.round((point.numerator / point.denominator) * 100))
})))

const narrativeSteps = [
  {
    label: '1. Headline',
    text: 'V2+ is the current Ukrainian-only Offline Strategy Promotion result: 174.77 UAH mean regret and 4/4 rolling windows.'
  },
  {
    label: '2. Control',
    text: 'strict_similar_day remains the frozen fallback and comparator. The dashboard does not switch live strategy defaults.'
  },
  {
    label: '3. TFT portfolio',
    text: 'TFT contributed 24/90 local post-hoc schedule opportunities, but the prior-only selector could not safely pick them and rolling replay failed 0/4.'
  },
  {
    label: '4. Evidence path',
    text: 'Forecasts become feasible schedules, schedules are strict-scored by LP/oracle regret, and claims stay offline/read-model only.'
  },
  {
    label: '5. Next',
    text: 'The next research branch is DT/LAVA-style candidate or schedule-neighbor supervision against V2+, not another dashboard default.'
  }
]

const claimBoundaries = [
  'thesis-grade only when source rows are observed and complete',
  'strict_similar_day remains default comparator',
  'V2+ is offline/read-model evidence, not live market execution',
  'TFT schedules are candidate evidence only until a prior-only selector beats V2+ robustly',
  'DT/LAVA work is next research, not deployed policy'
]

const errorRows = computed(() => {
  return Object.entries(defense.errors.value).map(([key, message]) => ({
    key,
    message
  }))
})

const formatDateTime = (value: string | null | undefined): string => {
  if (!value) {
    return 'unavailable'
  }

  return new Date(value).toLocaleString('en-GB', {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  })
}

const refresh = async (): Promise<void> => {
  await defense.loadDefenseDashboard()
}

onMounted(async () => {
  await registry.loadTenants()
  const selectedTenantExists = registry.tenants.value.some(tenant => tenant.tenant_id === selectedTenantId.value)
  if (!selectedTenantExists && registry.tenants.value[0]) {
    selectedTenantId.value = registry.tenants.value[0].tenant_id
  }

  await refresh()
})

useHead({
  title: 'Research Defense Dashboard | Smart Arbitrage'
})
</script>

<template>
  <main class="defense-shell">
    <header class="defense-topbar">
      <NuxtLink
        class="brand-link"
        to="/operator"
      >
        <UIcon name="i-lucide-arrow-left" />
        Operator
      </NuxtLink>
      <div class="topbar-controls">
        <label class="tenant-picker">
          <span>Tenant</span>
          <select v-model="selectedTenantId">
            <option
              v-for="tenant in registry.tenants.value"
              :key="tenant.tenant_id"
              :value="tenant.tenant_id"
            >
              {{ tenant.name || tenant.tenant_id }}
            </option>
          </select>
        </label>
        <button
          class="icon-button"
          type="button"
          :disabled="defense.isLoading.value"
          @click="refresh"
        >
          <UIcon name="i-lucide-refresh-cw" />
          Refresh
        </button>
      </div>
    </header>

    <section class="defense-hero">
      <div class="hero-copy">
        <p class="eyebrow">
          Research defense / FastAPI read model
        </p>
        <h1>Current result: V2+ improves Ukrainian BESS arbitrage offline, TFT portfolio did not replace it.</h1>
        <p class="hero-body">
          This route keeps the demo focused on the latest evidence from this week: V2+ is the headline Offline Strategy
          Promotion result, while TFT portfolio, market coupling, and DT/LAVA remain bounded research branches.
        </p>
        <div class="tenant-context">
          <span>{{ selectedTenant?.name || selectedTenantId }}</span>
          <span>{{ selectedTenant?.type || 'tenant' }}</span>
          <span>Loaded {{ defense.lastLoadedLabel.value }}</span>
          <span v-if="defense.activeErrorCount.value > 0">{{ defense.activeErrorCount.value }} API gaps</span>
        </div>
      </div>

      <div class="metric-grid">
        <article
          v-for="metric in thesisEvidence"
          :key="metric.label"
          class="metric-tile"
          tabindex="0"
        >
          <span>{{ metric.label }}</span>
          <strong>{{ metric.value }}</strong>
          <small>{{ metric.note }}</small>
          <span
            class="defense-tooltip"
            role="tooltip"
          >
            <strong>{{ metric.tooltipTitle }}</strong>
            <span>{{ metric.tooltipBody }}</span>
            <em>{{ metric.tooltipFormula }}</em>
          </span>
        </article>
      </div>
    </section>

    <section
      class="narrative-band"
      aria-label="Defense narrative"
    >
      <article
        v-for="step in narrativeSteps"
        :key="step.label"
        class="narrative-step"
      >
        <span>{{ step.label }}</span>
        <p>{{ step.text }}</p>
      </article>
    </section>

    <section class="pipeline-visual-panel">
      <div class="pipeline-visual-copy">
        <p class="eyebrow">
          V2+ pipeline visual
        </p>
        <h2>How the offline result is produced</h2>
        <p class="section-explainer">
          Ukrainian DAM history, weather/load context, official global-panel NBEATSx forecasts, candidate schedules,
          and strict LP/oracle scoring are joined into a read-model evidence path. The image is illustrative; the metric
          cards and charts below remain the deterministic source of numeric evidence.
        </p>
        <div class="pipeline-stat-strip">
          <article>
            <span>Input boundary</span>
            <strong>Ukraine only</strong>
            <small>OREE DAM, Open-Meteo/weather, tenant context</small>
          </article>
          <article>
            <span>Decision target</span>
            <strong>Regret</strong>
            <small>oracle_value_uah - decision_value_uah</small>
          </article>
          <article>
            <span>Execution claim</span>
            <strong>false</strong>
            <small>market_execution_enabled</small>
          </article>
        </div>
      </div>
      <figure class="pipeline-figure">
        <img
          :src="pipelineInfographicUrl"
          alt="V2+ offline schedule value pipeline infographic"
        >
        <figcaption>
          Generated dashboard visual: V2+ pipeline from Ukrainian source-backed features to strict LP/oracle evidence.
        </figcaption>
      </figure>
    </section>

    <section class="bilingual-explainer-panel">
      <div class="section-heading">
        <div>
          <p class="eyebrow">
            Bilingual strategy explainer
          </p>
          <h2>Offline V2+ today, DT/LAVA next</h2>
          <p class="section-explainer">
            A plain-language explanation for ML reviewers and thesis defense: what V2+ is, why it is not live execution,
            how data moves through the pipeline, and how this becomes the teacher path for DT/LAVA.
          </p>
        </div>
        <span class="source-pill">EN / UA</span>
      </div>
      <div class="bilingual-explainer-grid">
        <article
          v-for="section in CURRENT_BILINGUAL_STRATEGY_EXPLAINER"
          :key="section.label"
          class="bilingual-explainer-card"
        >
          <header>
            <span>{{ section.label }}</span>
          </header>
          <div class="language-columns">
            <div class="language-column">
              <p class="language-kicker">
                English
              </p>
              <h3>{{ section.englishTitle }}</h3>
              <p>{{ section.englishBody }}</p>
              <ul>
                <li
                  v-for="bullet in section.englishBullets"
                  :key="`en-${section.label}-${bullet}`"
                >
                  {{ bullet }}
                </li>
              </ul>
            </div>
            <div class="language-column language-column--ua">
              <p class="language-kicker">
                Українською
              </p>
              <h3>{{ section.ukrainianTitle }}</h3>
              <p>{{ section.ukrainianBody }}</p>
              <ul>
                <li
                  v-for="bullet in section.ukrainianBullets"
                  :key="`ua-${section.label}-${bullet}`"
                >
                  {{ bullet }}
                </li>
              </ul>
            </div>
          </div>
        </article>
      </div>
    </section>

    <section class="offline-promotion-panel">
      <div>
        <p class="eyebrow">
          Offline Strategy Promotion
        </p>
        <h2>Current thesis headline remains V2+</h2>
        <p class="section-explainer">
          The strongest current result is Ukrainian-only official global-panel NBEATSx Schedule/Value Learner V2+:
          {{ formatUah(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.meanRegretUah) }} mean regret,
          {{ formatPercent(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.improvementVsStrict) }} better than strict,
          and {{ CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.rollingPassCount }}/{{ CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.rollingWindowCount }}
          rolling windows. It is still read-model evidence only.
        </p>
      </div>
      <div class="offline-promotion-metrics">
        <article>
          <span>Strict baseline</span>
          <strong>{{ formatUah(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.strictMeanRegretUah) }}</strong>
        </article>
        <article>
          <span>Market execution</span>
          <strong>{{ CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.marketExecutionEnabled ? 'enabled' : 'false' }}</strong>
        </article>
        <article>
          <span>Backend gate</span>
          <strong>{{ offlinePromotionReadModelLabel }}</strong>
        </article>
      </div>
      <div
        v-if="offlinePromotionRows.length > 0"
        class="offline-promotion-rows"
      >
        <div class="evidence-scope-note evidence-scope-note--wide">
          <UIcon name="i-lucide-info" />
          <p>
            The fixed V2+ headline above comes from the frozen 365-anchor evidence packet. Rows below are FastAPI
            read-model rows from the available gate endpoint, so NBEATSx/TFT UAH values may reflect older compact or
            source-specific evidence and are kept for traceability, not as the headline comparator.
          </p>
        </div>
        <article
          v-for="row in offlinePromotionRows"
          :key="row.source_model_name"
        >
          <span>{{ row.source_model_name }}</span>
          <strong>{{ formatUah(row.latest_selected_mean_regret_uah) }}</strong>
          <small>{{ row.rolling_strict_pass_window_count }}/{{ row.rolling_window_count }} rolling / {{ row.production_promote ? 'read-model promoted' : row.promotion_blocker }}</small>
        </article>
      </div>
    </section>

    <section class="evidence-chart-panel">
      <div class="section-heading">
        <div>
          <p class="eyebrow">
            Evidence charts
          </p>
          <h2>Best results and closed branches</h2>
          <p class="section-explainer">
            Lower regret is better. These charts separate the promoted V2+ result from research branches that were tested
            but did not replace it.
          </p>
        </div>
        <span class="source-pill">strict LP/oracle scoring</span>
      </div>
      <div class="chart-grid">
        <article class="chart-card chart-card-wide">
          <div class="chart-card-header">
            <div>
              <p class="eyebrow">
                Regret ladder
              </p>
              <h3>V2+ is the current low-regret headline</h3>
            </div>
            <strong>{{ formatUah(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.meanRegretUah) }}</strong>
          </div>
          <div class="regret-ladder">
            <div
              v-for="point in regretLadderRows"
              :key="point.label"
              :class="`regret-row regret-row--${point.status}`"
            >
              <div class="regret-row-label">
                <span>{{ point.label }}</span>
                <small>{{ point.note }}</small>
              </div>
              <div class="regret-bar-track">
                <span
                  class="regret-bar-fill"
                  :style="{ width: `${point.barWidthPercent}%` }"
                />
              </div>
              <strong>{{ formatUah(point.meanRegretUah) }}</strong>
            </div>
          </div>
          <div class="calibration-explainer">
            <UIcon name="i-lucide-sliders-horizontal" />
            <div>
              <strong>Calibrated means “corrected before scoring”, not “peeked at the answer”.</strong>
              <p>
                V2+ first looks at previous anchors and learns a small horizon-by-horizon correction for forecast bias.
                Then it builds schedules and scores them with the same strict LP/oracle regret gate. Final-holdout
                realized prices are used only to score the result, not to choose the correction.
              </p>
            </div>
          </div>
        </article>

        <article class="chart-card">
          <div class="chart-card-header">
            <div>
              <p class="eyebrow">
                TFT portfolio closure
              </p>
              <h3>Complementary schedules exist, but not robustly</h3>
            </div>
            <strong>{{ CURRENT_TFT_PORTFOLIO_CLOSURE.rollingPassCount }}/{{ CURRENT_TFT_PORTFOLIO_CLOSURE.rollingWindowCount }}</strong>
          </div>
          <div class="portfolio-diagnostic-list">
            <div
              v-for="point in tftPortfolioRows"
              :key="point.label"
              :class="`portfolio-diagnostic portfolio-diagnostic--${point.status}`"
            >
              <div>
                <span>{{ point.label }}</span>
                <strong>{{ point.numerator }}/{{ point.denominator }}</strong>
                <small>{{ point.note }}</small>
              </div>
              <div class="portfolio-track">
                <span :style="{ width: `${point.barWidthPercent}%` }" />
              </div>
              <em>{{ point.percentLabel }}</em>
            </div>
          </div>
        </article>
      </div>

      <div class="v2-plus-improvement-panel">
        <div class="section-heading">
          <div>
            <p class="eyebrow">
              Why V2+ beats V2
            </p>
            <h3>Same strict judge, better schedule candidates</h3>
            <p class="section-explainer">
              V2+ did not weaken the benchmark and did not claim raw forecast superiority. It improved the decision
              layer by adding prior-safe schedule families around the failure modes found after V2, while keeping V2
              as fallback.
            </p>
          </div>
          <span class="source-pill">206.37 -> 174.77 UAH</span>
        </div>
        <div class="v2-plus-improvement-grid">
          <article
            v-for="point in CURRENT_V2_PLUS_IMPROVEMENT_STORY"
            :key="point.label"
            :class="`v2-plus-improvement-card v2-plus-improvement-card--${point.status}`"
          >
            <span>{{ point.label }}</span>
            <strong>{{ point.value }}</strong>
            <small>{{ point.englishBody }}</small>
            <em>{{ point.ukrainianBody }}</em>
          </article>
        </div>
      </div>

      <div class="tft-use-panel">
        <div class="section-heading">
          <div>
            <p class="eyebrow">
              Can TFT be used?
            </p>
            <h3>TFT is useful as candidate diversity, not as the selected policy yet</h3>
            <p class="section-explainer">
              The important nuance is timing. The 24 winning TFT schedules are known after realized prices are scored.
              A live or offline-promoted selector must know before the window starts, using only prior features.
            </p>
          </div>
          <span class="source-pill">no final-holdout leakage</span>
        </div>
        <div class="tft-use-grid">
          <article
            v-for="decision in CURRENT_TFT_USE_DECISION"
            :key="decision.label"
            :class="`tft-use-card tft-use-card--${decision.status}`"
          >
            <span>{{ decision.label }}</span>
            <strong>{{ decision.value }}</strong>
            <small>{{ decision.body }}</small>
          </article>
        </div>
        <div class="tft-safe-selection-panel">
          <div class="tft-safe-selection-heading">
            <p class="eyebrow">
              Why the 24 TFT wins are not selected yet
            </p>
            <h4>Good hindsight schedules are not enough for a safe selector</h4>
            <p>
              The selector must decide before the target hours begin. A schedule that is known to be good only after
              realized prices are scored is useful diagnostic evidence, not a safe promotion rule.
            </p>
          </div>
          <div class="tft-safe-selection-grid">
            <article
              v-for="item in CURRENT_TFT_SAFE_SELECTION_EXPLAINER"
              :key="item.label"
              :class="`tft-safe-selection-card tft-safe-selection-card--${item.status}`"
            >
              <span>{{ item.label }}</span>
              <div class="tft-safe-language-row">
                <div>
                  <strong>{{ item.englishTitle }}</strong>
                  <small>{{ item.englishBody }}</small>
                </div>
                <div>
                  <strong>{{ item.ukrainianTitle }}</strong>
                  <small>{{ item.ukrainianBody }}</small>
                </div>
              </div>
            </article>
          </div>
        </div>
      </div>
    </section>

    <section class="latest-experiment-panel">
      <div class="section-heading">
        <div>
          <p class="eyebrow">
            Latest experiments
          </p>
          <h2>This week’s dashboard story</h2>
        </div>
        <span class="source-pill">Demo Day 2</span>
      </div>
      <div class="latest-experiment-grid">
        <article
          v-for="experiment in CURRENT_DASHBOARD_EXPERIMENTS"
          :key="experiment.label"
          :class="`latest-experiment-card latest-experiment-card--${experiment.status}`"
        >
          <span>{{ experiment.label }}</span>
          <strong>{{ experiment.value }}</strong>
          <small>{{ experiment.meta }}</small>
        </article>
      </div>
    </section>

    <section class="section-grid">
      <div class="wide-panel">
        <div class="section-heading">
          <div>
            <p class="eyebrow">
              Legacy FastAPI benchmark context
            </p>
            <h2>104-anchor compact rows, kept for read-model health</h2>
            <p class="section-explainer">
              This table is not the headline 365-anchor V2+ packet. It stays on the defense page as a backend/API
              consistency check for older benchmark rows; the promoted result is summarized in the V2+ cards and charts
              above.
            </p>
          </div>
          <span class="source-pill">{{ defense.benchmarkSummary.value?.sourceMode || 'FastAPI pending' }}</span>
        </div>

        <div
          v-if="defense.modelRows.value.length > 0"
          class="table-wrap"
        >
          <table>
            <thead>
              <tr>
                <th>Model</th>
                <th>Role</th>
                <th>Anchors</th>
                <th>Mean regret</th>
                <th>Median regret</th>
                <th class="table-help-cell">
                  <span
                    class="table-help"
                    tabindex="0"
                  >
                    Win rate
                    <span
                      class="defense-tooltip"
                      role="tooltip"
                    >
                      <strong>Win rate</strong>
                      <span>Share of benchmark anchors where this row ranked first by regret among rows in its returned strategy response.</span>
                      <em>win_rate = count(rank_by_regret = 1) / anchor_count</em>
                    </span>
                  </span>
                </th>
                <th>Throughput</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="row in defense.modelRows.value"
                :key="row.modelName"
              >
                <td>{{ row.modelName }}</td>
                <td>{{ row.role }}</td>
                <td>{{ row.anchorCount }}</td>
                <td>{{ formatUah(row.meanRegretUah) }}</td>
                <td>{{ formatUah(row.medianRegretUah) }}</td>
                <td>{{ formatPercent(row.winRate) }}</td>
                <td>{{ row.meanThroughputMwh.toFixed(3) }} MWh</td>
              </tr>
            </tbody>
          </table>
        </div>
        <p
          v-else
          class="empty-state"
        >
          No benchmark rows returned by FastAPI for this tenant.
        </p>
      </div>

      <aside class="side-panel">
        <p class="eyebrow">
          Claim boundary
        </p>
        <h2>What examiner should not infer</h2>
        <ul class="boundary-list">
          <li
            v-for="boundary in claimBoundaries"
            :key="boundary"
          >
            <UIcon name="i-lucide-shield-check" />
            <span>{{ boundary }}</span>
          </li>
        </ul>
      </aside>
    </section>

    <section class="section-grid">
      <div class="wide-panel">
        <div class="section-heading">
          <div>
            <p class="eyebrow">
              Forecast evidence
            </p>
            <h2>Forecast rows are inputs, not promotion claims</h2>
            <p class="section-explainer">
              This section shows the live/read-model forecast stack that feeds preview charts. It is not the final
              thesis metric by itself: NBEATSx/TFT forecasts must become feasible schedules and then pass strict
              LP/oracle regret scoring. TFT remains a candidate source until it beats V2+ before the fact.
            </p>
          </div>
          <span class="source-pill">{{ defense.futureStack.value?.selected_forecast_model || 'forecast stack pending' }}</span>
        </div>

        <div
          v-if="futureForecastRows.length > 0"
          class="future-stack-grid"
        >
          <article
            v-for="row in futureForecastRows"
            :key="row.modelName"
            class="future-stack-tile"
          >
            <span>{{ row.modelFamily }}</span>
            <strong>{{ row.modelName }}</strong>
            <small>{{ row.pointCount }} forecast points / {{ row.uncertaintyKind }}</small>
            <small>
              {{ row.firstForecast ? Math.round(row.firstForecast).toLocaleString('en-GB') : 'n/a' }}
              to
              {{ row.lastForecast ? Math.round(row.lastForecast).toLocaleString('en-GB') : 'n/a' }}
              UAH/MWh
            </small>
            <small>
              regret {{ row.meanRegretUah ? formatUah(row.meanRegretUah) : 'n/a' }} /
              win {{ row.winRate ? formatPercent(row.winRate) : 'n/a' }}
            </small>
          </article>
        </div>
        <p
          v-else
          class="empty-state"
        >
          No NBEATSx/TFT forecast stack rows returned yet.
        </p>
        <div class="section-note-strip">
          <article>
            <span>Current role</span>
            <strong>Forecast context</strong>
            <small>These rows explain price scenarios and uncertainty; the selected headline result remains V2+ schedule/value evidence.</small>
          </article>
          <article>
            <span>TFT boundary</span>
            <strong>Candidate only</strong>
            <small>TFT p10/p50/p90 schedules can enter a portfolio, but cannot be selected from hindsight winners.</small>
          </article>
          <article>
            <span>Admission rule</span>
            <strong>Beat V2+</strong>
            <small>Any TFT-combined strategy must improve mean regret versus 174.77 UAH and preserve robustness.</small>
          </article>
        </div>
      </div>

      <aside class="side-panel">
        <p class="eyebrow">
          DT/LAVA next branch
        </p>
        <h2>Not a deployed policy</h2>
        <div
          v-if="dtPolicySummary"
          class="readiness-list"
        >
          <article class="readiness-row">
            <span>Readiness</span>
            <strong>{{ dtPolicySummary.readiness }}</strong>
            <small>{{ dtPolicySummary.rows }} rows / {{ dtPolicySummary.violations }} violations</small>
            <small>{{ dtPolicySummary.stateFeatures }}</small>
            <em>{{ dtPolicySummary.boundary }}</em>
          </article>
          <article class="readiness-row">
            <span>Value gap</span>
            <strong>{{ formatUah(dtPolicySummary.meanValueGap) }}</strong>
            <small>{{ formatUah(dtPolicySummary.valueVsHold) }} vs hold</small>
            <small>{{ dtPolicySummary.valueInterpretation }}</small>
            <em>{{ dtPolicySummary.operatorBoundary }}</em>
          </article>
        </div>
        <p
          v-else
          class="empty-state"
        >
          No DT policy preview rows returned yet.
        </p>
        <p class="section-explainer">
          DT/LAVA is the next research branch after the TFT portfolio closure. It must use V2+ as comparator and keep
          strict LP/oracle scoring before any claim changes. {{ futureBackendStatusText }}
        </p>
        <div class="dt-lava-plan-grid">
          <article
            v-for="step in CURRENT_DT_LAVA_NEXT_STEPS"
            :key="step.label"
            :class="`dt-lava-plan-card dt-lava-plan-card--${step.status}`"
          >
            <span>{{ step.label }}</span>
            <strong>{{ step.value }}</strong>
            <small>{{ step.body }}</small>
          </article>
        </div>
      </aside>
    </section>

    <section class="section-grid">
      <div class="wide-panel">
        <div class="section-heading">
          <div>
            <p class="eyebrow">
              Forecast diagnostics
            </p>
            <h2>Legacy error vs LP sensitivity diagnostic</h2>
            <p class="section-explainer">
              These buckets are explanatory diagnostics from older forecast-to-LP rows. They are still useful for
              explaining failure modes, but they are not the V2+ promotion packet and should not be read as the current
              selected strategy. Realized prices are used only after each anchor for diagnosis, not as model inputs.
            </p>
          </div>
          <span class="source-pill">{{ defense.sensitivity.value?.source_strategy_kind || 'not loaded' }}</span>
        </div>

        <div
          v-if="defense.sensitivity.value?.bucket_summary.length"
          class="bucket-grid"
        >
          <article
            v-for="bucket in defense.sensitivity.value.bucket_summary"
            :key="bucket.diagnostic_bucket"
            class="bucket-tile"
            tabindex="0"
          >
            <span>{{ bucket.diagnostic_bucket }}</span>
            <strong>{{ bucket.rows }} rows</strong>
            <small>{{ formatUah(bucket.mean_regret_uah) }} mean regret</small>
            <small>{{ Math.round(bucket.mean_forecast_mae_uah_mwh).toLocaleString('en-GB') }} UAH/MWh MAE</small>
            <span
              class="defense-tooltip"
              role="tooltip"
            >
              <strong>{{ bucket.diagnostic_bucket }}</strong>
              <span>Diagnostic group for rows with similar forecast-error and LP-dispatch behavior.</span>
              <em>mean_regret and MAE are averaged inside this bucket</em>
            </span>
          </article>
        </div>
        <p
          v-else
          class="empty-state"
        >
          No sensitivity buckets returned by FastAPI.
        </p>
        <div class="section-note-strip">
          <article>
            <span>What it explains</span>
            <strong>Failure modes</strong>
            <small>Whether value was lost by forecast magnitude, price rank, spread shape, or LP dispatch sensitivity.</small>
          </article>
          <article>
            <span>What it is not</span>
            <strong>Not headline V2+</strong>
            <small>The headline metric comes from the 365-anchor V2+ strict LP/oracle gate, not this older bucket table.</small>
          </article>
        </div>
      </div>

      <aside class="side-panel">
        <p class="eyebrow">
          Research branches
        </p>
        <h2>Not dashboard defaults</h2>
        <div class="readiness-list">
          <article
            v-for="row in defense.researchReadinessRows.value"
            :key="row.label"
            class="readiness-row"
          >
            <span>{{ row.label }}</span>
            <strong>{{ row.status }}</strong>
            <small>{{ row.metric }}</small>
            <em>{{ row.boundary }}</em>
          </article>
        </div>
      </aside>
    </section>

    <section class="section-grid">
      <div class="wide-panel">
        <div class="section-heading">
          <div>
            <p class="eyebrow">
              Live exogenous context
            </p>
            <h2>Grid, weather, telemetry for demo context</h2>
            <p class="section-explainer">
              These are current operator-context signals. They help explain the live tenant state, but they do not change
              the frozen V2+ thesis evidence unless a future point-in-time experiment explicitly routes and validates
              them through the strict gate.
            </p>
          </div>
          <span class="source-pill">live-only</span>
        </div>

        <div class="context-grid">
          <article class="context-tile">
            <span>Weather</span>
            <strong>{{ defense.exogenousSignals.value?.latest_weather?.source || 'unavailable' }}</strong>
            <small>
              {{ defense.exogenousSignals.value?.latest_weather?.temperature?.toFixed(1) || 'n/a' }} C /
              {{ defense.exogenousSignals.value?.latest_weather?.wind_speed?.toFixed(1) || 'n/a' }} m/s
            </small>
            <small>{{ formatDateTime(defense.exogenousSignals.value?.latest_weather?.timestamp) }}</small>
          </article>

          <article class="context-tile">
            <span>Grid risk</span>
            <strong>{{ defense.exogenousSignals.value?.national_grid_risk_score?.toFixed(2) || 'unavailable' }}</strong>
            <small>
              tenant region:
              {{ defense.exogenousSignals.value?.tenant_region_affected ? 'affected' : 'clear or unknown' }}
            </small>
            <small>{{ defense.exogenousSignals.value?.latest_grid_event?.raw_text_summary || 'no event text' }}</small>
          </article>

          <article class="context-tile">
            <span>Battery telemetry</span>
            <strong>{{ latestBatterySoc }}</strong>
            <small>
              SOH {{ defense.batteryState.value?.latest_telemetry?.soh
                ? formatPercent(defense.batteryState.value.latest_telemetry.soh)
                : 'unavailable' }}
            </small>
            <small>{{ formatDateTime(defense.batteryState.value?.latest_telemetry?.observed_at) }}</small>
          </article>
        </div>

        <div class="section-note-strip">
          <article>
            <span>Weather/load</span>
            <strong>Ukrainian context</strong>
            <small>Allowed as point-in-time context when source-backed and available before the decision window.</small>
          </article>
          <article>
            <span>Grid events</span>
            <strong>Operator explanation</strong>
            <small>Useful for demo and risk context; not a silent override of the offline promotion result.</small>
          </article>
          <article>
            <span>External markets</span>
            <strong>Governance blocked</strong>
            <small>ENTSO-E/Poland remains excluded from training until publication-time, FX, licensing, and domain-shift gates pass.</small>
          </article>
        </div>

        <div
          v-if="defense.exogenousSignals.value?.source_urls.length"
          class="source-list"
        >
          <a
            v-for="url in defense.exogenousSignals.value.source_urls"
            :key="url"
            :href="url"
            target="_blank"
            rel="noreferrer"
          >
            {{ url }}
          </a>
        </div>
      </div>

      <aside class="side-panel">
        <p class="eyebrow">
          FastAPI gaps
        </p>
        <h2>Live endpoint health</h2>
        <div
          v-if="errorRows.length > 0"
          class="error-list"
        >
          <article
            v-for="error in errorRows"
            :key="error.key"
            class="error-row"
          >
            <strong>{{ error.key }}</strong>
            <span>{{ error.message }}</span>
          </article>
        </div>
        <p
          v-else
          class="empty-state"
        >
          All requested defense read models responded.
        </p>
      </aside>
    </section>
  </main>
</template>

<style scoped>
.defense-shell {
  min-height: 100vh;
  padding: 1.25rem;
  color: #142033;
}

.defense-topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
  margin: 0 auto 1rem;
  max-width: 1380px;
}

.brand-link,
.icon-button,
.tenant-picker select {
  border: 1px solid rgba(20, 32, 51, 0.18);
  border-radius: 0.5rem;
  background: rgba(255, 255, 255, 0.9);
  color: #142033;
}

.brand-link,
.icon-button {
  display: inline-flex;
  align-items: center;
  gap: 0.45rem;
  min-height: 2.5rem;
  padding: 0 0.85rem;
  font-weight: 700;
  text-decoration: none;
}

.icon-button:disabled {
  opacity: 0.55;
}

.topbar-controls {
  display: flex;
  align-items: end;
  gap: 0.75rem;
  flex-wrap: wrap;
}

.tenant-picker {
  display: grid;
  gap: 0.25rem;
  font-size: 0.78rem;
  font-weight: 700;
  color: #465468;
}

.tenant-picker select {
  min-width: 16rem;
  min-height: 2.5rem;
  padding: 0 0.75rem;
  font: inherit;
}

.defense-hero,
.narrative-band,
.pipeline-visual-panel,
.evidence-chart-panel,
.section-grid {
  max-width: 1380px;
  margin: 0 auto 1rem;
}

.defense-hero {
  display: grid;
  grid-template-columns: minmax(0, 1.25fr) minmax(320px, 0.75fr);
  gap: 1rem;
  min-height: 28rem;
  align-items: stretch;
  padding: 1.25rem;
  border: 1px solid rgba(20, 32, 51, 0.14);
  border-radius: 0.75rem;
  background: linear-gradient(135deg, rgba(255, 255, 255, 0.96), rgba(235, 247, 255, 0.92));
  box-shadow: 0 18px 45px rgba(20, 32, 51, 0.08);
}

.hero-copy {
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 1rem;
  max-width: 54rem;
}

.eyebrow {
  margin: 0;
  color: #00669f;
  font-size: 0.78rem;
  font-weight: 800;
  text-transform: uppercase;
}

h1,
h2,
p {
  margin: 0;
}

h1 {
  max-width: 54rem;
  font-size: clamp(2.1rem, 4rem, 4rem);
  line-height: 1.03;
  letter-spacing: 0;
}

h2 {
  font-size: 1.25rem;
  line-height: 1.2;
  letter-spacing: 0;
}

.hero-body {
  max-width: 48rem;
  color: #465468;
  font-size: 1rem;
  line-height: 1.65;
}

.tenant-context {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
}

.tenant-context span,
.source-pill {
  border: 1px solid rgba(0, 102, 159, 0.2);
  border-radius: 999px;
  background: rgba(230, 246, 255, 0.86);
  padding: 0.4rem 0.7rem;
  color: #174b6f;
  font-size: 0.78rem;
  font-weight: 800;
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.75rem;
}

.metric-tile,
.narrative-step,
.wide-panel,
.side-panel,
.bucket-tile,
.context-tile,
.readiness-row,
.error-row {
  border: 1px solid rgba(20, 32, 51, 0.12);
  border-radius: 0.5rem;
  background: rgba(255, 255, 255, 0.92);
}

.metric-tile {
  position: relative;
  display: grid;
  align-content: center;
  gap: 0.4rem;
  min-height: 9.5rem;
  padding: 1rem;
  cursor: help;
  overflow: visible;
}

.metric-tile span,
.bucket-tile span,
.context-tile span,
.readiness-row span {
  color: #617084;
  font-size: 0.78rem;
  font-weight: 800;
  text-transform: uppercase;
}

.metric-tile strong {
  font-size: 1.35rem;
  line-height: 1.1;
}

.metric-tile small,
.bucket-tile small,
.context-tile small,
.readiness-row small,
.readiness-row em,
.error-row span {
  color: #617084;
  line-height: 1.45;
}

.metric-tile:focus-visible,
.bucket-tile:focus-visible,
.table-help:focus-visible {
  outline: 2px solid rgba(0, 102, 159, 0.45);
  outline-offset: 2px;
}

.defense-tooltip {
  position: absolute;
  left: 0.65rem;
  top: calc(100% + 0.4rem);
  z-index: 80;
  display: grid;
  width: min(20rem, calc(100vw - 2rem));
  gap: 0.3rem;
  border: 1px solid rgba(0, 102, 159, 0.26);
  border-radius: 0.5rem;
  background: rgba(255, 255, 255, 0.98);
  box-shadow: 0 18px 36px rgba(20, 32, 51, 0.16);
  padding: 0.75rem;
  color: #142033;
  opacity: 0;
  pointer-events: none;
  transform: translateY(0.3rem);
  transition: opacity 150ms ease, transform 150ms ease;
}

.defense-tooltip strong {
  color: #00669f;
  font-size: 0.82rem;
  font-weight: 850;
}

.defense-tooltip span,
.defense-tooltip em {
  color: #465468;
  font-size: 0.76rem;
  font-style: normal;
  font-weight: 650;
  line-height: 1.4;
  text-transform: none;
}

.defense-tooltip em {
  color: #174b6f;
  font-weight: 800;
}

.metric-tile:hover .defense-tooltip,
.metric-tile:focus-visible .defense-tooltip,
.bucket-tile:hover .defense-tooltip,
.bucket-tile:focus-visible .defense-tooltip,
.table-help:hover .defense-tooltip,
.table-help:focus-visible .defense-tooltip {
  opacity: 1;
  transform: translateY(0);
}

.narrative-band {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 0.75rem;
}

.narrative-step {
  padding: 1rem;
}

.narrative-step span {
  display: block;
  margin-bottom: 0.55rem;
  color: #00669f;
  font-weight: 800;
}

.narrative-step p {
  color: #465468;
  font-size: 0.92rem;
  line-height: 1.55;
}

.pipeline-visual-panel {
  display: grid;
  grid-template-columns: minmax(0, 0.62fr) minmax(420px, 1fr);
  gap: 1rem;
  border: 1px solid rgba(20, 32, 51, 0.12);
  border-radius: 0.9rem;
  background: linear-gradient(135deg, rgba(3, 42, 75, 0.98), rgba(4, 86, 132, 0.94));
  padding: 1rem;
  color: white;
  overflow: hidden;
}

.pipeline-visual-copy {
  display: grid;
  align-content: center;
  gap: 0.9rem;
}

.pipeline-visual-panel .eyebrow,
.pipeline-visual-panel .section-explainer {
  color: rgba(229, 249, 255, 0.84);
}

.pipeline-visual-panel h2 {
  font-size: 1.65rem;
}

.pipeline-stat-strip {
  display: grid;
  gap: 0.65rem;
}

.pipeline-stat-strip article {
  display: grid;
  gap: 0.2rem;
  border: 1px solid rgba(202, 249, 255, 0.2);
  border-radius: 0.65rem;
  background: rgba(3, 105, 161, 0.4);
  padding: 0.75rem;
}

.pipeline-stat-strip span {
  color: #d7ff4f;
  font-size: 0.72rem;
  font-weight: 850;
  text-transform: uppercase;
}

.pipeline-stat-strip strong {
  font-size: 1.05rem;
}

.pipeline-stat-strip small {
  color: rgba(229, 249, 255, 0.78);
  line-height: 1.4;
}

.pipeline-figure {
  display: grid;
  gap: 0.55rem;
  margin: 0;
}

.pipeline-figure img {
  width: 100%;
  min-height: 22rem;
  max-height: 35rem;
  border: 1px solid rgba(202, 249, 255, 0.24);
  border-radius: 0.8rem;
  object-fit: cover;
  box-shadow: 0 24px 52px rgba(0, 0, 0, 0.28);
}

.pipeline-figure figcaption {
  color: rgba(229, 249, 255, 0.74);
  font-size: 0.78rem;
  font-weight: 700;
  line-height: 1.4;
}

.bilingual-explainer-panel {
  display: grid;
  gap: 1rem;
  max-width: 1380px;
  margin: 0 auto 1rem;
  border: 1px solid rgba(20, 32, 51, 0.12);
  border-radius: 0.9rem;
  background: rgba(255, 255, 255, 0.95);
  padding: 1rem;
}

.bilingual-explainer-grid {
  display: grid;
  gap: 0.85rem;
}

.bilingual-explainer-card {
  display: grid;
  gap: 0.75rem;
  border: 1px solid rgba(14, 165, 233, 0.18);
  border-radius: 0.72rem;
  background: linear-gradient(135deg, rgba(224, 242, 254, 0.7), rgba(255, 255, 255, 0.94));
  padding: 0.85rem;
}

.bilingual-explainer-card header span {
  color: #0369a1;
  font-size: 0.74rem;
  font-weight: 900;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}

.language-columns {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.85rem;
}

.language-column {
  display: grid;
  align-content: start;
  gap: 0.48rem;
  border: 1px solid rgba(20, 32, 51, 0.08);
  border-radius: 0.62rem;
  background: rgba(255, 255, 255, 0.86);
  padding: 0.78rem;
}

.language-column--ua {
  background: rgba(240, 253, 244, 0.82);
}

.language-kicker {
  margin: 0;
  color: #475569;
  font-size: 0.7rem;
  font-weight: 850;
  text-transform: uppercase;
}

.language-column h3 {
  margin: 0;
  color: #142033;
  font-size: 1rem;
  line-height: 1.25;
}

.language-column p:not(.language-kicker) {
  margin: 0;
  color: #465468;
  font-size: 0.84rem;
  line-height: 1.56;
}

.language-column ul {
  display: grid;
  gap: 0.35rem;
  margin: 0.15rem 0 0;
  padding-left: 1.05rem;
}

.language-column li {
  color: #334155;
  font-size: 0.8rem;
  line-height: 1.45;
}

.offline-promotion-panel {
  display: grid;
  grid-template-columns: minmax(0, 0.92fr) minmax(320px, 0.62fr);
  gap: 1rem;
  border: 1px solid rgba(34, 197, 94, 0.28);
  border-radius: 1rem;
  background: linear-gradient(135deg, rgba(220, 252, 231, 0.95), rgba(255, 255, 255, 0.95));
  padding: 1rem;
}

.offline-promotion-metrics,
.offline-promotion-rows {
  display: grid;
  gap: 0.65rem;
}

.offline-promotion-metrics article,
.offline-promotion-rows article {
  display: grid;
  gap: 0.25rem;
  border: 1px solid rgba(20, 32, 51, 0.1);
  border-radius: 0.7rem;
  background: rgba(255, 255, 255, 0.84);
  padding: 0.72rem;
}

.offline-promotion-metrics span,
.offline-promotion-rows span {
  color: #166534;
  font-size: 0.74rem;
  font-weight: 850;
  text-transform: uppercase;
}

.offline-promotion-metrics strong,
.offline-promotion-rows strong {
  overflow-wrap: anywhere;
  color: #0f172a;
  font-size: 1rem;
}

.offline-promotion-rows {
  grid-column: 1 / -1;
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.evidence-scope-note {
  display: grid;
  grid-template-columns: 1.15rem minmax(0, 1fr);
  gap: 0.62rem;
  align-items: start;
  border: 1px solid rgba(14, 165, 233, 0.18);
  border-radius: 0.65rem;
  background: rgba(224, 242, 254, 0.76);
  padding: 0.72rem;
}

.evidence-scope-note--wide {
  grid-column: 1 / -1;
}

.evidence-scope-note svg {
  margin-top: 0.08rem;
  color: #0284c7;
}

.evidence-scope-note p {
  margin: 0;
  color: #465468;
  font-size: 0.82rem;
  line-height: 1.5;
}

.offline-promotion-rows small {
  color: #465468;
  line-height: 1.4;
}

.latest-experiment-panel {
  display: grid;
  gap: 1rem;
  max-width: 1380px;
  margin: 0 auto 1rem;
  border: 1px solid rgba(20, 32, 51, 0.12);
  border-radius: 0.75rem;
  background: rgba(255, 255, 255, 0.94);
  padding: 1rem;
}

.latest-experiment-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 0.75rem;
}

.latest-experiment-card {
  display: grid;
  gap: 0.35rem;
  min-height: 8rem;
  border: 1px solid rgba(20, 32, 51, 0.12);
  border-radius: 0.5rem;
  background: linear-gradient(180deg, rgba(241, 245, 249, 0.92), rgba(255, 255, 255, 0.96));
  padding: 0.85rem;
}

.latest-experiment-card span {
  color: #475569;
  font-size: 0.74rem;
  font-weight: 850;
  text-transform: uppercase;
}

.latest-experiment-card strong {
  color: #0f172a;
  font-size: 1.05rem;
  line-height: 1.15;
}

.latest-experiment-card small {
  color: #617084;
  line-height: 1.45;
}

.latest-experiment-card--headline {
  border-color: rgba(34, 197, 94, 0.3);
  background: linear-gradient(180deg, rgba(220, 252, 231, 0.94), rgba(255, 255, 255, 0.96));
}

.latest-experiment-card--closed {
  border-color: rgba(249, 115, 22, 0.26);
  background: linear-gradient(180deg, rgba(255, 237, 213, 0.92), rgba(255, 255, 255, 0.96));
}

.latest-experiment-card--shadow {
  border-color: rgba(245, 158, 11, 0.32);
  background: linear-gradient(180deg, rgba(254, 243, 199, 0.94), rgba(255, 255, 255, 0.96));
}

.latest-experiment-card--blocked {
  border-color: rgba(148, 163, 184, 0.3);
  background: linear-gradient(180deg, rgba(226, 232, 240, 0.92), rgba(255, 255, 255, 0.96));
}

.latest-experiment-card--next {
  border-color: rgba(14, 165, 233, 0.3);
  background: linear-gradient(180deg, rgba(224, 242, 254, 0.92), rgba(255, 255, 255, 0.96));
}

.evidence-chart-panel {
  display: grid;
  gap: 1rem;
  border: 1px solid rgba(20, 32, 51, 0.12);
  border-radius: 0.75rem;
  background: rgba(255, 255, 255, 0.94);
  padding: 1rem;
}

.tft-use-panel {
  display: grid;
  gap: 0.85rem;
  border: 1px solid rgba(14, 165, 233, 0.18);
  border-radius: 0.72rem;
  background: linear-gradient(135deg, rgba(224, 242, 254, 0.76), rgba(255, 255, 255, 0.92));
  padding: 1rem;
}

.tft-use-panel .section-heading {
  margin-bottom: 0;
}

.v2-plus-improvement-panel {
  display: grid;
  gap: 0.85rem;
  border: 1px solid rgba(34, 197, 94, 0.18);
  border-radius: 0.72rem;
  background: linear-gradient(135deg, rgba(220, 252, 231, 0.78), rgba(255, 255, 255, 0.94));
  padding: 1rem;
}

.v2-plus-improvement-panel .section-heading {
  margin-bottom: 0;
}

.v2-plus-improvement-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 0.75rem;
}

.v2-plus-improvement-card {
  display: grid;
  gap: 0.38rem;
  border: 1px solid rgba(20, 32, 51, 0.1);
  border-radius: 0.62rem;
  background: rgba(255, 255, 255, 0.9);
  padding: 0.78rem;
}

.v2-plus-improvement-card span {
  color: #475569;
  font-size: 0.72rem;
  font-weight: 850;
  text-transform: uppercase;
}

.v2-plus-improvement-card strong {
  color: #142033;
  font-size: 1rem;
  line-height: 1.2;
}

.v2-plus-improvement-card small,
.v2-plus-improvement-card em {
  color: #617084;
  font-style: normal;
  line-height: 1.45;
}

.v2-plus-improvement-card em {
  border-top: 1px solid rgba(20, 32, 51, 0.08);
  padding-top: 0.35rem;
}

.v2-plus-improvement-card--candidate_space {
  border-color: rgba(14, 165, 233, 0.24);
}

.v2-plus-improvement-card--fallback {
  border-color: rgba(34, 197, 94, 0.24);
}

.v2-plus-improvement-card--scoring {
  border-color: rgba(99, 102, 241, 0.22);
}

.v2-plus-improvement-card--boundary {
  border-color: rgba(249, 115, 22, 0.22);
}

.tft-use-grid,
.section-note-strip {
  display: grid;
  gap: 0.75rem;
}

.tft-use-grid {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.section-note-strip {
  grid-template-columns: repeat(3, minmax(0, 1fr));
  margin-top: 1rem;
}

.tft-use-card,
.section-note-strip article {
  display: grid;
  gap: 0.35rem;
  border: 1px solid rgba(20, 32, 51, 0.1);
  border-radius: 0.62rem;
  background: rgba(255, 255, 255, 0.88);
  padding: 0.78rem;
}

.tft-use-card span,
.section-note-strip span {
  color: #475569;
  font-size: 0.72rem;
  font-weight: 850;
  text-transform: uppercase;
}

.tft-use-card strong,
.section-note-strip strong {
  color: #142033;
  font-size: 1rem;
  line-height: 1.2;
}

.tft-use-card small,
.section-note-strip small {
  color: #617084;
  line-height: 1.45;
}

.tft-use-card--useful {
  border-color: rgba(14, 165, 233, 0.28);
  background: linear-gradient(180deg, rgba(224, 242, 254, 0.9), rgba(255, 255, 255, 0.94));
}

.tft-use-card--blocked {
  border-color: rgba(249, 115, 22, 0.26);
  background: linear-gradient(180deg, rgba(255, 237, 213, 0.9), rgba(255, 255, 255, 0.94));
}

.tft-use-card--next {
  border-color: rgba(34, 197, 94, 0.24);
  background: linear-gradient(180deg, rgba(220, 252, 231, 0.88), rgba(255, 255, 255, 0.94));
}

.tft-safe-selection-panel {
  display: grid;
  gap: 0.75rem;
  border: 1px solid rgba(20, 32, 51, 0.08);
  border-radius: 0.72rem;
  background: rgba(255, 255, 255, 0.86);
  padding: 0.85rem;
}

.tft-safe-selection-heading {
  display: grid;
  gap: 0.28rem;
}

.tft-safe-selection-heading h4 {
  margin: 0;
  color: #142033;
  font-size: 1.02rem;
  line-height: 1.25;
}

.tft-safe-selection-heading p:not(.eyebrow) {
  margin: 0;
  color: #617084;
  font-size: 0.84rem;
  line-height: 1.5;
}

.tft-safe-selection-grid {
  display: grid;
  gap: 0.65rem;
}

.tft-safe-selection-card {
  display: grid;
  gap: 0.5rem;
  border: 1px solid rgba(20, 32, 51, 0.1);
  border-radius: 0.62rem;
  background: rgba(248, 250, 252, 0.9);
  padding: 0.75rem;
}

.tft-safe-selection-card > span {
  color: #475569;
  font-size: 0.7rem;
  font-weight: 850;
  text-transform: uppercase;
}

.tft-safe-language-row {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.7rem;
}

.tft-safe-language-row div {
  display: grid;
  align-content: start;
  gap: 0.25rem;
}

.tft-safe-language-row strong {
  color: #142033;
  font-size: 0.9rem;
  line-height: 1.25;
}

.tft-safe-language-row small {
  color: #617084;
  line-height: 1.45;
}

.tft-safe-selection-card--opportunity {
  border-color: rgba(14, 165, 233, 0.24);
}

.tft-safe-selection-card--leakage,
.tft-safe-selection-card--diagnosis {
  border-color: rgba(249, 115, 22, 0.22);
}

.tft-safe-selection-card--next {
  border-color: rgba(34, 197, 94, 0.24);
}

.dt-lava-plan-grid {
  display: grid;
  gap: 0.7rem;
  margin-top: 0.85rem;
}

.dt-lava-plan-card {
  display: grid;
  gap: 0.32rem;
  border: 1px solid rgba(20, 32, 51, 0.1);
  border-radius: 0.62rem;
  background: rgba(255, 255, 255, 0.88);
  padding: 0.72rem;
}

.dt-lava-plan-card span {
  color: #475569;
  font-size: 0.7rem;
  font-weight: 850;
  text-transform: uppercase;
}

.dt-lava-plan-card strong {
  color: #142033;
  font-size: 0.94rem;
  line-height: 1.22;
}

.dt-lava-plan-card small {
  color: #617084;
  line-height: 1.45;
}

.dt-lava-plan-card--input {
  border-color: rgba(14, 165, 233, 0.24);
}

.dt-lava-plan-card--model {
  border-color: rgba(99, 102, 241, 0.22);
}

.dt-lava-plan-card--gate,
.dt-lava-plan-card--boundary {
  border-color: rgba(34, 197, 94, 0.24);
}

.chart-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.1fr) minmax(340px, 0.8fr);
  gap: 1rem;
}

.chart-card {
  display: grid;
  gap: 1rem;
  border: 1px solid rgba(20, 32, 51, 0.12);
  border-radius: 0.7rem;
  background: linear-gradient(180deg, rgba(248, 250, 252, 0.96), rgba(255, 255, 255, 0.98));
  padding: 1rem;
}

.chart-card-header {
  display: flex;
  align-items: start;
  justify-content: space-between;
  gap: 1rem;
}

.chart-card-header h3 {
  margin: 0.2rem 0 0;
  color: #142033;
  font-size: 1.08rem;
  line-height: 1.25;
}

.chart-card-header > strong {
  border-radius: 999px;
  background: rgba(220, 252, 231, 0.9);
  padding: 0.4rem 0.65rem;
  color: #166534;
  font-size: 0.9rem;
  white-space: nowrap;
}

.regret-ladder,
.portfolio-diagnostic-list {
  display: grid;
  gap: 0.75rem;
}

.calibration-explainer {
  display: grid;
  grid-template-columns: 1.35rem minmax(0, 1fr);
  gap: 0.7rem;
  align-items: start;
  border: 1px solid rgba(14, 165, 233, 0.2);
  border-radius: 0.65rem;
  background: linear-gradient(135deg, rgba(224, 242, 254, 0.92), rgba(255, 255, 255, 0.96));
  padding: 0.82rem;
}

.calibration-explainer svg {
  margin-top: 0.08rem;
  color: #0284c7;
}

.calibration-explainer strong {
  display: block;
  color: #142033;
  font-size: 0.9rem;
  line-height: 1.35;
}

.calibration-explainer p {
  margin-top: 0.25rem;
  color: #617084;
  font-size: 0.82rem;
  line-height: 1.5;
}

.regret-row {
  display: grid;
  grid-template-columns: minmax(11rem, 0.5fr) minmax(12rem, 1fr) 6.2rem;
  gap: 0.75rem;
  align-items: center;
}

.regret-row-label {
  display: grid;
  gap: 0.15rem;
}

.regret-row-label span,
.portfolio-diagnostic span {
  color: #334155;
  font-size: 0.8rem;
  font-weight: 850;
}

.regret-row-label small,
.portfolio-diagnostic small {
  color: #617084;
  line-height: 1.35;
}

.regret-bar-track,
.portfolio-track {
  position: relative;
  min-height: 0.78rem;
  border-radius: 999px;
  background: rgba(148, 163, 184, 0.2);
  overflow: hidden;
}

.regret-bar-fill,
.portfolio-track span {
  position: absolute;
  inset: 0 auto 0 0;
  border-radius: inherit;
  background: linear-gradient(90deg, #38bdf8, #22c55e);
}

.regret-row--control .regret-bar-fill,
.regret-row--failed .regret-bar-fill {
  background: linear-gradient(90deg, #fb923c, #f97316);
}

.regret-row--headline .regret-bar-fill {
  background: linear-gradient(90deg, #84cc16, #22c55e);
}

.regret-row--plateau .regret-bar-fill {
  background: linear-gradient(90deg, #a78bfa, #6366f1);
}

.regret-row strong {
  color: #142033;
  font-size: 0.9rem;
  text-align: right;
}

.portfolio-diagnostic {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: 0.45rem;
  border: 1px solid rgba(20, 32, 51, 0.1);
  border-radius: 0.6rem;
  padding: 0.7rem;
}

.portfolio-diagnostic div:first-child {
  display: grid;
  gap: 0.2rem;
}

.portfolio-diagnostic strong {
  color: #142033;
  font-size: 1.18rem;
}

.portfolio-diagnostic em {
  color: #617084;
  font-style: normal;
  font-weight: 800;
}

.portfolio-diagnostic--opportunity .portfolio-track span {
  background: linear-gradient(90deg, #38bdf8, #0ea5e9);
}

.portfolio-diagnostic--fallback .portfolio-track span {
  background: linear-gradient(90deg, #a78bfa, #6366f1);
}

.portfolio-diagnostic--blocked .portfolio-track span {
  background: linear-gradient(90deg, #fb923c, #f97316);
}

.section-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(280px, 0.34fr);
  gap: 1rem;
}

.wide-panel,
.side-panel {
  padding: 1rem;
}

.section-heading {
  display: flex;
  align-items: start;
  justify-content: space-between;
  gap: 1rem;
  margin-bottom: 1rem;
}

.section-explainer {
  max-width: 49rem;
  margin-top: 0.35rem;
  color: #617084;
  font-size: 0.86rem;
  line-height: 1.55;
}

.table-wrap {
  overflow-x: auto;
}

table {
  width: 100%;
  min-width: 760px;
  border-collapse: collapse;
}

th,
td {
  border-bottom: 1px solid rgba(20, 32, 51, 0.1);
  padding: 0.75rem 0.6rem;
  text-align: left;
  white-space: nowrap;
}

th {
  color: #465468;
  font-size: 0.75rem;
  text-transform: uppercase;
}

.table-help-cell {
  overflow: visible;
}

.table-help {
  position: relative;
  display: inline-flex;
  cursor: help;
}

.table-help .defense-tooltip {
  top: calc(100% + 0.5rem);
  left: -8rem;
  text-transform: none;
}

td {
  font-size: 0.9rem;
  font-weight: 650;
}

.boundary-list {
  display: grid;
  gap: 0.75rem;
  margin: 1rem 0 0;
  padding: 0;
  list-style: none;
}

.boundary-list li {
  display: grid;
  grid-template-columns: 1.15rem minmax(0, 1fr);
  gap: 0.55rem;
  color: #465468;
  line-height: 1.45;
}

.bucket-grid,
.context-grid,
.future-stack-grid,
.readiness-list,
.error-list {
  display: grid;
  gap: 0.75rem;
}

.bucket-grid,
.context-grid,
.future-stack-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.bucket-tile,
.context-tile,
.future-stack-tile,
.readiness-row,
.error-row {
  position: relative;
  display: grid;
  gap: 0.4rem;
  padding: 0.85rem;
  overflow: visible;
}

.bucket-tile strong,
.context-tile strong,
.future-stack-tile strong,
.readiness-row strong,
.error-row strong {
  font-size: 1rem;
}

.future-stack-tile {
  border: 1px solid rgba(20, 32, 51, 0.12);
  border-radius: 0.5rem;
  background: rgba(255, 255, 255, 0.92);
}

.future-stack-tile span {
  color: #00669f;
  font-size: 0.78rem;
  font-weight: 800;
  text-transform: uppercase;
}

.future-stack-tile small {
  color: #617084;
  line-height: 1.45;
}

.readiness-row em {
  font-style: normal;
  font-weight: 750;
}

.source-list {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
  margin-top: 1rem;
}

.source-list a {
  border: 1px solid rgba(20, 32, 51, 0.12);
  border-radius: 0.45rem;
  padding: 0.45rem 0.6rem;
  color: #00669f;
  font-size: 0.78rem;
  font-weight: 700;
  text-decoration: none;
}

.empty-state {
  color: #617084;
  line-height: 1.5;
}

@media (max-width: 1080px) {
  .defense-hero,
  .section-grid,
  .pipeline-visual-panel,
  .language-columns,
  .chart-grid,
  .offline-promotion-panel {
    grid-template-columns: 1fr;
  }

  .narrative-band,
  .bucket-grid,
  .context-grid,
  .latest-experiment-grid,
  .v2-plus-improvement-grid,
  .future-stack-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .tft-use-grid,
  .tft-safe-language-row,
  .section-note-strip {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .defense-shell {
    padding: 0.75rem;
  }

  .defense-topbar,
  .topbar-controls {
    align-items: stretch;
    flex-direction: column;
  }

  .tenant-picker select,
  .icon-button,
  .brand-link {
    width: 100%;
  }

  .defense-hero {
    min-height: auto;
  }

  h1 {
    font-size: 2.1rem;
  }

  .metric-grid,
  .narrative-band,
  .bucket-grid,
  .context-grid,
  .latest-experiment-grid,
  .v2-plus-improvement-grid,
  .future-stack-grid,
  .offline-promotion-rows,
  .tft-use-grid,
  .tft-safe-language-row,
  .section-note-strip {
    grid-template-columns: 1fr;
  }

  .regret-row {
    grid-template-columns: 1fr;
  }

  .regret-row strong {
    text-align: left;
  }

  .pipeline-figure img {
    min-height: 14rem;
  }
}
</style>
