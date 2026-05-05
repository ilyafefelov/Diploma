<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { formatPercent, formatUah } from '~/utils/defenseDataset'

const preferredTenantId = 'client_003_dnipro_factory'
const selectedTenantId = ref(preferredTenantId)
const registry = useControlPlaneRegistry()
const defense = useDefenseDashboard(selectedTenantId)

const selectedTenant = computed(() => {
  return registry.tenants.value.find(tenant => tenant.tenant_id === selectedTenantId.value) || null
})

const controlRow = computed(() => {
  return defense.modelRows.value.find(row => row.modelName === 'strict_similar_day') || null
})

const bestRow = computed(() => {
  return [...defense.modelRows.value].sort((left, right) => left.meanRegretUah - right.meanRegretUah)[0] || null
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
    label: 'Control baseline',
    value: controlRow.value ? formatUah(controlRow.value.meanRegretUah) : 'unavailable',
    note: 'strict_similar_day mean regret',
    tooltipTitle: 'Control baseline',
    tooltipBody: 'Mean regret for the strict similar-day strategy. This is the default control comparator, not a neural forecast.',
    tooltipFormula: 'mean_regret = avg(oracle_value_uah - decision_value_uah)'
  },
  {
    label: 'Best live model',
    value: bestRow.value?.modelName || 'unavailable',
    note: bestRow.value ? `${formatUah(bestRow.value.meanRegretUah)} mean regret` : 'FastAPI row missing',
    tooltipTitle: 'Best live model',
    tooltipBody: 'Lowest mean regret row returned by the live benchmark read model for the selected tenant.',
    tooltipFormula: 'best = argmin(mean_regret_uah)'
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

const narrativeSteps = [
  {
    label: '1. Data',
    text: 'Observed OREE DAM, tenant weather, grid-event text, and battery telemetry enter Bronze/Silver assets.'
  },
  {
    label: '2. Forecasts',
    text: 'strict_similar_day stays control; compact NBEATSx/TFT remain forecast candidates, not SOTA claim.'
  },
  {
    label: '3. Decision',
    text: 'Every forecast is routed through same LP, Level 1 battery simulator, and oracle-regret scorer.'
  },
  {
    label: '4. Diagnosis',
    text: 'Forecast error, spread mismatch, and LP sensitivity sit beside profit/regret metrics.'
  },
  {
    label: '5. Boundary',
    text: 'DFL, DT, and live trading surfaces are research primitives until strict evaluation proves them.'
  }
]

const claimBoundaries = [
  'thesis-grade only when source rows are observed and complete',
  'strict_similar_day remains default comparator',
  'DFL pilot is relaxed-LP diagnostic, not final DFL training',
  'Decision Transformer rows are offline trajectories, not deployed policy',
  'paper trading is simulated and carries no settlement identifiers'
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
          Research defense / live FastAPI read model
        </p>
        <h1>Can forecast-to-optimize improve Ukrainian BESS arbitrage against a strict similar-day control?</h1>
        <p class="hero-body">
          This route shows only live backend rows from FastAPI/Postgres. It separates empirical benchmark evidence
          from DFL, Decision Transformer, telemetry, and simulated paper-trading readiness.
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

    <section class="section-grid">
      <div class="wide-panel">
        <div class="section-heading">
          <div>
            <p class="eyebrow">
              Gold benchmark
            </p>
            <h2>Model regret evidence</h2>
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
              Forecast diagnostics
            </p>
            <h2>Error vs LP sensitivity</h2>
            <p class="section-explainer">
              Buckets explain why forecast-to-LP rows lost value: raw price error, weak spread ranking, or dispatch
              sensitivity after the LP converts forecasts into battery actions. Realized prices are used only after
              each anchor for diagnosis, not as model inputs.
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
      </div>

      <aside class="side-panel">
        <p class="eyebrow">
          Research primitives
        </p>
        <h2>DFL / DT readiness</h2>
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
            <h2>Grid, weather, telemetry</h2>
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
.readiness-list,
.error-list {
  display: grid;
  gap: 0.75rem;
}

.bucket-grid,
.context-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.bucket-tile,
.context-tile,
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
.readiness-row strong,
.error-row strong {
  font-size: 1rem;
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
  .section-grid {
    grid-template-columns: 1fr;
  }

  .narrative-band,
  .bucket-grid,
  .context-grid {
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
  .context-grid {
    grid-template-columns: 1fr;
  }
}
</style>
