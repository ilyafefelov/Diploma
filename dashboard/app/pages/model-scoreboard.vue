<script setup lang="ts">
import { computed } from 'vue'

type PublicPayload = Record<string, any>

const { data: scoreboardData } = await useFetch<PublicPayload>('/data/bess-arbitrage-index/forecast_scoreboard.json', {
  key: 'public-bess-forecast-scoreboard',
  server: false,
  default: () => ({ rows: [] })
})

const rows = computed<Record<string, any>[]>(() => (
  Array.isArray(scoreboardData.value?.rows) ? scoreboardData.value.rows : []
))

const formatMetric = (value: unknown, suffix = '') => {
  if (value === null || value === undefined || value === '') {
    return 'pending'
  }
  const numeric = Number(value)
  const fixed = Number.isFinite(numeric) ? numeric.toFixed(2) : '0.00'
  const [integerPart = '0', decimalPart] = fixed.split('.')
  const sign = integerPart.startsWith('-') ? '-' : ''
  const unsignedInteger = integerPart.replace('-', '')
  const groupedInteger = unsignedInteger.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  return `${sign}${groupedInteger}.${decimalPart}${suffix}`
}
</script>

<template>
  <main class="bess-public-shell">
    <div class="bess-public-frame">
      <header class="bess-public-topbar">
        <div class="bess-public-brand">
          <div class="bess-public-mark" aria-hidden="true">
            <UIcon name="i-lucide-table-properties" />
          </div>
          <div>
            <p class="bess-public-subtitle">Rolling evidence</p>
            <p class="bess-public-title">Model Scoreboard</p>
          </div>
        </div>
        <nav class="bess-public-nav" aria-label="Public BESS views">
          <NuxtLink to="/ukraine-bess-arbitrage-index">Index</NuxtLink>
          <NuxtLink to="/forecast-challenge">Forecast Challenge</NuxtLink>
          <NuxtLink to="/model-scoreboard">Model Scoreboard</NuxtLink>
        </nav>
      </header>

      <section class="bess-public-hero">
        <div class="bess-panel bess-panel--inset bess-hero-copy">
          <div>
            <h1>Rolling forecast quality before model promotion.</h1>
            <p>
              Score rows appear only after a forecast was committed before the delivery date and official OREE
              rows later became available for scoring.
            </p>
          </div>
          <div class="bess-hero-meta">
            <span class="bess-chip">Rows {{ scoreboardData?.row_count || rows.length }}</span>
            <span class="bess-chip">Generated {{ scoreboardData?.generated_at || 'pending' }}</span>
            <span class="bess-chip">No market execution</span>
          </div>
        </div>

        <aside class="bess-score-stack">
          <div class="bess-score-primary">
            <p class="bess-score-label">Score status</p>
            <p class="bess-score-value">
              {{ scoreboardData?.score_status || 'pending' }}
            </p>
            <p class="bess-score-meta">
              Ranking starts after enough realized forecast pairs exist.
            </p>
          </div>
        </aside>
      </section>

      <section class="bess-panel bess-chart-panel">
        <div class="bess-section-header">
          <div>
            <h2>Forecast scoring</h2>
            <p>MAE/RMSE are price metrics; dispatch regret and value capture connect forecasts to BESS economics.</p>
          </div>
        </div>
        <div class="bess-table-wrap">
          <table class="bess-table">
            <thead>
              <tr>
                <th>Model</th>
                <th>Window</th>
                <th>MAE</th>
                <th>RMSE</th>
                <th>Dispatch regret</th>
                <th>Value capture</th>
                <th>Boundary</th>
              </tr>
            </thead>
            <tbody>
              <tr v-if="rows.length === 0">
                <td colspan="7">No scored forecast pairs yet.</td>
              </tr>
              <tr v-for="row in rows" :key="`${row.model_name}-${row.target_delivery_date}`">
                <td>{{ row.model_name }}</td>
                <td>{{ row.target_delivery_date }}</td>
                <td>{{ formatMetric(row.mae_uah_mwh, ' UAH/MWh') }}</td>
                <td>{{ formatMetric(row.rmse_uah_mwh, ' UAH/MWh') }}</td>
                <td>{{ formatMetric(row.dispatch_regret_uah, ' UAH') }}</td>
                <td>{{ formatMetric(row.value_capture_ratio) }}</td>
                <td>{{ row.claim_boundary }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section class="bess-section-grid">
        <div class="bess-panel bess-panel--inset">
          <div class="bess-section-header">
            <div>
              <h2>Promotion ladder</h2>
              <p>Deterministic index, public forecasts, rolling scores, schedule selector, then research challengers.</p>
            </div>
          </div>
          <ul class="bess-detail-list">
            <li><span>Stage 0</span><strong>Realized deterministic index</strong></li>
            <li><span>Stage 1</span><strong>NBEATSx/TFT forecast challenge</strong></li>
            <li><span>Stage 2</span><strong>30+ scored delivery days</strong></li>
            <li><span>Stage 3</span><strong>Schedule-selection backtest</strong></li>
            <li><span>Stage 4</span><strong>V2+ optimization candidate</strong></li>
            <li><span>Stage 5</span><strong>DT/HF DT gated challenger</strong></li>
          </ul>
        </div>

        <div class="bess-panel bess-panel--inset">
          <div class="bess-section-header">
            <div>
              <h2>Research boundary</h2>
              <p>DT/HF DT evidence can be public only as a challenger to V2+ after enough source-backed score history.</p>
            </div>
          </div>
          <ul class="bess-detail-list">
            <li><span>Market execution</span><strong>false</strong></li>
            <li><span>Proposed bids</span><strong>not emitted</strong></li>
            <li><span>External EMS integration</span><strong>not claimed</strong></li>
            <li><span>Default model</span><strong>not DT/HF DT</strong></li>
          </ul>
        </div>
      </section>
    </div>
  </main>
</template>
