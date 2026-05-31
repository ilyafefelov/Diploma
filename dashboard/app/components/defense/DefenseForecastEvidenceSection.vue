<script setup lang="ts">
import type { DefenseDtPolicySummary, DefenseFutureForecastRow } from '~/types/defense-page'
import { CURRENT_DT_LAVA_NEXT_STEPS, formatPercent, formatUah } from '~/utils/defenseDataset'

defineProps<{
  backendStatusText: string
  dtPolicySummary: DefenseDtPolicySummary | null
  futureForecastRows: DefenseFutureForecastRow[]
  selectedForecastModel: string
}>()
</script>

<template>
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
        <span class="source-pill">{{ selectedForecastModel }}</span>
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
        strict LP/oracle scoring before any claim changes. {{ backendStatusText }}
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
</template>
