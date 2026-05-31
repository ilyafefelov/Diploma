<script setup lang="ts">
import type { ForecastDispatchSensitivityBucketResponse } from '~/types/control-plane'
import type { DefenseReadinessRow } from '~/types/defense-page'
import { formatUah } from '~/utils/defenseDataset'

defineProps<{
  bucketRows: ForecastDispatchSensitivityBucketResponse[]
  readinessRows: DefenseReadinessRow[]
  sourceStrategyKind: string
}>()
</script>

<template>
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
        <span class="source-pill">{{ sourceStrategyKind }}</span>
      </div>

      <div
        v-if="bucketRows.length > 0"
        class="bucket-grid"
      >
        <article
          v-for="bucket in bucketRows"
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
          v-for="row in readinessRows"
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
</template>
