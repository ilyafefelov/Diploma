<script setup lang="ts">
import type { AcademicMvpReadinessResponse } from '~/types/control-plane'
import type {
  DefenseDtComparisonRow,
  DefenseDtPassportRow,
  DefenseDtStatusRow
} from '~/types/defense-page'
import { formatUah } from '~/utils/defenseDataset'

defineProps<{
  comparisonRows: DefenseDtComparisonRow[]
  gatePassportRows: DefenseDtPassportRow[]
  phaseRows: DefenseDtStatusRow[]
  readiness: AcademicMvpReadinessResponse | null
  statusRows: DefenseDtStatusRow[]
}>()
</script>

<template>
  <section
    id="dt-shadow"
    class="dt-shadow-defense-panel"
  >
    <div class="section-heading">
      <div>
        <p class="eyebrow">
          DT Shadow / Academic MVP
        </p>
        <h2>Credentialless prototype evidence, not a promoted controller</h2>
        <p class="section-explainer">
          The Academic MVP packet proves a DAM/IDM hourly recommendation preview, LAVA/DT prototype contracts, chronological
          DT shadow training, and no-market-execution safety. Publication receipts remain blocked for market
          submission, so the transformer stays research-shadow only.
        </p>
      </div>
      <span class="source-pill">
        {{ readiness?.academic_mvp_gate_passed ? 'Academic MVP passed' : 'Academic MVP pending' }}
      </span>
    </div>

    <div
      v-if="readiness"
      class="dt-shadow-layout"
    >
      <div class="dt-shadow-left">
        <div class="dt-shadow-status-grid">
          <article
            v-for="row in statusRows"
            :key="row.label"
            class="dt-shadow-status-card"
          >
            <span>{{ row.label }}</span>
            <strong>{{ row.value }}</strong>
            <small>{{ row.note }}</small>
          </article>
        </div>

        <div
          v-if="comparisonRows.length > 0"
          class="dt-shadow-comparison"
        >
          <article
            v-for="row in comparisonRows"
            :key="row.label"
            :class="`dt-shadow-comparison-row dt-shadow-comparison-row--${row.status}`"
          >
            <div>
              <span>{{ row.label }}</span>
              <small>{{ row.note }}</small>
            </div>
            <div class="dt-shadow-bar-track">
              <span :style="{ width: `${row.regretBarWidthPercent}%` }" />
            </div>
            <strong>{{ formatUah(row.meanRegretUah) }}</strong>
            <em>{{ formatUah(row.meanValueUah) }} value</em>
          </article>
        </div>
        <p
          v-else
          class="empty-state"
        >
          DT shadow regret/value metrics are not loaded yet.
        </p>
      </div>

      <aside class="dt-shadow-right">
        <div class="dt-shadow-phase-list">
          <article
            v-for="phase in phaseRows"
            :key="phase.label"
          >
            <span>{{ phase.label }}</span>
            <strong>{{ phase.value }}</strong>
            <small>{{ phase.note }}</small>
          </article>
        </div>
        <div class="dt-shadow-passport-list">
          <article
            v-for="item in gatePassportRows.slice(0, 7)"
            :key="item.label"
            :class="`dt-shadow-passport-row dt-shadow-passport-row--${item.status}`"
          >
            <span>{{ item.label }}</span>
            <strong>{{ item.value }}</strong>
            <small>{{ item.reason }}</small>
          </article>
        </div>
      </aside>
    </div>

    <p
      v-else
      class="empty-state"
    >
      Academic MVP readiness packet is not available from the control-plane read model.
    </p>
  </section>
</template>
