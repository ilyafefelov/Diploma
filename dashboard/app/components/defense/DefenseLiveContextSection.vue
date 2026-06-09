<script setup lang="ts">
import type {
  DashboardBatteryStateResponse,
  DashboardExogenousSignalsResponse
} from '~/types/control-plane'
import type { DefenseErrorRow } from '~/types/defense-page'

defineProps<{
  batteryState: DashboardBatteryStateResponse | null
  errorRows: DefenseErrorRow[]
  exogenousSignals: DashboardExogenousSignalsResponse | null
  latestBatterySoc: string
}>()

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

const formatPercent = (value: number): string => `${Math.round(value * 100)}%`
</script>

<template>
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
          <strong>{{ exogenousSignals?.latest_weather?.source || 'unavailable' }}</strong>
          <small>
            {{ exogenousSignals?.latest_weather?.temperature?.toFixed(1) || 'n/a' }} C /
            {{ exogenousSignals?.latest_weather?.wind_speed?.toFixed(1) || 'n/a' }} m/s
          </small>
          <small>{{ formatDateTime(exogenousSignals?.latest_weather?.timestamp) }}</small>
        </article>

        <article class="context-tile">
          <span>Grid risk</span>
          <strong>{{ exogenousSignals?.national_grid_risk_score?.toFixed(2) || 'unavailable' }}</strong>
          <small>
            tenant region:
            {{ exogenousSignals?.tenant_region_affected ? 'affected' : 'clear or unknown' }}
          </small>
          <small>{{ exogenousSignals?.latest_grid_event?.raw_text_summary || 'no event text' }}</small>
        </article>

        <article class="context-tile">
          <span>Battery telemetry</span>
          <strong>{{ latestBatterySoc }}</strong>
          <small>
            SOH {{ batteryState?.latest_telemetry?.soh ? formatPercent(batteryState.latest_telemetry.soh) : 'unavailable' }}
          </small>
          <small>{{ formatDateTime(batteryState?.latest_telemetry?.observed_at) }}</small>
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
        v-if="exogenousSignals?.source_urls.length"
        class="source-list"
      >
        <a
          v-for="url in exogenousSignals.source_urls"
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
</template>
