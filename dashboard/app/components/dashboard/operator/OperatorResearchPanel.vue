<script setup lang="ts">
import type { ForecastDispatchSensitivityResponse } from '~/types/control-plane'
import type { OperatorResearchMetric } from '~/utils/operatorResearchMetrics'
import { formatUah } from '~/utils/defenseDataset'

defineProps<{
  metrics: OperatorResearchMetric[]
  sensitivity: ForecastDispatchSensitivityResponse | null
  isLoading: boolean
  lastLoadedLabel: string
  activeErrorCount: number
}>()
</script>

<template>
  <section class="surface-panel operator-research-panel">
    <div class="console-heading">
      <div>
        <p class="eyebrow">
          Research-backed controls
        </p>
        <h2 class="section-title">
          Benchmark, telemetry, and risk context
        </h2>
      </div>

      <div class="research-actions">
        <span class="status-badge">
          {{ isLoading ? 'Refreshing' : `Loaded ${lastLoadedLabel}` }}
        </span>
        <NuxtLink
          class="research-link"
          to="/defense"
        >
          <UIcon name="i-lucide-presentation" />
          Defense
        </NuxtLink>
      </div>
    </div>

    <div class="research-metric-grid">
      <article
        v-for="metric in metrics"
        :key="metric.label"
        class="research-metric"
        :class="`research-metric--${metric.tone}`"
        tabindex="0"
      >
        <span>{{ metric.label }}</span>
        <strong>{{ metric.value }}</strong>
        <small>{{ metric.meta }}</small>
        <span
          class="research-tooltip"
          role="tooltip"
        >
          <strong>{{ metric.tooltipTitle }}</strong>
          <span>{{ metric.tooltipBody }}</span>
          <em>{{ metric.tooltipFormula }}</em>
        </span>
      </article>
    </div>

    <div class="research-diagnostic-grid">
      <article
        class="research-diagnostic-card"
        tabindex="0"
      >
        <span>Forecast diagnosis</span>
        <strong>{{ sensitivity?.row_count ?? 0 }} rows</strong>
        <small>{{ sensitivity?.source_strategy_kind || 'not materialized' }}</small>
        <span
          class="research-tooltip"
          role="tooltip"
        >
          <strong>Forecast diagnosis</strong>
          <span>Rows are grouped by why forecast-to-LP performance changed: raw price error, low regret, spread mismatch, or LP sensitivity.</span>
          <em>diagnostic_bucket = f(forecast_error, spread_error, regret)</em>
        </span>
      </article>

      <article
        v-for="bucket in sensitivity?.bucket_summary?.slice(0, 3) || []"
        :key="bucket.diagnostic_bucket"
        class="research-diagnostic-card"
        tabindex="0"
      >
        <span>{{ bucket.diagnostic_bucket }}</span>
        <strong>{{ bucket.rows }} rows</strong>
        <small>{{ formatUah(bucket.mean_regret_uah) }} regret</small>
        <span
          class="research-tooltip"
          role="tooltip"
        >
          <strong>{{ bucket.diagnostic_bucket }}</strong>
          <span>This bucket explains a subset of benchmark rows with similar error-to-decision behavior.</span>
          <em>mean_regret and MAE are averaged inside this bucket only</em>
        </span>
      </article>

      <article
        v-if="activeErrorCount > 0"
        class="research-diagnostic-card research-diagnostic-card--warning"
        tabindex="0"
      >
        <span>Read-model gaps</span>
        <strong>{{ activeErrorCount }}</strong>
        <small>see defense route for endpoint details</small>
        <span
          class="research-tooltip"
          role="tooltip"
        >
          <strong>Read-model gaps</strong>
          <span>Required operator read models failed to respond. Optional research primitives are not counted as gaps.</span>
          <em>gaps = failed required FastAPI endpoints</em>
        </span>
      </article>
    </div>
  </section>
</template>

<style scoped>
.operator-research-panel {
  display: grid;
  gap: 0.8rem;
  padding: 0.8rem;
  min-width: 0;
}

.research-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 0.55rem;
  flex-wrap: wrap;
}

.research-link {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 0.4rem;
  min-height: 2.3rem;
  border: 1px solid rgba(255, 255, 255, 0.34);
  border-radius: 0.62rem;
  background: linear-gradient(180deg, #85ef41, #2b9b18);
  padding: 0.45rem 0.7rem;
  color: white;
  font-size: 0.74rem;
  font-weight: 900;
  text-decoration: none;
  text-transform: uppercase;
}

.research-metric-grid,
.research-diagnostic-grid {
  display: grid;
  gap: 0.55rem;
}

.research-metric-grid {
  grid-template-columns: repeat(5, minmax(0, 1fr));
}

.research-diagnostic-grid {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.research-metric,
.research-diagnostic-card {
  position: relative;
  display: grid;
  gap: 0.28rem;
  min-width: 0;
  border: 1px solid rgba(255, 255, 255, 0.26);
  border-radius: 0.72rem;
  background: linear-gradient(180deg, rgba(13, 151, 218, 0.78), rgba(6, 82, 147, 0.78));
  padding: 0.65rem;
  overflow: visible;
}

.research-metric span,
.research-diagnostic-card span {
  color: rgba(229, 249, 255, 0.78);
  font-size: 0.68rem;
  font-weight: 900;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.research-metric strong,
.research-diagnostic-card strong {
  overflow-wrap: anywhere;
  color: #b8ff32;
  font-size: 1rem;
  line-height: 1.12;
}

.research-metric small,
.research-diagnostic-card small {
  color: rgba(229, 249, 255, 0.82);
  font-size: 0.72rem;
  font-weight: 750;
  line-height: 1.35;
}

.research-metric--green {
  background: linear-gradient(180deg, rgba(52, 164, 28, 0.84), rgba(22, 101, 34, 0.84));
}

.research-metric--orange,
.research-diagnostic-card--warning {
  background: linear-gradient(180deg, rgba(236, 134, 14, 0.86), rgba(166, 74, 5, 0.86));
}

.research-metric--mint {
  background: linear-gradient(180deg, rgba(31, 180, 185, 0.82), rgba(13, 105, 132, 0.86));
}

.research-metric--lime {
  background: linear-gradient(180deg, rgba(82, 178, 35, 0.82), rgba(36, 111, 28, 0.86));
}

.research-tooltip {
  position: absolute;
  left: 0.45rem;
  bottom: calc(100% + 0.35rem);
  z-index: 120;
  display: grid;
  width: min(18rem, calc(100vw - 2rem));
  gap: 0.26rem;
  border: 1px solid rgba(202, 249, 255, 0.9);
  border-radius: 0.72rem;
  background: linear-gradient(180deg, rgba(0, 129, 204, 0.98), rgba(0, 56, 112, 0.98));
  padding: 0.62rem 0.7rem;
  color: rgba(238, 250, 255, 0.92);
  box-shadow: 0 18px 32px rgba(0, 39, 82, 0.32);
  opacity: 0;
  pointer-events: none;
  transform: translateY(0.24rem) scale(0.97);
  transition: opacity 150ms ease, transform 150ms ease;
}

.research-tooltip strong {
  color: #d7ff4f;
  font-size: 0.74rem;
  font-weight: 900;
}

.research-tooltip span,
.research-tooltip em {
  color: rgba(238, 250, 255, 0.88);
  font-size: 0.68rem;
  font-style: normal;
  font-weight: 700;
  line-height: 1.34;
}

.research-tooltip em {
  color: #d7ff4f;
}

.research-metric:hover .research-tooltip,
.research-metric:focus-visible .research-tooltip,
.research-diagnostic-card:hover .research-tooltip,
.research-diagnostic-card:focus-visible .research-tooltip {
  opacity: 1;
  transform: translateY(0) scale(1);
}

@media (max-width: 1320px) {
  .research-metric-grid,
  .research-diagnostic-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .research-metric-grid,
  .research-diagnostic-grid {
    grid-template-columns: 1fr;
  }
}
</style>
