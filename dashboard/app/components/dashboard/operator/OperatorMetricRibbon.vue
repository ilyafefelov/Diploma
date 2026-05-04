<script setup lang="ts">
import type { OperatorHeadlineMetric } from '~/types/operator-dashboard'

defineProps<{
  metrics: OperatorHeadlineMetric[]
}>()
</script>

<template>
  <section class="metric-ribbon">
    <article
      v-for="metric in metrics"
      :key="metric.label"
      class="metric-capsule"
      :class="`metric-capsule--${metric.tone}`"
      tabindex="0"
    >
      <div class="metric-capsule__icon">
        <UIcon :name="metric.icon" />
      </div>
      <div>
        <p class="metric-capsule__label">
          {{ metric.label }}
        </p>
        <p class="metric-capsule__value">
          {{ metric.value }}
        </p>
        <p class="metric-capsule__meta">
          {{ metric.meta }}
        </p>
      </div>
      <span
        class="metric-sparkline"
        aria-hidden="true"
      />
      <div
        class="metric-tooltip"
        role="tooltip"
      >
        <div class="metric-tooltip__header">
          <span
            class="metric-tooltip__gem"
            aria-hidden="true"
          />
          <span>Metric card</span>
        </div>
        <p class="metric-tooltip__title">
          {{ metric.tooltipTitle }}
        </p>
        <p class="metric-tooltip__body">
          {{ metric.tooltipBody }}
        </p>
        <p class="metric-tooltip__formula">
          {{ metric.tooltipFormula }}
        </p>
      </div>
    </article>
  </section>
</template>
