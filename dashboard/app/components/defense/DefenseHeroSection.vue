<script setup lang="ts">
import type { DefenseMetric } from '~/types/defense-page'

defineProps<{
  activeErrorCount: number
  lastLoadedLabel: string
  metrics: DefenseMetric[]
  selectedTenantId: string
  selectedTenantName: string | null | undefined
  selectedTenantType: string | null | undefined
}>()
</script>

<template>
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
        <span>{{ selectedTenantName || selectedTenantId }}</span>
        <span>{{ selectedTenantType || 'tenant' }}</span>
        <span>Loaded {{ lastLoadedLabel }}</span>
        <span v-if="activeErrorCount > 0">{{ activeErrorCount }} API gaps</span>
      </div>
    </div>

    <div class="metric-grid">
      <article
        v-for="metric in metrics"
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
</template>
