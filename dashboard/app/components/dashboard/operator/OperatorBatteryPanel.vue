<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  statusLabel: string
  socPercent: number
  socSourceLabel: string
  socFormula: string
  sohPercent: number
  sohSourceLabel: string
  sohFormula: string
  powerLabel: string
  telemetryIngestLabel: string
  telemetryIngestTooltip: string
}>()

const socBandLabel = computed(() => {
  if (props.socPercent < 8) {
    return 'Critical'
  }

  if (props.socPercent < 25) {
    return 'Low'
  }

  if (props.socPercent > 90) {
    return 'High'
  }

  return 'Healthy'
})

const safePowerMode = computed(() => {
  if (props.powerLabel.includes('-')) {
    return 'Charge'
  }

  if (props.powerLabel.includes('+')) {
    return 'Discharge'
  }

  return 'Hold'
})
</script>

<template>
  <section
    id="operator-battery"
    class="surface-panel battery-panel"
  >
    <div class="rail-heading">
      <div>
        <p class="eyebrow">
          Battery readiness
        </p>
        <h2 class="rail-title">
          {{ statusLabel }}
        </h2>
      </div>
      <UIcon
        class="rail-heading__icon"
        name="i-lucide-battery-charging"
      />
    </div>

    <div
      class="battery-ingest-pill"
      :title="telemetryIngestTooltip"
    >
      <UIcon name="i-lucide-radio-tower" />
      <span>{{ telemetryIngestLabel }}</span>
    </div>

    <div class="battery-stat-grid">
      <article class="metric-lens-card">
        <div class="metric-lens-card__label-row">
          <p>SOC</p>
          <UIcon
            class="metric-lens-card__icon"
            name="i-lucide-battery-full"
          />
        </div>
        <strong>{{ socPercent }}%</strong>
        <small class="metric-lens-card__kicker">{{ socBandLabel }}</small>
        <span class="battery-stat-grid__meta">{{ socSourceLabel }}</span>
        <div class="mini-meter">
          <span :style="{ width: `${socPercent}%` }" />
        </div>
        <span
          class="metric-lens-card__tooltip"
          role="tooltip"
        >
          <span class="metric-lens-card__tooltip-title">State of charge</span>
          <span>Formula: {{ socFormula }}</span>
          <span>Source priority: latest 5-minute telemetry, then hourly Silver snapshot, then baseline LP starting SOC.</span>
        </span>
      </article>
      <article class="metric-lens-card">
        <div class="metric-lens-card__label-row">
          <p>SOH proxy</p>
          <UIcon
            class="metric-lens-card__icon"
            name="i-lucide-heart-pulse"
          />
        </div>
        <strong>{{ sohPercent }}%</strong>
        <small class="metric-lens-card__kicker">Health estimate</small>
        <span class="battery-stat-grid__meta">{{ sohSourceLabel }}</span>
        <div class="mini-meter mini-meter-green">
          <span :style="{ width: `${sohPercent}%` }" />
        </div>
        <span
          class="metric-lens-card__tooltip"
          role="tooltip"
        >
          <span class="metric-lens-card__tooltip-title">Degradation proxy</span>
          <span>Formula: {{ sohFormula }}</span>
          <span>Interpretation: physical telemetry when present; otherwise Level 1 throughput proxy for operator context.</span>
        </span>
      </article>
      <article class="metric-lens-card">
        <div class="metric-lens-card__label-row">
          <p>First DAM action</p>
          <UIcon
            class="metric-lens-card__icon"
            name="i-lucide-activity"
          />
        </div>
        <strong>{{ powerLabel }}</strong>
        <small class="metric-lens-card__kicker">{{ safePowerMode }}</small>
        <span class="battery-stat-grid__meta">Review only</span>
        <span
          class="metric-lens-card__tooltip"
          role="tooltip"
        >
          <span class="metric-lens-card__tooltip-title">DAM delivery-hour preview</span>
          <span>Formula: preview_power = selected_schedule.recommended_net_power_mw for the first visible DAM action row.</span>
          <span>Interpretation: positive power means discharge review, negative means charge review; this is not a dispatch command.</span>
        </span>
      </article>
    </div>
  </section>
</template>
