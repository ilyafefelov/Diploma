<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  statusLabel: string
  socPercent: number
  sohPercent: number
  powerLabel: string
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
  <section class="surface-panel battery-panel">
    <div class="rail-heading">
      <div>
        <p class="eyebrow">
          Battery status
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
        <div class="mini-meter">
          <span :style="{ width: `${socPercent}%` }" />
        </div>
        <span
          class="metric-lens-card__tooltip"
          role="tooltip"
        >
          <span class="metric-lens-card__tooltip-title">State of charge</span>
          <span>Formula: SOC = projected_soc_after_fraction × 100%</span>
          <span>Interpretation: current battery charge readiness for the next dispatch slice.</span>
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
        <div class="mini-meter mini-meter-green">
          <span :style="{ width: `${sohPercent}%` }" />
        </div>
        <span
          class="metric-lens-card__tooltip"
          role="tooltip"
        >
          <span class="metric-lens-card__tooltip-title">Degradation proxy</span>
          <span>Formula: SOH_proxy = 96.2 - throughput × 0.12</span>
          <span>Interpretation: estimated remaining battery health as a soft constraint signal.</span>
        </span>
      </article>
      <article class="metric-lens-card">
        <div class="metric-lens-card__label-row">
          <p>Latest power intent</p>
          <UIcon
            class="metric-lens-card__icon"
            name="i-lucide-wave-square"
          />
        </div>
        <strong>{{ powerLabel }}</strong>
        <small class="metric-lens-card__kicker">{{ safePowerMode }}</small>
        <span class="battery-stat-grid__meta">Limit ±100 MW</span>
        <span
          class="metric-lens-card__tooltip"
          role="tooltip"
        >
          <span class="metric-lens-card__tooltip-title">Intent to dispatch</span>
          <span>Formula: power_cmd = clamp((price_gap / max_deviation) × max_power_mw, -max_power_mw, +max_power_mw)</span>
          <span>Interpretation: positive power is export, negative means import/charge intent.</span>
        </span>
      </article>
    </div>
  </section>
</template>
