<script setup lang="ts">
defineProps<{
  statusLabel: string
  socPercent: number
  sohPercent: number
  powerLabel: string
}>()
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
      <article>
        <p>SOC</p>
        <strong>{{ socPercent }}%</strong>
        <div class="mini-meter">
          <span :style="{ width: `${socPercent}%` }" />
        </div>
      </article>
      <article>
        <p>SOH proxy</p>
        <strong>{{ sohPercent }}%</strong>
        <div class="mini-meter mini-meter-green">
          <span :style="{ width: `${sohPercent}%` }" />
        </div>
      </article>
      <article>
        <p>Power</p>
        <strong>{{ powerLabel }}</strong>
        <span class="battery-stat-grid__meta">Limit ±100 MW</span>
      </article>
    </div>
  </section>
</template>
