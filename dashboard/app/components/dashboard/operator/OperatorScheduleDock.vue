<script setup lang="ts">
import type { OperatorTimelineSegment } from '~/types/operator-dashboard'

defineProps<{
  selectedTenantName: string
  selectedTenantBadge: string
  timelineSegments: OperatorTimelineSegment[]
  dispatchModeLabel: string
  predictionHeadLabel: string
}>()
</script>

<template>
  <footer class="schedule-dock">
    <div class="schedule-dock__heading">
      <UIcon name="i-lucide-clock-3" />
      <div>
        <p>Schedule timeline</p>
        <span>{{ selectedTenantName }} / {{ selectedTenantBadge }}</span>
        <small>{{ predictionHeadLabel }}</small>
      </div>
    </div>

    <div class="schedule-track">
      <article
        v-for="segment in timelineSegments"
        :key="`${segment.time}-${segment.label}`"
        class="schedule-segment"
        :class="`schedule-segment--${segment.tone}`"
        tabindex="0"
      >
        <span>{{ segment.time }}</span>
        <strong>{{ segment.label }}</strong>
        <small>{{ segment.value }}</small>
        <span
          class="schedule-tooltip"
          role="tooltip"
        >
          <span class="schedule-tooltip__title">{{ segment.tooltipTitle }}</span>
          <span class="schedule-tooltip__body">{{ segment.tooltipBody }}</span>
        </span>
      </article>
    </div>

    <div class="dock-selectors">
      <label>
        <span>Strategy mode</span>
        <USelect
          class="dock-select"
          :model-value="'arbitrage-reg'"
          :items="[
            { label: 'Arbitrage + Reg', value: 'arbitrage-reg' },
            { label: 'Arbitrage only', value: 'arbitrage-only' }
          ]"
          value-key="value"
          label-key="label"
          variant="none"
          color="info"
        />
      </label>
      <label>
        <span>Dispatch mode</span>
        <USelect
          class="dock-select"
          :model-value="dispatchModeLabel"
          :items="[
            { label: dispatchModeLabel, value: dispatchModeLabel },
            { label: 'Manual review', value: 'Manual review' }
          ]"
          value-key="value"
          label-key="label"
          variant="none"
          color="info"
        />
      </label>
    </div>
  </footer>
</template>
