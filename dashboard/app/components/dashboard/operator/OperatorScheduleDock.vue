<script setup lang="ts">
import { computed, ref } from 'vue'
import type { OperatorTimelineSegment } from '~/types/operator-dashboard'

const props = defineProps<{
  selectedTenantName: string
  selectedTenantBadge: string
  timelineSegments: OperatorTimelineSegment[]
  dispatchModeLabel: string
  predictionHeadLabel: string
}>()

const activeTooltipSegment = ref<OperatorTimelineSegment | null>(null)
const tooltipLeft = ref(12)
const tooltipTop = ref(12)

const tooltipStyle = computed(() => ({
  left: `${tooltipLeft.value}px`,
  top: `${tooltipTop.value}px`
}))

const showSegmentTooltip = (segment: OperatorTimelineSegment, event: MouseEvent | FocusEvent): void => {
  const target = event.currentTarget

  if (!(target instanceof HTMLElement)) {
    return
  }

  const rect = target.getBoundingClientRect()
  const viewportWidth = window.innerWidth
  const tooltipWidth = Math.min(300, viewportWidth - 24)
  activeTooltipSegment.value = segment
  tooltipLeft.value = Math.max(12, Math.min(rect.left + rect.width / 2 - tooltipWidth / 2, viewportWidth - tooltipWidth - 12))
  tooltipTop.value = Math.max(12, rect.top - 116)
}

const hideSegmentTooltip = (): void => {
  activeTooltipSegment.value = null
}
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
        v-for="segment in props.timelineSegments"
        :key="`${segment.time}-${segment.label}`"
        class="schedule-segment"
        :class="`schedule-segment--${segment.tone}`"
        tabindex="0"
        @mouseenter="showSegmentTooltip(segment, $event)"
        @focus="showSegmentTooltip(segment, $event)"
        @mouseleave="hideSegmentTooltip"
        @blur="hideSegmentTooltip"
      >
        <span>{{ segment.time }}</span>
        <strong>{{ segment.label }}</strong>
        <small>{{ segment.value }}</small>
      </article>
    </div>

    <div
      v-if="activeTooltipSegment"
      class="schedule-dock__floating-tooltip"
      :style="tooltipStyle"
      role="tooltip"
    >
      <span class="schedule-tooltip__title">{{ activeTooltipSegment.tooltipTitle }}</span>
      <span class="schedule-tooltip__body">{{ activeTooltipSegment.tooltipBody }}</span>
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
