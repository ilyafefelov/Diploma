<script setup lang="ts">
import { computed, ref } from 'vue'
import type { OperatorMarketVenue, OperatorTimelineSegment } from '~/types/operator-dashboard'

import type { ShadowHourlyRecommendationRow } from '~/utils/operatorShadowPreview'

const props = defineProps<{
  selectedTenantName: string
  selectedTenantBadge: string
  timelineSegments: OperatorTimelineSegment[]
  dispatchModeLabel: string
  predictionHeadLabel: string
  marketBoundaryLabel: string
  selectedMarketVenue: OperatorMarketVenue
  batteryCapacityContextLabel: string
  deliveryWindowLabel: string
  selectedPreviewSourceLabel: string
  isShadowPreviewMode: boolean
  hourlyRecommendationRows: ShadowHourlyRecommendationRow[]
  hourlyEmptyMessage: string
  shadowPreviewLastLoadedLabel: string
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
        <p>{{ isShadowPreviewMode ? 'Shadow delivery-day preview' : `${selectedMarketVenue} hourly delivery review` }}</p>
        <span>{{ selectedTenantName }} / {{ selectedTenantBadge }}</span>
        <strong class="schedule-dock__battery-context">
          <UIcon name="i-lucide-battery-charging" />
          {{ batteryCapacityContextLabel }}
        </strong>
        <small>{{ deliveryWindowLabel }}</small>
        <small>{{ predictionHeadLabel }}</small>
        <small>Schedule shown: {{ selectedPreviewSourceLabel }}</small>
        <small>{{ marketBoundaryLabel }}</small>
        <small v-if="isShadowPreviewMode">
          DT/shadow loaded {{ shadowPreviewLastLoadedLabel }} / projected preview / manual refresh only
        </small>
      </div>
    </div>

    <div class="schedule-track">
      <article
        v-for="segment in props.timelineSegments"
        :key="`${segment.time}-${segment.label}`"
        class="schedule-segment"
        :class="`schedule-segment--${segment.tone}`"
        role="group"
        :aria-label="`${segment.time}: ${segment.label}. ${segment.value}. ${segment.marketSideLabel}; ${segment.indicativePriceLabel}`"
        tabindex="0"
        @mouseenter="showSegmentTooltip(segment, $event)"
        @focus="showSegmentTooltip(segment, $event)"
        @mouseleave="hideSegmentTooltip"
        @blur="hideSegmentTooltip"
      >
        <span>{{ segment.time }}</span>
        <strong>{{ segment.label }}</strong>
        <small>{{ segment.value }}</small>
        <em>{{ segment.marketSideLabel }} / {{ segment.indicativePriceLabel }}</em>
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
      <span class="schedule-tooltip__boundary">{{ activeTooltipSegment.marketBoundaryLabel }}</span>
    </div>

    <div
      class="shadow-hourly-table"
      aria-label="Hourly recommendation table"
    >
      <table class="shadow-hourly-table__table">
        <caption class="sr-only">
          Hourly recommendation table
        </caption>
        <thead>
          <tr class="shadow-hourly-table__head">
            <th scope="col">
              Timestamp
            </th>
            <th scope="col">
              Action
            </th>
            <th scope="col">
              MW / MWh / capacity
            </th>
            <th scope="col">
              SOC
            </th>
            <th scope="col">
              Candidate / family
            </th>
            <th scope="col">
              Value
            </th>
            <th scope="col">
              Regret / value gap
            </th>
            <th scope="col">
              Gate
            </th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="row in hourlyRecommendationRows"
            :key="`${row.timestamp}-${row.candidateLabel}`"
            class="shadow-hourly-table__row"
          >
            <td>{{ row.timestamp }}</td>
            <td>
              <strong>{{ row.action }}</strong>
            </td>
            <td>{{ row.quantityLabel }}</td>
            <td>{{ row.socPathLabel }}</td>
            <td>{{ row.candidateLabel }} / {{ row.scheduleFamily }}</td>
            <td>{{ row.expectedValueLabel }}</td>
            <td>{{ row.regretVsV2Label }}; {{ row.regretVsStrictLabel }}</td>
            <td>{{ row.gateStatus }}</td>
          </tr>
        </tbody>
      </table>
      <p v-if="hourlyRecommendationRows.length === 0">
        {{ hourlyEmptyMessage }}
      </p>
    </div>

    <div class="dock-selectors">
      <div class="dock-status-pill">
        <span>Selected strategy</span>
        <strong>{{ predictionHeadLabel }}</strong>
      </div>
      <div class="dock-status-pill">
        <span>Review mode</span>
        <strong>{{ dispatchModeLabel }}</strong>
      </div>
    </div>
  </footer>
</template>
