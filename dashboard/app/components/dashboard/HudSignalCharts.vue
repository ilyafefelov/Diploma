<script setup lang="ts">
import { computed } from 'vue'

import ClientVChart from '~/components/dashboard/ClientVChart.vue'
import HudSignalMarketExplainers from '~/components/dashboard/signal/HudSignalMarketExplainers.vue'
import HudSignalScheduleExplainers from '~/components/dashboard/signal/HudSignalScheduleExplainers.vue'
import type { OperatorRecommendationResponse, SignalPreview } from '~/types/control-plane'
import type { OperatorChartHorizon, OperatorMarketVenue } from '~/types/operator-dashboard'
import { buildMarketPulseChartOption, buildSelectedStrategyDispatchChartOption, formatWeatherSourceLabel } from '~/utils/dashboardChartTheme'
import {
  operatorPriceContextModeLabel,
  operatorPriceContextSourceLabel,
  selectOperatorMarketSignalPreview,
  sliceOperatorRecommendationForChartHorizon,
  sliceSignalPreviewForChartHorizon
} from '~/utils/operatorPreviewControls'

const props = defineProps<{
  signalPreview: SignalPreview | null
  operatorRecommendation: OperatorRecommendationResponse | null
  selectedMarketVenue: OperatorMarketVenue
  selectedChartHorizon: OperatorChartHorizon
  marketPreviewError: string
  isLoading: boolean
  lastLoadedLabel: string
  explanationMode: 'mvp' | 'future'
}>()

const selectedMarketSignalPreview = computed(() => selectOperatorMarketSignalPreview(
  props.signalPreview,
  props.operatorRecommendation
))
const visibleSignalPreview = computed(() => sliceSignalPreviewForChartHorizon(
  selectedMarketSignalPreview.value,
  props.selectedChartHorizon
))
const visibleOperatorRecommendation = computed(() => sliceOperatorRecommendationForChartHorizon(
  props.operatorRecommendation,
  props.selectedChartHorizon
))
const marketOption = computed(() => buildMarketPulseChartOption(
  visibleSignalPreview.value,
  props.selectedMarketVenue
))
const dispatchOption = computed(() => buildSelectedStrategyDispatchChartOption(
  visibleOperatorRecommendation.value,
  visibleSignalPreview.value
))
const selectedStrategyLabel = computed(() => {
  if (!props.operatorRecommendation) {
    return 'selected strategy pending'
  }

  const selectedOption = props.operatorRecommendation.available_strategies.find(strategy =>
    strategy.strategy_id === props.operatorRecommendation?.selected_strategy_id
  )

  return selectedOption?.label || props.operatorRecommendation.selected_strategy_id
})
const hasSelectedSchedule = computed(() => (props.operatorRecommendation?.recommendation_schedule.length || 0) > 0)
const hasMarketPreviewBlocker = computed(() => props.marketPreviewError.trim().length > 0)
const priceContextModeLabel = computed(() => operatorPriceContextModeLabel(props.operatorRecommendation))
const priceContextSourceLabel = computed(() => operatorPriceContextSourceLabel(props.operatorRecommendation))
const weatherSourceBadge = computed(() => {
  const sources = selectedMarketSignalPreview.value?.weather_sources || []

  if (sources.length === 0) {
    return 'Price source: not loaded yet'
  }

  const formattedSources = [...new Set(sources.map(source => formatWeatherSourceLabel(source)))]

  if (formattedSources.length === 1) {
    return `Price source: ${formattedSources[0]}`
  }

  return `Price sources: ${formattedSources.join(' + ')}`
})
</script>

<template>
  <div class="signal-grid">
    <section class="signal-card">
      <div class="signal-card__header">
        <div>
          <p class="signal-card__eyebrow">
            Market pulse
          </p>
          <h3 class="signal-card__title">
            Market context for the selected strategy
          </h3>
          <p
            v-if="hasMarketPreviewBlocker"
            class="signal-card__summary"
          >
            Source-backed {{ selectedMarketVenue }} rows are not available for this preview. The chart is withheld so a
            general signal surface is not relabelled as official {{ selectedMarketVenue }} context.
          </p>
          <p
            v-else
            class="signal-card__summary"
          >
            This chart explains the price/weather context visible to <strong>{{ selectedStrategyLabel }}</strong>.
            Read it as {{ priceContextModeLabel }} from <strong>{{ priceContextSourceLabel }}</strong>. It is context,
            not a bid; the selected preview schedule is shown in Dispatch Balance and the schedule dock. All values use
            <strong>UAH/MWh</strong>.
          </p>
        </div>

        <p class="signal-card__meta">
          Updated {{ lastLoadedLabel }}
        </p>
      </div>

      <div class="signal-card__guide">
        <span class="signal-guide-pill">Y-axis: UAH/MWh</span>
        <span class="signal-guide-pill signal-guide-pill-blue">Blue line: selected-period price context</span>
        <span class="signal-guide-pill">Green bars: extra effect from weather</span>
        <span class="signal-guide-pill">Dashed green: final price after weather</span>
        <span class="signal-guide-pill signal-guide-pill-source">{{ weatherSourceBadge }}</span>
        <span class="signal-guide-pill signal-guide-pill-source">Review context for selected preview</span>
        <span class="signal-guide-pill">Bottom axis: local time of day</span>
      </div>

      <div
        v-if="hasMarketPreviewBlocker"
        class="signal-chart signal-chart-fallback"
      >
        {{ marketPreviewError }}
      </div>
      <div
        v-else-if="isLoading"
        class="signal-chart signal-chart-fallback"
      >
        Loading market pulse...
      </div>
      <ClientVChart
        v-else
        :option="marketOption"
        autoresize
        class="signal-chart"
      />

      <HudSignalMarketExplainers :explanation-mode="props.explanationMode" />
    </section>

    <section class="signal-card">
      <div class="signal-card__header">
        <div>
          <p class="signal-card__eyebrow">
            Schedule balance
          </p>
          <h3 class="signal-card__title">
            Selected schedule and value preview
          </h3>
          <p
            v-if="hasMarketPreviewBlocker"
            class="signal-card__summary"
          >
            Schedule/value preview is blocked until the selected venue has source-backed rows or valid
            pre-publication NBEATSx/TFT evidence. No substitute prices are rendered.
          </p>
          <p
            v-else
            class="signal-card__summary"
          >
            Blue bars now follow <strong>{{ selectedStrategyLabel }}</strong> from the operator recommendation endpoint.
            Lines show selected net value and visible value gap for review. This is the same preview strategy family as
            the lower schedule dock, still read-model evidence and not a dispatch command.
          </p>
        </div>

        <p class="signal-card__meta">
          {{ hasSelectedSchedule ? 'Selected-strategy preview' : 'API-backed preview' }}
        </p>
      </div>

      <div class="signal-card__guide">
        <span class="signal-guide-pill signal-guide-pill-blue">Bars: selected net power in MW</span>
        <span class="signal-guide-pill">Green line: selected net value in UAH</span>
        <span class="signal-guide-pill signal-guide-pill-berry">Pink line: visible value gap in UAH</span>
        <span class="signal-guide-pill">Preview only: not dispatch command</span>
        <span class="signal-guide-pill">Feasibility is re-solved before display</span>
      </div>

      <div
        v-if="hasMarketPreviewBlocker"
        class="signal-chart signal-chart-fallback"
      >
        {{ marketPreviewError }}
      </div>
      <div
        v-else-if="isLoading"
        class="signal-chart signal-chart-fallback"
      >
        Loading schedule preview...
      </div>
      <ClientVChart
        v-else
        :option="dispatchOption"
        autoresize
        class="signal-chart"
      />

      <HudSignalScheduleExplainers :explanation-mode="props.explanationMode" />
    </section>
  </div>
</template>

<style scoped src="../../assets/css/hud-signal-charts.css"></style>
