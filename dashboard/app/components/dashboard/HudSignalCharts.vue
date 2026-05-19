<script setup lang="ts">
import { computed } from 'vue'

import { BarChart, LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

import CollapsibleTextCard from '~/components/dashboard/CollapsibleTextCard.vue'
import type { OperatorRecommendationResponse, SignalPreview } from '~/types/control-plane'
import { buildMarketPulseChartOption, buildSelectedStrategyDispatchChartOption, formatWeatherSourceLabel } from '~/utils/dashboardChartTheme'

use([CanvasRenderer, LineChart, BarChart, GridComponent, TooltipComponent, LegendComponent])

const props = defineProps<{
  signalPreview: SignalPreview | null
  operatorRecommendation: OperatorRecommendationResponse | null
  isLoading: boolean
  lastLoadedLabel: string
  explanationMode: 'mvp' | 'future'
}>()

const marketOption = computed(() => buildMarketPulseChartOption(props.signalPreview))
const dispatchOption = computed(() => buildSelectedStrategyDispatchChartOption(props.operatorRecommendation, props.signalPreview))
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
const weatherSourceBadge = computed(() => {
  const sources = props.signalPreview?.weather_sources || []

  if (sources.length === 0) {
    return 'Weather source: not loaded yet'
  }

  const formattedSources = [...new Set(sources.map(source => formatWeatherSourceLabel(source)))]

  if (formattedSources.length === 1) {
    return `Weather source: ${formattedSources[0]}`
  }

  return `Weather sources: ${formattedSources.join(' + ')}`
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
          <p class="signal-card__summary">
            This chart explains the price/weather context visible to <strong>{{ selectedStrategyLabel }}</strong>.
            Read it as market price plus weather effect. It is context, not a bid; the selected preview schedule is shown
            in Dispatch Balance and the schedule dock. All values use <strong>UAH/MWh</strong>.
          </p>
        </div>

        <p class="signal-card__meta">
          Updated {{ lastLoadedLabel }}
        </p>
      </div>

      <div class="signal-card__guide">
        <span class="signal-guide-pill">Y-axis: UAH/MWh</span>
        <span class="signal-guide-pill signal-guide-pill-blue">Blue line: expected hourly price</span>
        <span class="signal-guide-pill">Green bars: extra effect from weather</span>
        <span class="signal-guide-pill">Dashed green: final price after weather</span>
        <span class="signal-guide-pill signal-guide-pill-source">{{ weatherSourceBadge }}</span>
        <span class="signal-guide-pill signal-guide-pill-source">Use now: context for selected preview</span>
        <span class="signal-guide-pill">Bottom axis: local time of day</span>
      </div>

      <div
        v-if="isLoading"
        class="signal-chart signal-chart-fallback"
      >
        Loading market pulse...
      </div>
      <VChart
        v-else
        :option="marketOption"
        autoresize
        class="signal-chart"
      />

      <div class="signal-explainer-grid">
        <CollapsibleTextCard
          v-if="props.explanationMode === 'mvp'"
          title="How the current price is calculated"
          eyebrow="Current calculation"
        >
          <p class="signal-explainer-card__copy">
            <strong>Expected price</strong> comes from the current API read model for the selected tenant. The visible
            V2+ preview adapter can reuse this context, but the thesis result itself was validated offline on the
            365-anchor Ukrainian panel.
          </p>
          <p class="signal-explainer-card__formula">
            Formula: <strong>price_after_weather = market_price + weather_bias</strong>
          </p>
          <p class="signal-explainer-card__copy">
            <strong>Weather effect</strong> is predicted by a ridge-style calibration model trained on joined
            price-and-weather history for the selected location. The current features are cloud cover, precipitation,
            humidity above 65%, absolute temperature gap from 18C, effective solar, and wind speed.
          </p>
        </CollapsibleTextCard>

        <CollapsibleTextCard
          v-else
          title="How the research forecast should be used"
          eyebrow="Research forecast calculation"
        >
          <p class="signal-explainer-card__copy">
            In the current thesis architecture, raw <strong>NBEATSx</strong> and <strong>TFT</strong> forecasts are useful
            only after they become feasible schedules and pass the strict LP/oracle regret gate.
          </p>
          <p class="signal-explainer-card__formula">
            Evidence flow: <strong>market forecast model -> candidate schedules -> schedule/value gate</strong>
          </p>
          <p class="signal-explainer-card__copy">
            The weather explanation will shift from a single calibrated uplift number to model-driven attribution,
            for example feature importance, attention, uncertainty bands, and scenario-specific forecast deltas.
          </p>
        </CollapsibleTextCard>

        <CollapsibleTextCard
          class="signal-explainer-card-accent"
          :title="props.explanationMode === 'mvp' ? 'Current market and weather sources' : 'Future forecast evidence'"
          :eyebrow="props.explanationMode === 'mvp' ? 'Current data sources' : 'Research evidence data sources'"
          tone="accent"
        >
          <template v-if="props.explanationMode === 'mvp'">
            <p class="signal-explainer-card__copy">
              <strong>Price side:</strong> the API can use observed OREE DAM history when the real-data stack is
              materialized. Any synthetic fallback is demo-grade only and should not support thesis-grade claims.
            </p>
            <p class="signal-explainer-card__copy">
              <strong>Weather side:</strong> weather comes from <strong>Open-Meteo</strong> when available, otherwise from a
              synthetic fallback weather window. The badge above shows which source was used for the visible points.
            </p>
            <p class="signal-explainer-card__copy signal-explainer-card__copy-note">
              This explanation is specific to the visible preview path. Current thesis evidence is led by V2+
              schedule/value scoring; the dashboard preview is not live market execution.
            </p>
          </template>
          <template v-else>
            <p class="signal-explainer-card__eyebrow">
              Research evidence data sources
            </p>
            <p class="signal-explainer-card__copy">
              <strong>Forecast inputs:</strong> DAM or IDM market history, weather history and forecasts, calendar signals,
              regime context, and possibly cross-market coupling features.
            </p>
            <p class="signal-explainer-card__copy">
              <strong>Explanation surface:</strong> instead of one uplift bar, operators should expect forecast bands,
              feature attribution, and scenario comparisons tied directly to NBEATSx or TFT outputs.
            </p>
            <p class="signal-explainer-card__copy signal-explainer-card__copy-note">
              The visible chart can stay simple, but the explanation contract should move from heuristic uplift to
              model-backed evidence.
            </p>
          </template>
        </CollapsibleTextCard>
      </div>
    </section>

    <section class="signal-card">
      <div class="signal-card__header">
        <div>
          <p class="signal-card__eyebrow">
            Dispatch balance
          </p>
          <h3 class="signal-card__title">
            Selected dispatch and value preview
          </h3>
          <p class="signal-card__summary">
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
        v-if="isLoading"
        class="signal-chart signal-chart-fallback"
      >
        Loading dispatch preview...
      </div>
      <VChart
        v-else
        :option="dispatchOption"
        autoresize
        class="signal-chart"
      />

      <div class="signal-explainer-grid">
        <CollapsibleTextCard
          v-if="props.explanationMode === 'mvp'"
          title="How the selected schedule is calculated now"
          eyebrow="Selected strategy adapter"
        >
          <p class="signal-explainer-card__copy">
            The API asks for the selected strategy. For <strong>Offline V2+</strong>, the dashboard uses a frozen
            read-model preview adapter, then runs the same battery feasibility projection before returning the schedule.
          </p>
          <p class="signal-explainer-card__formula">
            Flow: <strong>selected strategy -> preview forecast/context -> feasible schedule -> operator review</strong>
          </p>
          <p class="signal-explainer-card__copy">
            <strong>Positive MW</strong> means discharge, and <strong>negative MW</strong> means charge. The lower dock
            filters idle hours so the next meaningful actions are visible first.
          </p>
        </CollapsibleTextCard>

        <CollapsibleTextCard
          v-else
          title="How dispatch should be decided later"
          eyebrow="Research dispatch logic"
        >
          <p class="signal-explainer-card__copy">
            In the target stack, the action bar should no longer be described as a normalized price-distance heuristic.
            Future DT/LAVA work should predict candidate schedules or schedule blocks first, then compete against V2+.
          </p>
          <p class="signal-explainer-card__formula">
            Research flow: <strong>forecast state + battery state + return target -> candidate schedule trajectory</strong>
          </p>
          <p class="signal-explainer-card__copy">
            At that point, action explanation should describe policy intent, safety constraints, and counterfactual value,
            not only price distance from a local average.
          </p>
        </CollapsibleTextCard>

        <CollapsibleTextCard
          :title="props.explanationMode === 'mvp' ? 'How value gap is calculated now' : 'What the future opportunity metric should mean'"
          :eyebrow="props.explanationMode === 'mvp' ? 'Selected value gap' : 'Future opportunity metric'"
          tone="rose"
        >
          <template v-if="props.explanationMode === 'mvp'">
            <p class="signal-explainer-card__copy">
              The value gap line is an operator-facing counterfactual preview: how much value is visible between the
              selected action and the best visible action at that hour. It is not market settlement revenue.
            </p>
            <p class="signal-explainer-card__formula">
              Interpretation: <strong>value_gap = best_visible_value - selected_action_value</strong>
            </p>
            <p class="signal-explainer-card__copy signal-explainer-card__copy-note">
              This value helps explain the preview schedule. It does not change the claim boundary:
              <strong>Offline Strategy Promotion</strong>, not live trading.
            </p>
          </template>
          <template v-else>
            <p class="signal-explainer-card__eyebrow">
              Future opportunity metric
            </p>
            <p class="signal-explainer-card__copy">
              In production, the pink line should become an explicit decision-quality metric such as regret against a
              counterfactual optimum, policy value gap, or expected opportunity cost under uncertainty.
            </p>
            <p class="signal-explainer-card__formula">
              Target interpretation: <strong>value_gap = value(best feasible action) - value(chosen action)</strong>
            </p>
            <p class="signal-explainer-card__copy signal-explainer-card__copy-note">
              That shift makes the explanation consistent with candidate-value learning and avoids carrying today’s heuristic score into a
              stronger decision stack.
            </p>
          </template>
        </CollapsibleTextCard>
      </div>
    </section>
  </div>
</template>

<style scoped>
.signal-grid {
  display: grid;
  gap: 0.78rem;
}

.signal-card {
  display: grid;
  gap: 0.82rem;
  min-width: 0;
  padding: 0.85rem;
  border: 1px solid rgba(255, 255, 255, 0.62);
  border-radius: 0.92rem;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.12), transparent 42%),
    radial-gradient(circle at top right, rgba(126, 211, 33, 0.2), transparent 32%),
    linear-gradient(180deg, rgba(0, 111, 185, 0.94), rgba(0, 54, 112, 0.94));
  box-shadow:
    0 16px 34px rgba(0, 53, 103, 0.26),
    inset 0 1px 0 rgba(255, 255, 255, 0.32);
  transition: transform 180ms ease, box-shadow 180ms ease;
}

.signal-card:hover {
  transform: translateY(-2px);
  box-shadow:
    0 20px 42px rgba(0, 53, 103, 0.32),
    inset 0 1px 0 rgba(255, 255, 255, 0.42);
}

.signal-card__header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 0.8rem;
}

.signal-card__eyebrow {
  font-size: 0.64rem;
  font-weight: 800;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: rgba(215, 255, 79, 0.84);
}

.signal-card__title {
  margin-top: 0.2rem;
  font-size: 1rem;
  line-height: 1.15;
  color: white;
  text-shadow: 0 2px 7px rgba(0, 42, 82, 0.28);
}

.signal-card__summary {
  margin-top: 0.34rem;
  max-width: 38rem;
  font-size: 0.78rem;
  line-height: 1.45;
  color: rgba(229, 249, 255, 0.78);
}

.signal-card__meta {
  flex: 0 0 auto;
  border-radius: 999px;
  background: rgba(126, 211, 33, 0.16);
  padding: 0.32rem 0.5rem;
  font-size: 0.68rem;
  color: rgba(230, 255, 179, 0.9);
  font-weight: 900;
}

.signal-card__guide {
  display: flex;
  flex-wrap: wrap;
  gap: 0.55rem;
}

.signal-guide-pill {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 0.34rem 0.56rem;
  background: rgba(126, 211, 33, 0.18);
  color: rgba(241, 253, 255, 0.9);
  font-size: 0.66rem;
  font-weight: 800;
}

.signal-guide-pill-blue {
  background: rgba(83, 209, 255, 0.2);
}

.signal-guide-pill-berry {
  background: rgba(255, 111, 174, 0.2);
}

.signal-guide-pill-source {
  background: rgba(28, 208, 160, 0.22);
}

.signal-chart {
  min-height: 21rem;
  border: 1px solid rgba(255, 255, 255, 0.28);
  border-radius: 0.72rem;
  background:
    linear-gradient(180deg, rgba(222, 245, 255, 0.94), rgba(191, 229, 250, 0.9));
  padding: 0.25rem 0;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.74);
}

.signal-chart-fallback {
  display: flex;
  align-items: center;
  justify-content: center;
  border: 2px dashed rgba(0, 121, 193, 0.16);
  border-radius: 1.25rem;
  color: rgba(230, 249, 255, 0.8);
}

.signal-explainer-grid {
  display: grid;
  gap: 0.55rem;
}

.signal-explainer-card__eyebrow {
  font-size: 0.7rem;
  font-weight: 800;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.signal-explainer-card__copy,
.signal-explainer-card__formula {
  font-size: 0.78rem;
  line-height: 1.55;
  color: var(--ink-strong);
}

.signal-explainer-card__formula {
  color: var(--ink-strong);
}

.signal-explainer-card__copy-note {
  color: var(--ink-soft);
}

@media (min-width: 960px) {
  .signal-grid {
    grid-template-columns: minmax(0, 1.06fr) minmax(0, 1fr);
  }

  .signal-explainer-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>
