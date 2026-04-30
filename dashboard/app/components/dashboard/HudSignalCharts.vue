<script setup lang="ts">
import { computed } from 'vue'

import { BarChart, LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

import CollapsibleTextCard from '~/components/dashboard/CollapsibleTextCard.vue'
import type { SignalPreview } from '~/types/control-plane'
import { buildDispatchBalanceChartOption, buildMarketPulseChartOption, formatWeatherSourceLabel } from '~/utils/dashboardChartTheme'

use([CanvasRenderer, LineChart, BarChart, GridComponent, TooltipComponent, LegendComponent])

const props = defineProps<{
  signalPreview: SignalPreview | null
  isLoading: boolean
  lastLoadedLabel: string
  explanationMode: 'mvp' | 'future'
}>()

const marketOption = computed(() => buildMarketPulseChartOption(props.signalPreview))
const dispatchOption = computed(() => buildDispatchBalanceChartOption(props.signalPreview))
const weatherSourceBadge = computed(() => {
  const sources = props.signalPreview?.weather_sources || []

  if (sources.length === 0) {
    return 'Weather source: not loaded yet'
  }

  const formattedSources = [...new Set(sources.map((source) => formatWeatherSourceLabel(source)))]

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
          <p class="signal-card__eyebrow">Market pulse</p>
          <h3 class="signal-card__title">How weather may change the expected electricity price</h3>
          <p class="signal-card__summary">
            This chart starts from the current MVP baseline DAM forecast for each hour, then adds a calibrated weather
            effect. Read it as: <strong>expected price</strong> + <strong>weather effect</strong> = <strong>weather-adjusted price</strong>.
            All values use <strong>UAH/MWh</strong>, which means Ukrainian hryvnia for one megawatt-hour of electricity.
          </p>
        </div>

        <p class="signal-card__meta">Updated {{ lastLoadedLabel }}</p>
      </div>

      <div class="signal-card__guide">
        <span class="signal-guide-pill">Y-axis: UAH/MWh</span>
        <span class="signal-guide-pill signal-guide-pill-blue">Blue line: expected hourly price</span>
        <span class="signal-guide-pill">Green bars: extra effect from weather</span>
        <span class="signal-guide-pill">Dashed green: final price after weather</span>
        <span class="signal-guide-pill signal-guide-pill-source">{{ weatherSourceBadge }}</span>
        <span class="signal-guide-pill">Bottom axis: local time of day</span>
      </div>

      <div v-if="isLoading" class="signal-chart signal-chart-fallback">Loading market pulse...</div>
      <VChart v-else :option="marketOption" autoresize class="signal-chart" />

      <div class="signal-explainer-grid">
        <CollapsibleTextCard
          v-if="props.explanationMode === 'mvp'"
          title="How the current price is calculated"
          eyebrow="Current calculation"
          :open="true"
        >
          <p class="signal-explainer-card__copy">
            <strong>Expected price</strong> comes from the current baseline solver path in the API. The backend builds a
            tenant-aware MVP DAM price history, resolves an anchor hour, runs the hourly baseline solver, and samples the
            resulting forecast horizon.
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
          title="How the future price should be calculated"
          eyebrow="Future production calculation"
          :open="true"
        >
          <p class="signal-explainer-card__copy">
            In the target architecture, <strong>expected price</strong> will come from a forecasting stack led by
            <strong>NBEATSx</strong> and <strong>TFT</strong>, not from the current MVP baseline solver path.
          </p>
          <p class="signal-explainer-card__formula">
            Target flow: <strong>market forecast model -> weather-aware feature attribution -> decision policy input</strong>
          </p>
          <p class="signal-explainer-card__copy">
            The weather explanation will shift from a single calibrated uplift number to model-driven attribution,
            for example feature importance, attention, uncertainty bands, and scenario-specific forecast deltas.
          </p>
        </CollapsibleTextCard>

        <CollapsibleTextCard
          class="signal-explainer-card-accent"
          :title="props.explanationMode === 'mvp' ? 'Current market and weather sources' : 'Future forecast evidence'"
          :eyebrow="props.explanationMode === 'mvp' ? 'Current data sources' : 'Future production data sources'"
          tone="accent"
        >
          <template v-if="props.explanationMode === 'mvp'">
            <p class="signal-explainer-card__copy">
              <strong>Price side:</strong> the MVP preview currently uses tenant-aware synthetic DAM history in the API, with
              tenant location bias plus simple hour-of-day adjustments.
            </p>
            <p class="signal-explainer-card__copy">
              <strong>Weather side:</strong> weather comes from <strong>Open-Meteo</strong> when available, otherwise from a
              synthetic fallback weather window. The badge above shows which source was used for the visible points.
            </p>
            <p class="signal-explainer-card__copy signal-explainer-card__copy-note">
              This explanation is specific to the current MVP path and will change once forecast generation moves to
              <strong>NBEATSx + TFT</strong> and downstream decisions move to <strong>DT/M3DT</strong>.
            </p>
          </template>
          <template v-else>
            <p class="signal-explainer-card__eyebrow">Future production data sources</p>
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
          <p class="signal-card__eyebrow">Dispatch balance</p>
          <h3 class="signal-card__title">Battery action and missed-value preview</h3>
          <p class="signal-card__summary">
            Blue bars show a simplified battery action preview derived from the weather-adjusted price curve. Pink line
            shows a simplified <strong>missed value</strong> score for operator review. Battery action is shown in
            <strong>MW</strong>, and missed value is shown in <strong>UAH</strong>.
          </p>
        </div>

        <p class="signal-card__meta">API-backed preview</p>
      </div>

      <div class="signal-card__guide">
        <span class="signal-guide-pill signal-guide-pill-blue">Bars: battery action in MW</span>
        <span class="signal-guide-pill signal-guide-pill-berry">Pink line: missed value in UAH</span>
        <span class="signal-guide-pill">Preview only: simplified operator explanation</span>
      </div>

      <div v-if="isLoading" class="signal-chart signal-chart-fallback">Loading dispatch preview...</div>
      <VChart v-else :option="dispatchOption" autoresize class="signal-chart" />

      <div class="signal-explainer-grid">
        <CollapsibleTextCard
          v-if="props.explanationMode === 'mvp'"
          title="How battery action is calculated now"
          eyebrow="Battery action formula"
        >
          <p class="signal-explainer-card__copy">
            The API first computes a weather-adjusted price for each visible hour. Then it compares each hour to the
            average adjusted price across the preview window and scales the difference into the battery power corridor.
          </p>
          <p class="signal-explainer-card__formula">
            Formula: <strong>charge_intent = clamp(((adjusted_price - avg_adjusted_price) / max_deviation) * max_power_mw, -max_power_mw, +max_power_mw)</strong>
          </p>
          <p class="signal-explainer-card__copy">
            In the current preview, <strong>positive MW</strong> means the hour looks more valuable for discharge and
            <strong>negative MW</strong> means it looks better for charge.
          </p>
        </CollapsibleTextCard>

        <CollapsibleTextCard
          v-else
          title="How dispatch should be decided later"
          eyebrow="Future dispatch logic"
        >
          <p class="signal-explainer-card__copy">
            In the target stack, the action bar should no longer be described as a normalized price-distance heuristic.
            It should come from a decision policy such as <strong>DT/M3DT</strong> that consumes forecasts, battery state,
            and economic context directly.
          </p>
          <p class="signal-explainer-card__formula">
            Target flow: <strong>forecast state + battery state + return target -> policy action trajectory</strong>
          </p>
          <p class="signal-explainer-card__copy">
            At that point, action explanation should describe policy intent, safety constraints, and counterfactual value,
            not only price distance from a local average.
          </p>
        </CollapsibleTextCard>

        <CollapsibleTextCard
          :title="props.explanationMode === 'mvp' ? 'How missed value is calculated now' : 'What the future opportunity metric should mean'"
          :eyebrow="props.explanationMode === 'mvp' ? 'Missed value formula' : 'Future opportunity metric'"
          tone="rose"
        >
          <template v-if="props.explanationMode === 'mvp'">
            <p class="signal-explainer-card__copy">
              Missed value is not a market settlement field and not the LP objective. It is a simplified operator-facing
              opportunity score used in this MVP to show where weather uplift and price deviation make an hour look more
              important.
            </p>
            <p class="signal-explainer-card__formula">
              Formula: <strong>missed_value = max(80, weather_bias * 2.4 + abs(adjusted_price - avg_adjusted_price) * 0.45)</strong>
            </p>
            <p class="signal-explainer-card__copy signal-explainer-card__copy-note">
              This value will be replaced later by explanations tied to the stronger stack: forecast attribution from
              <strong>NBEATSx/TFT</strong> and action-value or policy reasoning from <strong>DT/M3DT</strong>.
            </p>
          </template>
          <template v-else>
            <p class="signal-explainer-card__eyebrow">Future opportunity metric</p>
            <p class="signal-explainer-card__copy">
              In production, the pink line should become an explicit decision-quality metric such as regret against a
              counterfactual optimum, policy value gap, or expected opportunity cost under uncertainty.
            </p>
            <p class="signal-explainer-card__formula">
              Target interpretation: <strong>value_gap = value(best feasible action) - value(chosen action)</strong>
            </p>
            <p class="signal-explainer-card__copy signal-explainer-card__copy-note">
              That shift makes the explanation consistent with DT/M3DT and avoids carrying today’s heuristic score into a
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
  gap: 1rem;
}

.signal-card {
  display: grid;
  gap: 0.9rem;
  padding: 1.05rem;
  border-radius: 1.5rem;
  background:
    radial-gradient(circle at top right, rgba(126, 211, 33, 0.14), transparent 34%),
    radial-gradient(circle at bottom left, rgba(83, 178, 234, 0.12), transparent 30%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.84), rgba(243, 250, 255, 0.84));
  border: 2px solid rgba(255, 255, 255, 0.92);
  box-shadow: 0 20px 45px rgba(0, 121, 193, 0.08);
  transition: transform 180ms ease, box-shadow 180ms ease;
}

.signal-card:hover {
  transform: translateY(-2px);
  box-shadow: 0 26px 52px rgba(0, 121, 193, 0.12);
}

.signal-card__eyebrow {
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.signal-card__title {
  margin-top: 0.4rem;
  font-size: 1.15rem;
  line-height: 1.15;
  color: var(--ink-strong);
}

.signal-card__summary {
  margin-top: 0.45rem;
  max-width: 38rem;
  font-size: 0.92rem;
  line-height: 1.55;
  color: var(--ink-soft);
}

.signal-card__meta {
  font-size: 0.82rem;
  color: var(--ink-soft);
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
  padding: 0.45rem 0.75rem;
  background: rgba(126, 211, 33, 0.12);
  color: var(--ink-strong);
  font-size: 0.78rem;
  font-weight: 700;
}

.signal-guide-pill-blue {
  background: rgba(0, 121, 193, 0.12);
}

.signal-guide-pill-berry {
  background: rgba(255, 111, 174, 0.14);
}

.signal-guide-pill-source {
  background: rgba(28, 208, 160, 0.16);
}

.signal-chart {
  min-height: 16rem;
  padding: 0.2rem 0;
}

.signal-chart-fallback {
  display: flex;
  align-items: center;
  justify-content: center;
  border: 2px dashed rgba(0, 121, 193, 0.16);
  border-radius: 1.25rem;
  color: var(--ink-soft);
}

.signal-explainer-grid {
  display: grid;
  gap: 0.75rem;
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
  font-size: 0.85rem;
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
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .signal-explainer-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>