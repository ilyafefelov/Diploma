<script setup lang="ts">
import { computed } from 'vue'

import { BarChart, LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

import CollapsibleTextCard from '~/components/dashboard/CollapsibleTextCard.vue'
import type { BaselineLpPreview } from '~/types/control-plane'
import { buildBaselineForecastChartOption, buildBaselineScheduleChartOption } from '~/utils/dashboardChartTheme'

use([CanvasRenderer, LineChart, BarChart, GridComponent, TooltipComponent, LegendComponent])

const props = defineProps<{
  baselinePreview: BaselineLpPreview | null
  isLoading: boolean
  lastLoadedLabel: string
  explanationMode: 'mvp' | 'future'
}>()

const forecastOption = computed(() => buildBaselineForecastChartOption(props.baselinePreview))
const scheduleOption = computed(() => buildBaselineScheduleChartOption(props.baselinePreview))
const startingSocSourceLabel = computed(() => props.baselinePreview?.starting_soc_source || 'not reported')
const telemetryFreshnessLabel = computed(() => formatTelemetryFreshness(props.baselinePreview?.telemetry_freshness))

const economicsItems = computed(() => {
  if (!props.baselinePreview) {
    return [
      {
        label: 'Gross value',
        value: 'Waiting',
        tooltipTitle: 'Gross market value',
        tooltipBody: 'Projected market revenue before degradation cost is applied.',
        tooltipFormula: 'Calculated by summing hourly market value across the recommendation schedule.'
      },
      {
        label: 'Degradation',
        value: 'Waiting',
        tooltipTitle: 'Degradation penalty',
        tooltipBody: 'Estimated battery wear cost from moving energy through the pack.',
        tooltipFormula: 'Calculated from simulated throughput and the configured cost per full cycle.'
      },
      {
        label: 'Net value',
        value: 'Waiting',
        tooltipTitle: 'Net plan value',
        tooltipBody: 'Projected economic outcome after subtracting battery wear from gross market value.',
        tooltipFormula: 'Gross value minus degradation penalty.'
      },
      {
        label: 'Throughput',
        value: 'Waiting',
        tooltipTitle: 'Battery throughput',
        tooltipBody: 'Total energy expected to pass through the battery during the feasible plan.',
        tooltipFormula: 'Sum of hourly charge and discharge energy handled by the projected state model.'
      }
    ]
  }

  const economics = props.baselinePreview.economics

  return [
    {
      label: 'Gross value',
      value: `${Math.round(economics.total_gross_market_value_uah).toLocaleString('en-GB')} UAH`,
      tooltipTitle: 'Gross market value',
      tooltipBody: 'This is the projected market-facing revenue from the baseline LP schedule before battery wear is charged against it.',
      tooltipFormula: 'Built by summing the hourly gross market value of every scheduled recommendation point.'
    },
    {
      label: 'Degradation',
      value: `${Math.round(economics.total_degradation_penalty_uah).toLocaleString('en-GB')} UAH`,
      tooltipTitle: 'Degradation penalty',
      tooltipBody: 'This is the expected battery wear cost caused by executing the feasible plan through the projected battery model.',
      tooltipFormula: 'Built from total simulated throughput and the configured degradation cost per equivalent full cycle.'
    },
    {
      label: 'Net value',
      value: `${Math.round(economics.total_net_value_uah).toLocaleString('en-GB')} UAH`,
      tooltipTitle: 'Net plan value',
      tooltipBody: 'This is the operator-facing value left after the battery wear penalty is deducted from gross market value.',
      tooltipFormula: 'Built as gross value minus degradation penalty across the full recommendation horizon.'
    },
    {
      label: 'Throughput',
      value: `${economics.total_throughput_mwh.toFixed(2)} MWh`,
      tooltipTitle: 'Battery throughput',
      tooltipBody: 'This is the total energy volume that the battery is expected to process while following the feasible plan.',
      tooltipFormula: 'Built by summing hourly charge and discharge energy from the projected state trace.'
    }
  ]
})

const feasiblePlanItems = computed(() => {
  if (!props.baselinePreview) {
    return [
      {
        label: 'Power corridor',
        value: 'Waiting',
        note: 'Signed dispatch envelope in MW.',
        tooltipTitle: 'Signed dispatch limits',
        tooltipBody: 'Fallback clip band used to keep every simulated step within the inverter and safety envelope.',
        tooltipFormula: 'power_cmd_clipped = clamp(recommended_net_power_mw, -Pmax, +Pmax)'
      },
      {
        label: 'SOC guardrails',
        value: 'Waiting',
        note: 'Projected battery band in %.',
        tooltipTitle: 'SOC guardrails',
        tooltipBody: 'Preview SOC must remain inside feasible charge limits for every timestep.',
        tooltipFormula: 'SOC_next = SOC_prev + (charge_eff × positive_power - discharge_power / discharge_eff) × Δt / E_cap'
      },
      {
        label: 'Planning grain',
        value: 'Waiting',
        note: 'Hourly review step for the preview.',
        tooltipTitle: 'Dispatch grain',
        tooltipBody: 'Controls how often the recommendation point is evaluated and executed.',
        tooltipFormula: 'control_points = total_horizon_minutes / interval_minutes'
      }
    ]
  }

  const metrics = props.baselinePreview.battery_metrics

  return [
    {
      label: 'Power corridor',
      value: `-${metrics.max_power_mw.toFixed(1)} to +${metrics.max_power_mw.toFixed(1)} MW`,
      note: 'Negative values mean charging, positive values mean discharge.',
      tooltipTitle: 'Signed dispatch limits',
      tooltipBody: 'The feasible model clamps every action to this battery-inverter corridor so no step violates nominal capability.',
      tooltipFormula: 'power_command = clamp(raw_command, -Pmax, +Pmax)'
    },
    {
      label: 'SOC guardrails',
      value: `${Math.round(metrics.soc_min_fraction * 100)}% to ${Math.round(metrics.soc_max_fraction * 100)}%`,
      note: 'Projected state must stay inside the feasible battery window.',
      tooltipTitle: 'SOC guardrails',
      tooltipBody: 'Battery SOC is constrained to this admissible band for reliability and longevity.',
      tooltipFormula: 'soc_min_fraction ≤ SOC_t ≤ soc_max_fraction'
    },
    {
      label: 'Planning grain',
      value: `${props.baselinePreview.interval_minutes} min`,
      note: 'Every recommendation point is one operator review bucket.',
      tooltipTitle: 'Dispatch grain',
      tooltipBody: 'The interval length sets both smoothing of policy signal and schedule granularity.',
      tooltipFormula: 'Δt = interval_minutes / 60'
    }
  ]
})

const formatTelemetryFreshness = (freshness: Record<string, unknown> | null | undefined): string => {
  if (!freshness) {
    return 'not reported'
  }

  const freshnessLabel = freshness.telemetry_freshness
    ?? freshness.freshness
    ?? freshness.status

  return typeof freshnessLabel === 'string' ? freshnessLabel : 'metadata available'
}
</script>

<template>
  <section class="baseline-slab">
    <div class="baseline-slab__header">
      <div>
        <p class="baseline-slab__eyebrow">
          Slice 2 preview
        </p>
        <h3 class="baseline-slab__title">
          Baseline LP recommendation surface
        </h3>
      </div>

      <div class="baseline-slab__meta-block">
        <p class="baseline-slab__meta">
          Updated {{ lastLoadedLabel }}
        </p>
        <p class="baseline-slab__meta baseline-slab__meta-soft">
          Recommendation preview only, not bid intent
        </p>
      </div>
    </div>

    <div class="baseline-slab__economics">
      <article
        v-for="item in economicsItems"
        :key="item.label"
        class="economics-pill economics-pill-interactive"
        tabindex="0"
      >
        <p class="economics-pill__label">
          {{ item.label }}
        </p>
        <p class="economics-pill__value">
          {{ item.value }}
        </p>

        <div
          class="sims-tooltip"
          role="tooltip"
        >
          <div class="sims-tooltip__topline">
            <span class="sims-tooltip__plumbob" />
            <p class="sims-tooltip__eyebrow">
              Metric explainer
            </p>
          </div>
          <p class="sims-tooltip__title">
            {{ item.tooltipTitle }}
          </p>
          <p class="sims-tooltip__body">
            {{ item.tooltipBody }}
          </p>
          <p class="sims-tooltip__formula">
            {{ item.tooltipFormula }}
          </p>
        </div>
      </article>
    </div>

    <div class="baseline-feasible-strip">
      <article
        v-for="item in feasiblePlanItems"
        :key="item.label"
        class="feasible-pill feasible-pill-interactive"
        tabindex="0"
      >
        <p class="feasible-pill__label">
          {{ item.label }}
        </p>
        <p class="feasible-pill__value">
          {{ item.value }}
        </p>
        <p class="feasible-pill__note">
          {{ item.note }}
        </p>
        <div
          class="sims-tooltip"
          role="tooltip"
        >
          <div class="sims-tooltip__topline">
            <span class="sims-tooltip__plumbob" />
            <p class="sims-tooltip__eyebrow">
              Metric explainer
            </p>
          </div>
          <p class="sims-tooltip__title">
            {{ item.tooltipTitle }}
          </p>
          <p class="sims-tooltip__body">
            {{ item.tooltipBody }}
          </p>
          <p class="sims-tooltip__formula">
            {{ item.tooltipFormula }}
          </p>
        </div>
      </article>
    </div>

    <div class="baseline-slab__grid">
      <section class="baseline-card baseline-card-forecast">
        <div class="baseline-card__header">
          <div>
            <p class="baseline-card__eyebrow">
              Forecast horizon
            </p>
            <h4 class="baseline-card__title">
              Hourly DAM baseline forecast
            </h4>
            <p class="baseline-card__summary">
              Y-axis values are quoted in <strong>UAH/MWh</strong>.
            </p>
          </div>
        </div>

        <div
          v-if="isLoading"
          class="baseline-chart baseline-chart-fallback"
        >
          Loading baseline forecast...
        </div>
        <VChart
          v-else
          :option="forecastOption"
          autoresize
          class="baseline-chart"
        />
      </section>

      <section class="baseline-card baseline-card-balance">
        <div class="baseline-card__header">
          <div>
            <p class="baseline-card__eyebrow">
              Feasible plan
            </p>
            <h4 class="baseline-card__title">
              Signed MW schedule and projected SOC
            </h4>
            <p class="baseline-card__summary">
              Bars use signed <strong>MW</strong>; the pink line is projected <strong>SOC %</strong> after each feasible step.
            </p>
          </div>
        </div>

        <div
          v-if="isLoading"
          class="baseline-chart baseline-chart-fallback"
        >
          Loading projected state...
        </div>
        <VChart
          v-else
          :option="scheduleOption"
          autoresize
          class="baseline-chart"
        />
      </section>
    </div>

    <div class="baseline-explainer-grid">
      <CollapsibleTextCard
        :title="props.explanationMode === 'mvp' ? 'Where the current baseline forecast comes from' : 'Where the future forecast should come from'"
        :eyebrow="props.explanationMode === 'mvp' ? 'Current forecast source' : 'Future forecast source'"
      >
        <template v-if="props.explanationMode === 'mvp'">
          <p class="baseline-explainer-card__copy">
            The baseline forecast line comes from <strong>HourlyDamBaselineSolver.solve_next_dispatch</strong> in the API.
            The solver receives tenant-aware DAM price history, current SOC, and battery limits, then returns hourly
            points from the <strong>forecast</strong> field. When the real-data stack is materialized, that history is
            observed OREE DAM data; any synthetic fallback is demo-grade only.
          </p>
          <p class="baseline-explainer-card__formula">
            Displayed series: <strong>forecast[i] = solve_result.forecast[i].predicted_price_uah_mwh</strong>
          </p>
          <p class="baseline-explainer-card__copy">
            This panel is still the baseline LP preview, not an NBEATSx/TFT forecast and not bid intent. Starting SOC source:
            <strong>{{ startingSocSourceLabel }}</strong>; telemetry freshness:
            <strong>{{ telemetryFreshnessLabel }}</strong>.
          </p>
        </template>
        <template v-else>
          <p class="baseline-explainer-card__eyebrow">
            Future forecast source
          </p>
          <p class="baseline-explainer-card__copy">
            In production, this chart should be fed by the dedicated forecast stack, most likely <strong>NBEATSx</strong>
            and <strong>TFT</strong>, with richer weather, calendar, and market-state features.
          </p>
          <p class="baseline-explainer-card__formula">
            Target series: <strong>forecast = model(price_history, weather, calendar, market_state)</strong>
          </p>
          <p class="baseline-explainer-card__copy">
            The explanation should move from “solver output” to “forecast model output plus uncertainty and attribution.”
          </p>
        </template>
      </CollapsibleTextCard>

      <CollapsibleTextCard
        :title="props.explanationMode === 'mvp' ? 'How the feasible plan is built now' : 'How the future decision path should work'"
        :eyebrow="props.explanationMode === 'mvp' ? 'Current feasible plan logic' : 'Future decision logic'"
        tone="accent"
      >
        <template v-if="props.explanationMode === 'mvp'">
          <p class="baseline-explainer-card__copy">
            The bar chart uses <strong>recommendation_schedule[].recommended_net_power_mw</strong>. The pink line uses
            <strong>projected_state.trace[].soc_after_fraction</strong> converted to percent after feasibility simulation.
          </p>
          <p class="baseline-explainer-card__formula">
            Displayed SOC: <strong>soc_percent = projected_state.trace[i].soc_after_fraction * 100</strong>
          </p>
          <p class="baseline-explainer-card__copy">
            The plan is feasible because it is run through the projected battery model with capacity, power, SOC limits,
            efficiency, and degradation cost taken from the battery metrics in the API response.
          </p>
        </template>
        <template v-else>
          <p class="baseline-explainer-card__eyebrow">
            Future decision logic
          </p>
          <p class="baseline-explainer-card__copy">
            Once decisions move to <strong>DT/M3DT</strong>, the action path should come from a learned policy and then be
            checked by the same deterministic battery and gatekeeper constraints.
          </p>
          <p class="baseline-explainer-card__formula">
            Target flow: <strong>forecast state + battery state + return target -> policy schedule -> feasibility check</strong>
          </p>
          <p class="baseline-explainer-card__copy">
            The SOC line can stay, but its explanation should tie back to the validated policy trajectory rather than the
            current LP recommendation schedule.
          </p>
        </template>
      </CollapsibleTextCard>
    </div>

    <CollapsibleTextCard
      class="baseline-boundary"
      title="Planning boundary"
      eyebrow="Operator boundary"
      tone="default"
    >
      <template v-if="props.explanationMode === 'mvp'">
        <p class="baseline-boundary__copy">
          This surface shows a feasible hourly recommendation derived from the baseline LP and constrained battery state.
          It is for operator review and demo planning, not market-order or dispatch semantics.
        </p>
        <p class="baseline-boundary__copy baseline-boundary__copy-strong">
          Feasible plan means the preview already respects the visible power corridor, SOC guardrails, interval grain,
          and degradation-aware projected state.
        </p>
      </template>
      <template v-else>
        <p class="baseline-boundary__copy">
          In the future stack, this panel should become the policy-review surface: forecast output, chosen trajectory,
          deterministic constraint checks, and operator-readable reasons for the action path.
        </p>
        <p class="baseline-boundary__copy baseline-boundary__copy-strong">
          The LP surface remains useful as a benchmark, but the production explanation should reference the final policy,
          its counterfactual value, and the safety checks that accepted or rejected it.
        </p>
      </template>
    </CollapsibleTextCard>
  </section>
</template>

<style scoped>
.baseline-slab {
  position: relative;
  display: grid;
  gap: 0.85rem;
  padding: 0.95rem;
  border-radius: 0.95rem;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.12), transparent 42%),
    radial-gradient(circle at top left, rgba(126, 211, 33, 0.18), transparent 24%),
    linear-gradient(180deg, rgba(0, 117, 188, 0.96), rgba(0, 63, 122, 0.96));
  border: 1px solid rgba(255, 255, 255, 0.64);
  box-shadow:
    0 18px 38px rgba(0, 53, 103, 0.28),
    inset 0 1px 0 rgba(255, 255, 255, 0.34);
  overflow: visible;
}

.baseline-slab__header,
.baseline-card__header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 0.85rem;
}

.baseline-slab__eyebrow,
.baseline-card__eyebrow,
.economics-pill__label,
.baseline-boundary__title {
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: rgba(215, 255, 79, 0.82);
}

.baseline-slab__title,
.baseline-card__title {
  margin-top: 0.35rem;
  color: white;
  line-height: 1.05;
  text-shadow: 0 2px 7px rgba(0, 42, 82, 0.28);
}

.baseline-slab__title {
  font-size: 1.25rem;
}

.baseline-card__title {
  font-size: 1.08rem;
}

.baseline-slab__meta-block {
  display: grid;
  gap: 0.15rem;
  text-align: right;
}

.baseline-slab__meta {
  font-size: 0.84rem;
  color: rgba(236, 250, 255, 0.86);
}

.baseline-slab__meta-soft {
  color: rgba(229, 249, 255, 0.68);
}

.baseline-slab__economics,
.baseline-feasible-strip,
.baseline-slab__grid,
.baseline-explainer-grid {
  display: grid;
  gap: 0.9rem;
}

.economics-pill,
.feasible-pill,
.baseline-card,
.baseline-boundary {
  display: grid;
  gap: 0.35rem;
  padding: 0.72rem;
  border: 1px solid rgba(255, 255, 255, 0.26);
  border-radius: 0.72rem;
  background:
    radial-gradient(circle at top right, rgba(126, 211, 33, 0.12), transparent 28%),
    linear-gradient(180deg, rgba(8, 132, 204, 0.74), rgba(3, 74, 137, 0.72));
  transition: transform 180ms ease, box-shadow 180ms ease, border-color 180ms ease;
}

.baseline-explainer-card__eyebrow {
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.baseline-explainer-card__copy,
.baseline-explainer-card__formula {
  font-size: 0.88rem;
  line-height: 1.55;
  color: var(--ink-strong);
}

.economics-pill-interactive {
  position: relative;
  cursor: help;
  isolation: isolate;
}

.economics-pill:hover,
.feasible-pill:hover,
.baseline-card:hover,
.baseline-boundary:hover {
  transform: translateY(-2px);
  border-color: rgba(255, 255, 255, 0.46);
  box-shadow:
    0 16px 30px rgba(0, 44, 87, 0.2),
    inset 0 1px 0 rgba(255, 255, 255, 0.22);
}

.economics-pill__value {
  font-size: 1.15rem;
  font-weight: 800;
  color: #b8ff32;
}

.sims-tooltip {
  position: absolute;
  left: 0;
  right: auto;
  top: calc(100% + 0.8rem);
  z-index: 4;
  width: min(19rem, calc(100vw - 3rem));
  display: grid;
  gap: 0.45rem;
  padding: 1rem 1.05rem;
  border: 2px solid rgba(255, 255, 255, 0.92);
  border-radius: 1.3rem;
  background:
    radial-gradient(circle at top right, rgba(126, 211, 33, 0.18), transparent 28%),
    radial-gradient(circle at bottom left, rgba(83, 178, 234, 0.14), transparent 24%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(236, 248, 255, 0.98));
  box-shadow:
    0 22px 48px rgba(0, 121, 193, 0.16),
    inset 0 1px 0 rgba(255, 255, 255, 0.92);
  opacity: 0;
  visibility: hidden;
  transform: translateY(0.35rem) scale(0.98);
  transform-origin: top left;
  transition: opacity 160ms ease, transform 160ms ease, visibility 160ms ease;
  pointer-events: none;
  overflow: hidden;
}

.sims-tooltip::before {
  content: '';
  position: absolute;
  inset: 0;
  pointer-events: none;
  background: linear-gradient(115deg, rgba(255, 255, 255, 0) 24%, rgba(255, 255, 255, 0.26) 38%, rgba(255, 255, 255, 0) 52%);
  transform: translateX(-130%);
}

.sims-tooltip::after {
  content: '';
  position: absolute;
  left: 1.2rem;
  bottom: calc(100% - 0.08rem);
  width: 1rem;
  height: 1rem;
  background: linear-gradient(135deg, rgba(255, 255, 255, 0.98), rgba(236, 248, 255, 0.98));
  border-left: 1px solid rgba(255, 255, 255, 0.92);
  border-top: 1px solid rgba(255, 255, 255, 0.92);
  transform: rotate(45deg);
}

.economics-pill-interactive:hover .sims-tooltip,
.economics-pill-interactive:focus-visible .sims-tooltip {
  opacity: 1;
  visibility: visible;
  transform: translateY(0) scale(1);
  animation: sims-tooltip-pop 220ms ease-out, sims-tooltip-float 2.4s ease-in-out 220ms infinite;
}

.economics-pill-interactive:hover .sims-tooltip::before,
.economics-pill-interactive:focus-visible .sims-tooltip::before {
  animation: sims-tooltip-sheen 1.2s ease-out forwards;
}

.baseline-slab__economics .economics-pill-interactive:nth-child(n + 3) .sims-tooltip,
.baseline-feasible-strip .feasible-pill-interactive:nth-child(3) .sims-tooltip {
  right: 0;
  left: auto;
  transform-origin: top right;
}

.baseline-slab__economics .economics-pill-interactive:nth-child(n + 3) .sims-tooltip::after,
.baseline-feasible-strip .feasible-pill-interactive:nth-child(3) .sims-tooltip::after {
  right: 1.2rem;
  left: auto;
}

.economics-pill-interactive:focus-visible {
  outline: none;
  border-color: rgba(0, 121, 193, 0.24);
  box-shadow: 0 0 0 4px rgba(83, 178, 234, 0.16);
}

.sims-tooltip__eyebrow {
  font-size: 0.68rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.sims-tooltip__topline {
  display: inline-flex;
  align-items: center;
  gap: 0.55rem;
}

.sims-tooltip__plumbob {
  width: 0.8rem;
  height: 1.15rem;
  clip-path: polygon(50% 0%, 100% 50%, 50% 100%, 0% 50%);
  background: linear-gradient(180deg, #cfff8e 0%, var(--plumbob-green) 100%);
  box-shadow: 0 0 16px rgba(126, 211, 33, 0.34);
  animation: sims-tooltip-plumbob 1.8s ease-in-out infinite;
}

.sims-tooltip__title {
  font-size: 1rem;
  font-weight: 800;
  color: var(--ink-strong);
  line-height: 1.25;
}

.sims-tooltip__body,
.sims-tooltip__formula {
  font-size: 0.85rem;
  line-height: 1.55;
  color: var(--ink-soft);
}

.sims-tooltip__formula {
  color: var(--sims-blue-deep);
  font-weight: 700;
}

@keyframes sims-tooltip-pop {
  0% {
    transform: translateY(0.45rem) scale(0.94);
  }

  65% {
    transform: translateY(-0.08rem) scale(1.01);
  }

  100% {
    transform: translateY(0) scale(1);
  }
}

@keyframes sims-tooltip-float {
  0%,
  100% {
    transform: translateY(0) scale(1);
  }

  50% {
    transform: translateY(-0.08rem) scale(1);
  }
}

@keyframes sims-tooltip-sheen {
  from {
    transform: translateX(-130%);
  }

  to {
    transform: translateX(130%);
  }
}

@keyframes sims-tooltip-plumbob {
  0%,
  100% {
    transform: translateY(0);
    filter: brightness(1);
  }

  50% {
    transform: translateY(-0.08rem);
    filter: brightness(1.08);
  }
}

.feasible-pill {
  align-content: start;
  background:
    radial-gradient(circle at top right, rgba(126, 211, 33, 0.12), transparent 26%),
    linear-gradient(180deg, rgba(8, 132, 204, 0.74), rgba(3, 74, 137, 0.72));
}

.feasible-pill__label {
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: rgba(215, 255, 79, 0.78);
}

.feasible-pill__value {
  font-size: 1rem;
  font-weight: 800;
  color: white;
}

.feasible-pill__note,
.baseline-card__summary {
  font-size: 0.88rem;
  line-height: 1.5;
  color: rgba(229, 249, 255, 0.76);
}

.baseline-chart {
  min-height: 19rem;
  border: 1px solid rgba(255, 255, 255, 0.36);
  border-radius: 0.72rem;
  background:
    linear-gradient(180deg, rgba(222, 245, 255, 0.94), rgba(191, 229, 250, 0.9));
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.74);
}

.baseline-chart-fallback {
  display: flex;
  align-items: center;
  justify-content: center;
  border: 2px dashed rgba(0, 121, 193, 0.16);
  border-radius: 1.2rem;
  color: var(--ink-soft);
}

.baseline-boundary__copy {
  line-height: 1.65;
  color: #17334d;
  font-weight: 650;
}

.baseline-boundary__copy-strong {
  color: #071f38;
  font-weight: 850;
}

@media (min-width: 860px) {
  .baseline-slab__economics {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }

  .baseline-feasible-strip {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .baseline-slab__grid {
    grid-template-columns: minmax(0, 1.1fr) minmax(0, 1fr);
  }
}
</style>
