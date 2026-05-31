<script setup lang="ts">
import CollapsibleTextCard from '~/components/dashboard/CollapsibleTextCard.vue'

const props = defineProps<{
  explanationMode: 'mvp' | 'future'
  startingSocSourceLabel: string
  telemetryFreshnessLabel: string
  selectedStrategyLabel: string
}>()
</script>

<template>
  <div class="baseline-explainer-grid">
    <CollapsibleTextCard
      :title="props.explanationMode === 'mvp' ? 'Where the selected baseline forecast comes from' : 'Where the future forecast should come from'"
      :eyebrow="props.explanationMode === 'mvp' ? 'Selected forecast source' : 'Future forecast source'"
    >
      <template v-if="props.explanationMode === 'mvp'">
        <p class="baseline-explainer-card__copy">
          The baseline forecast line comes from the deterministic LP preview solver in the API. The solver receives
          official/source-backed market rows, selected starting SOC, and battery limits, then returns hourly points from
          the <strong>forecast</strong> field. Published targets use official rows first; unpublished horizons may use
          governed forecast context.
        </p>
        <p class="baseline-explainer-card__formula">
          Displayed series: <strong>forecast[i] = solve_result.forecast[i].predicted_price_uah_mwh</strong>
        </p>
        <p class="baseline-explainer-card__copy">
          This panel is still the baseline LP preview, not an NBEATSx/TFT forecast and not bid intent. Starting SOC source:
          <strong>{{ props.startingSocSourceLabel }}</strong>; telemetry freshness:
          <strong>{{ props.telemetryFreshnessLabel }}</strong>.
          The selected strategy shown elsewhere is <strong>{{ props.selectedStrategyLabel }}</strong>.
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
          The explanation should move from "solver output" to "forecast model output plus uncertainty and attribution."
        </p>
      </template>
    </CollapsibleTextCard>

    <CollapsibleTextCard
      :title="props.explanationMode === 'mvp' ? 'How the feasible plan is built now' : 'How the future decision path should work'"
      :eyebrow="props.explanationMode === 'mvp' ? 'Selected feasible plan logic' : 'Future decision logic'"
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
          Research decision logic
        </p>
        <p class="baseline-explainer-card__copy">
          Future DT/LAVA work must compete against frozen V2+ first. Any learned action path should still be checked by
          the same deterministic battery and gatekeeper constraints.
        </p>
        <p class="baseline-explainer-card__formula">
          Research flow: <strong>forecast state + battery state + return target -> candidate schedule -> feasibility check</strong>
        </p>
        <p class="baseline-explainer-card__copy">
          The SOC line can stay, but its explanation should tie back to the validated policy trajectory rather than the
          selected LP recommendation schedule.
        </p>
      </template>
    </CollapsibleTextCard>
  </div>
</template>

<style scoped>
.baseline-explainer-grid {
  display: grid;
  gap: 0.9rem;
}

.baseline-explainer-card__eyebrow {
  color: var(--ink-soft);
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
}

.baseline-explainer-card__copy,
.baseline-explainer-card__formula {
  color: var(--ink-strong);
  font-size: 0.88rem;
  line-height: 1.55;
}
</style>
