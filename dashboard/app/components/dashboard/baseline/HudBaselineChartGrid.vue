<script setup lang="ts">
import ClientVChart from '~/components/dashboard/ClientVChart.vue'

defineProps<{
  isLoading: boolean
  forecastOption: Record<string, unknown>
  scheduleOption: Record<string, unknown>
}>()
</script>

<template>
  <div class="baseline-slab__grid">
    <section class="baseline-card baseline-card-forecast">
      <div class="baseline-card__header">
        <div>
          <p class="baseline-card__eyebrow">
            Forecast horizon
          </p>
          <h4 class="baseline-card__title">
            Baseline LP forecast input
          </h4>
          <p class="baseline-card__summary">
            This is the price curve used by the baseline LP comparator, not the selected strategy evidence path. Y-axis
            values are quoted in <strong>UAH/MWh</strong>.
          </p>
          <p class="baseline-card__note">
            Why the shape can differ from the market-signal chart: this line is the LP input after DAM-window alignment and
            cap-safe filtering. It is planning context, not a live observed market trace.
          </p>
        </div>
      </div>

      <div
        v-if="isLoading"
        class="baseline-chart baseline-chart-fallback"
      >
        Loading baseline forecast...
      </div>
      <ClientVChart
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
            Baseline signed MW schedule and projected SOC
          </h4>
          <p class="baseline-card__summary">
            Bars use signed <strong>MW</strong>; the pink line is projected <strong>SOC %</strong> after each baseline
            feasible step. Positive MW means discharge, negative MW means charge.
          </p>
        </div>
      </div>

      <div
        v-if="isLoading"
        class="baseline-chart baseline-chart-fallback"
      >
        Loading projected state...
      </div>
      <ClientVChart
        v-else
        :option="scheduleOption"
        autoresize
        class="baseline-chart"
      />
    </section>
  </div>
</template>

<style scoped>
.baseline-slab__grid {
  display: grid;
  gap: 0.9rem;
}

.baseline-card {
  display: grid;
  min-width: 0;
  gap: 0.35rem;
  padding: 0.72rem;
  border: 1px solid var(--operator-card-border);
  border-radius: 0.72rem;
  background:
    radial-gradient(circle at top right, var(--operator-card-accent-wash), transparent 28%),
    linear-gradient(180deg, var(--operator-card-gradient-top), var(--operator-card-gradient-bottom));
  transition: transform 180ms ease, box-shadow 180ms ease, border-color 180ms ease;
}

.baseline-card:hover {
  transform: translateY(-2px);
  border-color: color-mix(in oklab, var(--panel-strong) 52%, transparent);
  box-shadow:
    0 16px 30px color-mix(in oklab, var(--operator-surface) 22%, transparent),
    inset 0 1px 0 var(--operator-card-border);
}

.baseline-card__header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 0.85rem;
}

.baseline-card__eyebrow {
  color: var(--operator-accent-readable);
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
}

.baseline-card__title {
  margin-top: 0.35rem;
  color: var(--operator-control-foreground);
  font-size: 1.08rem;
  line-height: 1.05;
  text-shadow: 0 2px 7px color-mix(in oklab, var(--operator-surface) 30%, transparent);
}

.baseline-card__summary {
  color: var(--operator-text-muted);
  font-size: 0.88rem;
  line-height: 1.5;
}

.baseline-card__note {
  margin-top: 0.12rem;
  padding-left: 0.52rem;
  border-left: 2px solid var(--operator-line-subtle);
  color: var(--operator-text-soft);
  font-size: 0.8rem;
  line-height: 1.45;
}

.baseline-chart {
  height: 19rem;
  min-height: 19rem;
  border: 1px solid var(--operator-control-button-border);
  border-radius: 0.72rem;
  background:
    linear-gradient(
      180deg,
      color-mix(in oklab, var(--canvas-top) 94%, var(--accent-cyan) 6%),
      color-mix(in oklab, var(--canvas-base) 88%, var(--accent-cyan) 12%)
    );
  box-shadow: inset 0 1px 0 color-mix(in oklab, var(--panel-strong) 82%, transparent);
}

.baseline-chart-fallback {
  display: flex;
  align-items: center;
  justify-content: center;
  border: 2px dashed color-mix(in oklab, var(--accent-cyan) 18%, transparent);
  border-radius: 1.2rem;
  color: var(--ink-soft);
}

@media (min-width: 860px) {
  .baseline-slab__grid {
    grid-template-columns: minmax(0, 1.1fr) minmax(0, 1fr);
  }
}
</style>
