<script setup lang="ts">
import CollapsibleTextCard from '~/components/dashboard/CollapsibleTextCard.vue'

const props = defineProps<{
  explanationMode: 'mvp' | 'future'
}>()
</script>

<template>
  <CollapsibleTextCard
    class="baseline-boundary"
    title="Planning boundary"
    eyebrow="Operator boundary"
    tone="default"
  >
    <template v-if="props.explanationMode === 'mvp'">
      <p class="baseline-boundary__copy">
        This surface shows a feasible hourly recommendation derived from the baseline LP comparator and constrained
        battery state. It is useful for comparison with the selected strategy preview, but it is not the chosen schedule
        unless the user explicitly selects the baseline strategy.
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
</template>

<style scoped>
.baseline-boundary {
  display: grid;
  gap: 0.35rem;
  padding: 0.72rem;
  border: 1px solid var(--operator-card-border);
  border-radius: 0.72rem;
  background:
    radial-gradient(circle at top right, var(--operator-card-accent-wash), transparent 28%),
    linear-gradient(180deg, var(--operator-card-gradient-top), var(--operator-card-gradient-bottom));
  transition: transform 180ms ease, box-shadow 180ms ease, border-color 180ms ease;
}

.baseline-boundary:hover {
  transform: translateY(-2px);
  border-color: color-mix(in oklab, var(--panel-strong) 52%, transparent);
  box-shadow:
    0 16px 30px color-mix(in oklab, var(--operator-surface) 22%, transparent),
    inset 0 1px 0 var(--operator-card-border);
}

.baseline-boundary__copy {
  color: var(--ink-strong);
  font-weight: 650;
  line-height: 1.65;
}

.baseline-boundary__copy-strong {
  color: color-mix(in oklab, var(--ink-strong) 92%, var(--accent-cyan-strong));
  font-weight: 850;
}
</style>
