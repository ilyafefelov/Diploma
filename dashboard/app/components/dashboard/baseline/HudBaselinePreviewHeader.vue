<script setup lang="ts">
import type { BaselineBoundaryItem, BaselineCompactItem } from '~/types/hud-baseline-preview'

defineProps<{
  isExpanded: boolean
  lastLoadedLabel: string
  selectedStrategyLabel: string
  selectedStrategyValueLabel: string
  baselineBoundaryItems: BaselineBoundaryItem[]
  compactPreviewItems: BaselineCompactItem[]
}>()

const emit = defineEmits<{
  (event: 'toggle'): void
}>()
</script>

<template>
  <div class="baseline-slab__header">
    <div>
      <p class="baseline-slab__eyebrow">
        Baseline comparator preview
      </p>
      <h3 class="baseline-slab__title">
        Baseline LP comparator surface
      </h3>
      <p class="baseline-slab__summary">
        This panel is the deterministic <strong>baseline LP</strong> comparator. It is kept so the operator can compare
        the selected strategy preview above against a simpler hourly LP plan. Its UAH cards are baseline-only economics,
        so they can differ from the top ribbon.
      </p>
    </div>

    <div class="baseline-slab__meta-block">
      <p class="baseline-slab__meta">
        Updated {{ lastLoadedLabel }}
      </p>
      <p class="baseline-slab__meta baseline-slab__meta-soft">
        Comparator preview only, not selected strategy
      </p>
      <UButton
        class="baseline-slab__toggle"
        :icon="isExpanded ? 'i-lucide-chevron-up' : 'i-lucide-chevron-down'"
        :label="isExpanded ? 'Collapse baseline' : 'Expand baseline'"
        color="info"
        variant="soft"
        size="xs"
        @click="emit('toggle')"
      />
    </div>
  </div>

  <div class="baseline-comparison-strip">
    <article class="baseline-comparison-pill baseline-comparison-pill-active">
      <span>This panel</span>
      <strong>Baseline LP comparator</strong>
      <small>Simple forecast + LP feasible schedule.</small>
    </article>
    <article class="baseline-comparison-pill">
      <span>Top ribbon / schedule dock</span>
      <strong>{{ selectedStrategyLabel }}</strong>
      <small>{{ selectedStrategyValueLabel }} selected-strategy preview.</small>
    </article>
    <article class="baseline-comparison-pill">
      <span>Why numbers differ</span>
      <strong>Different strategy, same tenant</strong>
      <small>Baseline cards score the LP comparator; top cards score the selected strategy preview.</small>
    </article>
  </div>

  <div
    class="baseline-boundary-strip"
    aria-label="Baseline market preview boundary"
  >
    <span
      v-for="item in baselineBoundaryItems"
      :key="item.label"
      class="baseline-boundary-pill"
    >
      <strong>{{ item.label }}</strong>
      {{ item.value }}
    </span>
  </div>

  <div
    v-if="!isExpanded"
    class="baseline-slab__compact-preview"
  >
    <article
      v-for="item in compactPreviewItems"
      :key="`compact-${item.label}`"
      class="baseline-compact-pill"
    >
      <span>{{ item.label }}</span>
      <strong>{{ item.value }}</strong>
    </article>
    <p class="baseline-slab__compact-copy">
      Compact view keeps baseline value, throughput, and comparator role visible. Open full baseline to inspect forecast
      shape, feasible SOC path, and method details.
    </p>
  </div>
</template>

<style scoped>
.baseline-slab__header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 0.85rem;
}

.baseline-slab__eyebrow {
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--operator-accent-readable);
}

.baseline-slab__title {
  margin-top: 0.35rem;
  color: var(--operator-control-foreground);
  font-size: 1.25rem;
  line-height: 1.05;
  text-shadow: 0 2px 7px color-mix(in oklab, var(--operator-surface) 30%, transparent);
}

.baseline-slab__summary {
  max-width: 58rem;
  margin-top: 0.38rem;
  color: var(--operator-text-body);
  font-size: 0.86rem;
  font-weight: 700;
  line-height: 1.5;
}

.baseline-slab__meta-block {
  display: grid;
  gap: 0.15rem;
  text-align: right;
}

.baseline-slab__meta {
  font-size: 0.84rem;
  color: var(--operator-text-bright-muted);
}

.baseline-slab__meta-soft {
  color: var(--operator-text-muted);
}

.baseline-slab__toggle {
  justify-self: end;
}

.baseline-slab__compact-preview,
.baseline-comparison-strip,
.baseline-boundary-strip {
  display: grid;
  gap: 0.9rem;
}

.baseline-comparison-strip {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.baseline-boundary-strip {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.baseline-comparison-pill {
  display: grid;
  min-width: 0;
  min-height: 5.4rem;
  gap: 0.24rem;
  padding: 0.64rem 0.72rem;
  border: 1px solid var(--operator-line-muted);
  border-radius: 0.72rem;
  background:
    radial-gradient(circle at top right, var(--operator-card-accent-wash), transparent 30%),
    linear-gradient(180deg, var(--operator-card-gradient-top), var(--operator-card-gradient-bottom));
}

.baseline-comparison-pill-active {
  border-color: var(--operator-accent-muted);
  background:
    radial-gradient(circle at top right, var(--operator-accent-glow), transparent 32%),
    linear-gradient(180deg, var(--operator-topbar-gradient-top), var(--operator-topbar-gradient-bottom));
}

.baseline-comparison-pill span,
.baseline-compact-pill span,
.baseline-boundary-pill strong {
  color: var(--operator-accent-readable);
  font-size: 0.66rem;
  font-weight: 900;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.baseline-comparison-pill strong {
  color: var(--operator-control-foreground);
  font-size: 0.98rem;
  font-weight: 900;
  line-height: 1.15;
}

.baseline-comparison-pill small {
  color: var(--operator-text-muted);
  font-size: 0.72rem;
  font-weight: 750;
  line-height: 1.32;
}

.baseline-boundary-pill {
  display: grid;
  min-width: 0;
  gap: 0.16rem;
  padding: 0.5rem 0.58rem;
  border: 1px solid var(--operator-line-dim);
  border-radius: 0.62rem;
  background: var(--operator-surface-muted);
  color: var(--operator-text-body);
  font-size: 0.78rem;
  font-weight: 750;
  line-height: 1.3;
}

.baseline-boundary-pill strong {
  font-size: 0.64rem;
  letter-spacing: 0.11em;
}

.baseline-slab__compact-preview {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.baseline-compact-pill {
  display: grid;
  min-width: 0;
  gap: 0.2rem;
  padding: 0.58rem 0.68rem;
  border: 1px solid var(--operator-line-subtle);
  border-radius: 0.7rem;
  background:
    radial-gradient(circle at top right, var(--operator-accent-wash), transparent 34%),
    linear-gradient(180deg, var(--operator-card-gradient-top), var(--operator-card-gradient-bottom));
}

.baseline-compact-pill strong {
  color: var(--operator-positive);
  font-size: 1rem;
  font-weight: 900;
}

.baseline-slab__compact-copy {
  grid-column: 1 / -1;
  margin: 0;
  color: var(--operator-text-body);
  font-size: 0.8rem;
  font-weight: 700;
  line-height: 1.45;
}

@media (max-width: 859px) {
  .baseline-slab__header {
    display: grid;
  }

  .baseline-slab__meta-block {
    text-align: left;
  }

  .baseline-slab__toggle {
    justify-self: start;
  }

  .baseline-slab__compact-preview,
  .baseline-comparison-strip,
  .baseline-boundary-strip {
    grid-template-columns: 1fr;
  }
}
</style>
