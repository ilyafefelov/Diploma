<script setup lang="ts">
import type { BaselineFeasiblePlanItem, BaselineMetricTooltipItem } from '~/types/hud-baseline-preview'

defineProps<{
  economicsItems: BaselineMetricTooltipItem[]
  feasiblePlanItems: BaselineFeasiblePlanItem[]
}>()
</script>

<template>
  <div class="baseline-slab__economics">
    <article
      v-for="item in economicsItems"
      :key="item.label"
      class="economics-pill economics-pill-interactive"
      role="group"
      :aria-label="`Baseline economic metric ${item.label}: ${item.value}`"
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
      role="group"
      :aria-label="`Baseline feasible-plan metric ${item.label}: ${item.value}`"
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
</template>

<style scoped>
.baseline-slab__economics,
.baseline-feasible-strip {
  display: grid;
  gap: 0.9rem;
}

.economics-pill,
.feasible-pill {
  position: relative;
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

.feasible-pill {
  align-content: start;
}

.economics-pill-interactive,
.feasible-pill-interactive {
  cursor: help;
  isolation: isolate;
}

.economics-pill:hover,
.feasible-pill:hover {
  transform: translateY(-2px);
  border-color: color-mix(in oklab, var(--panel-strong) 52%, transparent);
  box-shadow:
    0 16px 30px color-mix(in oklab, var(--operator-surface) 22%, transparent),
    inset 0 1px 0 var(--operator-card-border);
}

.economics-pill-interactive:focus-visible,
.feasible-pill-interactive:focus-visible {
  outline: 2px solid var(--focus-ring);
  outline-offset: 3px;
  border-color: color-mix(in oklab, var(--accent-cyan) 28%, transparent);
  box-shadow: 0 0 0 4px color-mix(in oklab, var(--accent-cyan) 18%, transparent);
}

.economics-pill__label,
.feasible-pill__label {
  color: var(--operator-accent-readable);
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
}

.economics-pill__value {
  color: var(--operator-positive);
  font-size: 1.15rem;
  font-weight: 800;
}

.feasible-pill__value {
  color: var(--operator-control-foreground);
  font-size: 1rem;
  font-weight: 800;
}

.feasible-pill__note {
  color: var(--operator-text-muted);
  font-size: 0.88rem;
  line-height: 1.5;
}

.sims-tooltip {
  position: absolute;
  top: calc(100% + 0.8rem);
  left: 0;
  right: auto;
  z-index: 4;
  display: grid;
  width: min(19rem, calc(100vw - 3rem));
  gap: 0.45rem;
  padding: 1rem 1.05rem;
  overflow: hidden;
  border: 2px solid var(--panel-strong);
  border-radius: 1.3rem;
  background:
    radial-gradient(circle at top right, color-mix(in oklab, var(--plumbob-green) 18%, transparent), transparent 28%),
    radial-gradient(circle at bottom left, color-mix(in oklab, var(--accent-cyan) 14%, transparent), transparent 24%),
    linear-gradient(
      180deg,
      color-mix(in oklab, var(--panel-strong) 99%, var(--canvas-top)),
      color-mix(in oklab, var(--canvas-top) 92%, var(--canvas-base))
    );
  box-shadow:
    0 22px 48px color-mix(in oklab, var(--accent-cyan) 18%, transparent),
    inset 0 1px 0 var(--panel-strong);
  opacity: 0;
  visibility: hidden;
  transform: translateY(0.35rem) scale(0.98);
  transform-origin: top left;
  transition: opacity 160ms ease, transform 160ms ease, visibility 160ms ease;
  pointer-events: none;
}

.sims-tooltip::before {
  position: absolute;
  inset: 0;
  background: linear-gradient(
    115deg,
    transparent 24%,
    color-mix(in oklab, var(--panel-strong) 28%, transparent) 38%,
    transparent 52%
  );
  content: '';
  pointer-events: none;
  transform: translateX(-130%);
}

.sims-tooltip::after {
  position: absolute;
  bottom: calc(100% - 0.08rem);
  left: 1.2rem;
  width: 1rem;
  height: 1rem;
  border-top: 1px solid var(--panel-strong);
  border-left: 1px solid var(--panel-strong);
  background: linear-gradient(135deg, var(--panel-strong), var(--canvas-top));
  content: '';
  transform: rotate(45deg);
}

.economics-pill-interactive:hover .sims-tooltip,
.economics-pill-interactive:focus-visible .sims-tooltip,
.feasible-pill-interactive:hover .sims-tooltip,
.feasible-pill-interactive:focus-visible .sims-tooltip {
  opacity: 1;
  visibility: visible;
  transform: translateY(0) scale(1);
  animation: sims-tooltip-pop 220ms ease-out, sims-tooltip-float 2.4s ease-in-out 220ms infinite;
}

.economics-pill-interactive:hover .sims-tooltip::before,
.economics-pill-interactive:focus-visible .sims-tooltip::before,
.feasible-pill-interactive:hover .sims-tooltip::before,
.feasible-pill-interactive:focus-visible .sims-tooltip::before {
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

.sims-tooltip__topline {
  display: inline-flex;
  align-items: center;
  gap: 0.55rem;
}

.sims-tooltip__plumbob {
  width: 0.8rem;
  height: 1.15rem;
  background: linear-gradient(180deg, color-mix(in oklab, var(--plumbob-green) 46%, var(--panel-strong)) 0%, var(--plumbob-green) 100%);
  box-shadow: 0 0 16px color-mix(in oklab, var(--plumbob-green) 38%, transparent);
  clip-path: polygon(50% 0%, 100% 50%, 50% 100%, 0% 50%);
  animation: sims-tooltip-plumbob 1.8s ease-in-out infinite;
}

.sims-tooltip__eyebrow {
  color: var(--ink-soft);
  font-size: 0.68rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
}

.sims-tooltip__title {
  color: var(--ink-strong);
  font-size: 1rem;
  font-weight: 800;
  line-height: 1.25;
}

.sims-tooltip__body,
.sims-tooltip__formula {
  color: var(--ink-soft);
  font-size: 0.85rem;
  line-height: 1.55;
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
    filter: brightness(1);
    transform: translateY(0);
  }

  50% {
    filter: brightness(1.08);
    transform: translateY(-0.08rem);
  }
}

@media (min-width: 860px) {
  .baseline-slab__economics {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }

  .baseline-feasible-strip {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (prefers-reduced-motion: reduce) {
  .economics-pill,
  .feasible-pill,
  .sims-tooltip,
  .sims-tooltip::before,
  .sims-tooltip__plumbob {
    animation: none !important;
    transition: none;
  }

  .economics-pill:hover,
  .feasible-pill:hover {
    transform: none;
  }
}
</style>
