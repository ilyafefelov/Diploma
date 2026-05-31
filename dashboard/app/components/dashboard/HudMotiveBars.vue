<script setup lang="ts">
const props = defineProps<{
  items: Array<{
    label: string
    value: number
    tone: 'blue' | 'green' | 'orange'
    hint: string
  }>
}>()

const toneIcon: Record<string, string> = {
  blue: 'i-lucide-radar',
  green: 'i-lucide-leaf',
  orange: 'i-lucide-triangle-alert'
}
</script>

<template>
  <div class="motive-stack">
    <article
      v-for="item in props.items"
      :key="item.label"
      class="motive-row"
      :class="`motive-row--${item.tone}`"
      role="group"
      :aria-label="`${item.label}: ${item.value}% score. ${item.hint}`"
      tabindex="0"
    >
      <div class="motive-row__topline">
        <div class="motive-row__topline-meta">
          <span class="motive-row__label">{{ item.label }}</span>
          <UIcon
            class="motive-row__icon"
            :name="toneIcon[item.tone]"
          />
        </div>
        <strong class="motive-row__value">{{ item.value }}%</strong>
      </div>

      <div class="motive-track">
        <div
          class="motive-fill"
          :class="`motive-fill--${item.tone}`"
          :style="{ width: `${item.value}%` }"
        />
      </div>

      <p class="motive-row__hint">
        {{ item.hint }}
      </p>
      <span
        class="motive-row__tooltip"
        role="tooltip"
      >
        <span class="motive-row__tooltip-title">{{ item.label }}</span>
        <span class="motive-row__tooltip-body">{{ item.hint }}</span>
        <span class="motive-row__tooltip-formula">
          Formula: score = min(100, offset + dynamic factor) from dashboard inputs and status.
        </span>
      </span>
    </article>
  </div>
</template>

<style scoped>
.motive-stack {
  display: grid;
  gap: 0.95rem;
}

.motive-row {
  display: grid;
  gap: 0.45rem;
  padding: 0.2rem 0;
  position: relative;
  border-radius: 0.75rem;
  transition: background-color 140ms ease, transform 140ms ease, box-shadow 140ms ease;
}

.motive-row:hover,
.motive-row:focus-visible {
  transform: translateY(-2px);
  background: color-mix(in oklab, var(--panel-strong) 22%, transparent);
  box-shadow:
    0 16px 26px color-mix(in oklab, var(--operator-surface) 22%, transparent),
    inset 0 1px 0 var(--operator-card-border);
}

.motive-row:focus-visible {
  outline: 2px solid var(--focus-ring);
  outline-offset: 0.22rem;
}

.motive-row__topline {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
}

.motive-row__topline-meta {
  display: inline-flex;
  align-items: center;
  gap: 0.48rem;
}

.motive-row__label,
.motive-row__hint {
  color: var(--ink-soft);
}

.motive-row__label {
  font-size: 0.74rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
}

.motive-row__icon {
  width: 1rem;
  height: 1rem;
  opacity: 0.92;
}

.motive-row__value {
  color: var(--ink-strong);
  font-size: 0.95rem;
}

.motive-track {
  overflow: hidden;
  height: 0.9rem;
  border-radius: 999px;
  background: var(--panel-base);
  border: 2px solid var(--panel-strong);
  box-shadow: inset 0 1px 4px color-mix(in oklab, var(--ink-strong) 6%, transparent);
}

.motive-fill {
  height: 100%;
  border-radius: 999px;
  position: relative;
  overflow: hidden;
  transition: width 420ms ease, filter 180ms ease;
}

.motive-fill::after {
  content: '';
  position: absolute;
  inset: 0;
  background: linear-gradient(
    90deg,
    color-mix(in oklab, var(--panel-strong) 8%, transparent),
    color-mix(in oklab, var(--panel-strong) 42%, transparent),
    color-mix(in oklab, var(--panel-strong) 8%, transparent)
  );
  transform: translateX(-100%);
  animation: motive-sheen 3.8s ease-in-out infinite;
}

.motive-fill--blue {
  background: linear-gradient(90deg, var(--operator-rail-mini-meter-blue-top) 0%, var(--sims-blue) 100%);
}

.motive-fill--green {
  background: linear-gradient(90deg, var(--operator-rail-mini-meter-green-top) 0%, var(--plumbob-green) 100%);
}

.motive-fill--orange {
  background: linear-gradient(90deg, color-mix(in oklab, var(--accent-berry) 58%, var(--panel-strong)) 0%, var(--accent-berry) 100%);
}

.motive-row__hint {
  font-size: 0.88rem;
  line-height: 1.5;
}

.motive-row__tooltip {
  position: absolute;
  right: 0.08rem;
  left: 0.08rem;
  bottom: calc(100% + 0.44rem);
  z-index: 12;
  display: grid;
  gap: 0.22rem;
  padding: 0.55rem 0.62rem;
  border: 1px solid var(--operator-tooltip-border);
  border-radius: 0.72rem;
  background:
    radial-gradient(circle at top right, var(--operator-card-accent-wash), transparent 36%),
    linear-gradient(180deg, var(--operator-tooltip-gradient-top), var(--operator-tooltip-gradient-bottom));
  color: var(--operator-rail-tooltip-text);
  font-size: 0.68rem;
  line-height: 1.35;
  opacity: 0;
  pointer-events: none;
  transform: translateY(0.2rem) scale(0.97);
  transition: opacity 140ms ease, transform 140ms ease;
}

.motive-row:hover .motive-row__tooltip,
.motive-row:focus-visible .motive-row__tooltip {
  opacity: 1;
  transform: translateY(0) scale(1);
}

.motive-row__tooltip-title {
  font-size: 0.66rem;
  font-weight: 900;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: var(--operator-accent-soft);
}

.motive-row__tooltip-body,
.motive-row__tooltip-formula {
  color: var(--operator-rail-tooltip-text-muted);
}

.motive-row__tooltip-formula {
  color: var(--sims-blue-hover);
  font-weight: 700;
}

@keyframes motive-sheen {
  0%, 100% {
    transform: translateX(-100%);
  }

  45%, 55% {
    transform: translateX(100%);
  }
}

@media (prefers-reduced-motion: reduce) {
  .motive-row,
  .motive-fill,
  .motive-row__tooltip {
    transition: none;
  }

  .motive-fill::after {
    animation: none;
    transform: none;
  }
}
</style>
