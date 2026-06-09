<script setup lang="ts">
const props = withDefaults(defineProps<{
  title: string
  eyebrow?: string
  tone?: 'default' | 'accent' | 'rose' | 'blue' | 'green' | 'orange'
  open?: boolean
}>(), {
  eyebrow: '',
  tone: 'default',
  open: false
})
</script>

<template>
  <details
    class="collapsible-card"
    :class="`collapsible-card-${props.tone}`"
    :open="props.open"
  >
    <summary class="collapsible-card__summary">
      <div>
        <p
          v-if="props.eyebrow"
          class="collapsible-card__eyebrow"
        >
          {{ props.eyebrow }}
        </p>
        <p class="collapsible-card__title">
          {{ props.title }}
        </p>
      </div>
      <span
        class="collapsible-card__chevron"
        aria-hidden="true"
      />
    </summary>

    <div class="collapsible-card__body">
      <slot />
    </div>
  </details>
</template>

<style scoped>
.collapsible-card {
  display: grid;
  gap: 0;
  padding: 0.72rem 0.85rem;
  overflow: hidden;
  border: 1px solid var(--operator-line-subtle);
  border-radius: 0.75rem;
  background:
    radial-gradient(circle at top right, var(--operator-card-accent-wash), transparent 30%),
    linear-gradient(180deg, var(--operator-card-gradient-top), var(--operator-card-gradient-bottom));
  box-shadow:
    inset 0 1px 0 var(--operator-card-border),
    0 10px 20px color-mix(in oklab, var(--operator-surface) 16%, transparent);
  transition: background 160ms ease, border-color 160ms ease, box-shadow 160ms ease;
}

.collapsible-card[open] {
  gap: 0.62rem;
  background:
    radial-gradient(circle at top right, color-mix(in oklab, var(--plumbob-green) 16%, transparent), transparent 30%),
    linear-gradient(
      180deg,
      color-mix(in oklab, var(--canvas-top) 96%, var(--accent-cyan) 4%),
      color-mix(in oklab, var(--canvas-base) 88%, var(--accent-cyan) 12%)
    );
  border-color: var(--panel-strong);
}

.collapsible-card-accent {
  border-color: color-mix(in oklab, var(--plumbob-green) 28%, transparent);
}

.collapsible-card-rose {
  border-color: color-mix(in oklab, var(--accent-berry) 28%, transparent);
}

.collapsible-card-blue {
  border-color: color-mix(in oklab, var(--accent-cyan) 34%, transparent);
}

.collapsible-card-green {
  border-color: color-mix(in oklab, var(--plumbob-green) 34%, transparent);
}

.collapsible-card-orange {
  border-color: var(--operator-warning-border-muted);
}

.collapsible-card__summary {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  cursor: pointer;
  list-style: none;
}

.collapsible-card__summary::-webkit-details-marker {
  display: none;
}

.collapsible-card__eyebrow {
  font-size: 0.62rem;
  font-weight: 800;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: var(--operator-text-muted);
}

.collapsible-card[open] .collapsible-card__eyebrow {
  color: var(--ink-soft);
}

.collapsible-card__title {
  margin-top: 0.08rem;
  font-size: 0.82rem;
  font-weight: 800;
  color: var(--operator-control-foreground);
  line-height: 1.2;
}

.collapsible-card[open] .collapsible-card__title {
  color: var(--ink-strong);
}

.collapsible-card__chevron {
  width: 0.72rem;
  height: 0.72rem;
  border-right: 2px solid var(--operator-accent);
  border-bottom: 2px solid var(--operator-accent);
  transform: rotate(45deg);
  transition: transform 160ms ease;
  flex: 0 0 auto;
}

.collapsible-card[open] .collapsible-card__chevron {
  transform: rotate(225deg);
}

.collapsible-card__body {
  display: grid;
  gap: 0.45rem;
  padding-top: 0.12rem;
}

.collapsible-card:not([open]) .collapsible-card__body {
  display: none;
}
</style>
