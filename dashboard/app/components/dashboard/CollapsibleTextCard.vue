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
  border: 1px solid rgba(134, 219, 255, 0.34);
  border-radius: 0.75rem;
  background:
    radial-gradient(circle at top right, rgba(126, 211, 33, 0.13), transparent 30%),
    linear-gradient(180deg, rgba(12, 128, 199, 0.58), rgba(3, 74, 137, 0.56));
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.28),
    0 10px 20px rgba(0, 42, 82, 0.14);
  transition: background 160ms ease, border-color 160ms ease, box-shadow 160ms ease;
}

.collapsible-card[open] {
  gap: 0.62rem;
  background:
    radial-gradient(circle at top right, rgba(126, 211, 33, 0.16), transparent 30%),
    linear-gradient(180deg, rgba(232, 248, 255, 0.96), rgba(197, 234, 255, 0.92));
  border-color: rgba(255, 255, 255, 0.74);
}

.collapsible-card-accent {
  border-color: rgba(126, 211, 33, 0.24);
}

.collapsible-card-rose {
  border-color: rgba(255, 111, 174, 0.24);
}

.collapsible-card-blue {
  border-color: rgba(83, 178, 234, 0.34);
}

.collapsible-card-green {
  border-color: rgba(126, 211, 33, 0.34);
}

.collapsible-card-orange {
  border-color: rgba(245, 166, 35, 0.34);
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
  color: rgba(219, 246, 255, 0.76);
}

.collapsible-card[open] .collapsible-card__eyebrow {
  color: var(--ink-soft);
}

.collapsible-card__title {
  margin-top: 0.08rem;
  font-size: 0.82rem;
  font-weight: 800;
  color: white;
  line-height: 1.2;
}

.collapsible-card[open] .collapsible-card__title {
  color: var(--ink-strong);
}

.collapsible-card__chevron {
  width: 0.72rem;
  height: 0.72rem;
  border-right: 2px solid rgba(215, 255, 79, 0.92);
  border-bottom: 2px solid rgba(215, 255, 79, 0.92);
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
