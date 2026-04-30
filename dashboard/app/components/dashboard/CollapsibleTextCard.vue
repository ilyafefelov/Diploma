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
  <details class="collapsible-card" :class="`collapsible-card-${props.tone}`" :open="props.open">
    <summary class="collapsible-card__summary">
      <div>
        <p v-if="props.eyebrow" class="collapsible-card__eyebrow">{{ props.eyebrow }}</p>
        <p class="collapsible-card__title">{{ props.title }}</p>
      </div>
      <span class="collapsible-card__chevron" aria-hidden="true"></span>
    </summary>

    <div class="collapsible-card__body">
      <slot />
    </div>
  </details>
</template>

<style scoped>
.collapsible-card {
  display: grid;
  gap: 0.55rem;
  padding: 0.9rem;
  border-radius: 1.2rem;
  background: rgba(255, 255, 255, 0.76);
  border: 1px solid rgba(0, 121, 193, 0.12);
}

.collapsible-card-accent {
  background: rgba(126, 211, 33, 0.08);
  border-color: rgba(126, 211, 33, 0.24);
}

.collapsible-card-rose {
  background: rgba(255, 111, 174, 0.08);
  border-color: rgba(255, 111, 174, 0.24);
}

.collapsible-card-blue {
  background: linear-gradient(180deg, rgba(0, 121, 193, 0.09), rgba(255, 255, 255, 0.8));
}

.collapsible-card-green {
  background: linear-gradient(180deg, rgba(126, 211, 33, 0.11), rgba(255, 255, 255, 0.82));
}

.collapsible-card-orange {
  background: linear-gradient(180deg, rgba(83, 178, 234, 0.12), rgba(255, 255, 255, 0.84));
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
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.collapsible-card__title {
  margin-top: 0.15rem;
  font-size: 0.95rem;
  font-weight: 800;
  color: var(--ink-strong);
}

.collapsible-card__chevron {
  width: 0.85rem;
  height: 0.85rem;
  border-right: 2px solid var(--ink-soft);
  border-bottom: 2px solid var(--ink-soft);
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
}
</style>