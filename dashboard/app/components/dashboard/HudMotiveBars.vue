<script setup lang="ts">
const props = defineProps<{
  items: Array<{
    label: string
    value: number
    tone: 'blue' | 'green' | 'orange'
    hint: string
  }>
}>()
</script>

<template>
  <div class="motive-stack">
    <article v-for="item in props.items" :key="item.label" class="motive-row">
      <div class="motive-row__topline">
        <span class="motive-row__label">{{ item.label }}</span>
        <strong class="motive-row__value">{{ item.value }}%</strong>
      </div>

      <div class="motive-track">
        <div class="motive-fill" :class="`motive-fill--${item.tone}`" :style="{ width: `${item.value}%` }"></div>
      </div>

      <p class="motive-row__hint">{{ item.hint }}</p>
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
}

.motive-row__topline {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
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

.motive-row__value {
  color: var(--ink-strong);
  font-size: 0.95rem;
}

.motive-track {
  overflow: hidden;
  height: 0.9rem;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.7);
  border: 2px solid rgba(255, 255, 255, 0.85);
  box-shadow: inset 0 1px 4px rgba(0, 0, 0, 0.05);
}

.motive-fill {
  height: 100%;
  border-radius: 999px;
  transition: width 420ms ease;
}

.motive-fill--blue {
  background: linear-gradient(90deg, #52b8ff 0%, var(--sims-blue) 100%);
}

.motive-fill--green {
  background: linear-gradient(90deg, #b6ff6d 0%, var(--plumbob-green) 100%);
}

.motive-fill--orange {
  background: linear-gradient(90deg, #ffd67d 0%, var(--accent-amber) 100%);
}

.motive-row__hint {
  font-size: 0.88rem;
  line-height: 1.5;
}
</style>