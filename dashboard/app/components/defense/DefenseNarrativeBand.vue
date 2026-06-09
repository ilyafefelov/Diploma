<script setup lang="ts">
import type { DefenseNarrativeStep } from '~/types/defense-page'

defineProps<{
  steps: DefenseNarrativeStep[]
}>()
</script>

<template>
  <section
    class="narrative-band"
    aria-label="Defense narrative"
  >
    <article
      v-for="step in steps"
      :key="step.label"
      class="narrative-step"
    >
      <span>{{ step.label }}</span>
      <p>{{ step.text }}</p>
    </article>
  </section>
</template>
