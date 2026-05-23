<script setup lang="ts">
import { computed } from 'vue'
import type { OperatorMoodChip } from '~/types/operator-dashboard'

const props = defineProps<{
  chips: OperatorMoodChip[]
  activeAlertCount: number
}>()

const postureLabel = computed(() => props.activeAlertCount > 0 ? 'Gaps' : 'Review')
</script>

<template>
  <section class="surface-panel mood-panel">
    <div class="rail-heading">
      <div>
        <p class="eyebrow">
          Preview posture
        </p>
        <h2 class="rail-title">
          {{ postureLabel }}
        </h2>
      </div>
      <UIcon
        class="rail-heading__icon"
        name="i-lucide-smile"
      />
    </div>

    <div class="mood-meter">
      <span />
    </div>

    <div class="mood-chip-grid">
      <article
        v-for="chip in chips"
        :key="chip.label"
        class="mood-chip"
        :class="`mood-chip--${chip.tone}`"
      >
        <span>{{ chip.label }}</span>
        <strong>{{ chip.value }}</strong>
      </article>
    </div>
  </section>
</template>
