<script setup lang="ts">
import CollapsibleTextCard from '~/components/dashboard/CollapsibleTextCard.vue'
import HudMotiveBars from '~/components/dashboard/HudMotiveBars.vue'
import type { OperatorMotiveItem } from '~/types/operator-dashboard'

defineProps<{
  motiveItems: OperatorMotiveItem[]
  primaryBoundaryCopy: string
  nextStepsItems: string[]
  selectedRunConfigSnippet: string
}>()
</script>

<template>
  <section class="surface-panel notes-panel">
    <div class="rail-heading">
      <div>
        <p class="eyebrow">
          Control cues
        </p>
        <h2 class="rail-title">
          Motives
        </h2>
      </div>
    </div>

    <HudMotiveBars :items="motiveItems" />

    <div class="narrative-stack">
      <CollapsibleTextCard
        title="Primary boundary"
        eyebrow="System boundary"
        tone="blue"
      >
        <p class="note-block__copy">
          {{ primaryBoundaryCopy }}
        </p>
      </CollapsibleTextCard>

      <CollapsibleTextCard
        title="What comes next"
        eyebrow="Roadmap cue"
        tone="green"
      >
        <ol class="note-list">
          <li
            v-for="item in nextStepsItems"
            :key="item"
          >
            {{ item }}
          </li>
        </ol>
      </CollapsibleTextCard>

      <CollapsibleTextCard
        title="Prepared config"
        eyebrow="Runtime payload"
        tone="orange"
      >
        <pre class="config-preview">{{ selectedRunConfigSnippet }}</pre>
      </CollapsibleTextCard>
    </div>
  </section>
</template>
