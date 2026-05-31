<script setup lang="ts">
import { CURRENT_DASHBOARD_EXPERIMENTS } from '~/utils/defenseDataset'
</script>

<template>
  <section class="latest-experiment-panel">
    <div class="section-heading">
      <div>
        <p class="eyebrow">
          Latest experiments
        </p>
        <h2>This week’s dashboard story</h2>
      </div>
      <span class="source-pill">Demo Day 2</span>
    </div>
    <div class="latest-experiment-grid">
      <article
        v-for="experiment in CURRENT_DASHBOARD_EXPERIMENTS"
        :key="experiment.label"
        :class="`latest-experiment-card latest-experiment-card--${experiment.status}`"
      >
        <span>{{ experiment.label }}</span>
        <strong>{{ experiment.value }}</strong>
        <small>{{ experiment.meta }}</small>
      </article>
    </div>
  </section>
</template>
