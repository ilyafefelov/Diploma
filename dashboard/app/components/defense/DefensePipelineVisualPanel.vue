<script setup lang="ts">
defineProps<{
  infographicUrl: string
}>()
</script>

<template>
  <section class="pipeline-visual-panel">
    <div class="pipeline-visual-copy">
      <p class="eyebrow">
        V2+ pipeline visual
      </p>
      <h2>How the offline result is produced</h2>
      <p class="section-explainer">
        Ukrainian DAM history, weather/load context, official global-panel NBEATSx forecasts, candidate schedules,
        and strict LP/oracle scoring are joined into a read-model evidence path. The image is illustrative; the metric
        cards and charts below remain the deterministic source of numeric evidence.
      </p>
      <div class="pipeline-stat-strip">
        <article>
          <span>Input boundary</span>
          <strong>Ukraine only</strong>
          <small>OREE DAM, Open-Meteo/weather, tenant context</small>
        </article>
        <article>
          <span>Decision target</span>
          <strong>Regret</strong>
          <small>oracle_value_uah - decision_value_uah</small>
        </article>
        <article>
          <span>Execution claim</span>
          <strong>false</strong>
          <small>market_execution_enabled</small>
        </article>
      </div>
    </div>
    <figure class="pipeline-figure">
      <img
        :src="infographicUrl"
        alt="V2+ offline schedule value pipeline infographic"
      >
      <figcaption>
        Generated dashboard visual: V2+ pipeline from Ukrainian source-backed features to strict LP/oracle evidence.
      </figcaption>
    </figure>
  </section>
</template>
