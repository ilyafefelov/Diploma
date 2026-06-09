<script setup lang="ts">
defineProps<{
  decisionSourceFacts: string[]
  backendStatusFacts: string[]
}>()
</script>

<template>
  <div class="future-explainer-grid">
    <article>
      <span>Forecast source</span>
      <p class="future-explainer-lead">
        Headline evidence is Ukrainian-only V2+ schedule/value evidence.
      </p>
      <div class="future-explainer-facts">
        <span>The price chart is context, not a bidding command.</span>
        <span>Unsafe out-of-cap forecast rows are filtered out from schedule inputs.</span>
        <span>Selected policy rows stay in DAM/IDM delivery-hour review mode.</span>
      </div>
    </article>
    <article>
      <span>Decision source</span>
      <p class="future-explainer-lead">
        Selected strategy preview comes from the operator recommendation endpoint and is re-checked by the same
        battery feasibility LP layer.
      </p>
      <div class="future-explainer-facts">
        <span
          v-for="fact in decisionSourceFacts"
          :key="fact"
        >{{ fact }}</span>
      </div>
    </article>
    <article>
      <span>Backend status</span>
      <p class="future-explainer-lead">
        Current read-model and runtime health signals.
      </p>
      <div class="future-explainer-facts">
        <span
          v-for="item in backendStatusFacts"
          :key="item"
        >{{ item }}</span>
      </div>
    </article>
  </div>
</template>

<style scoped>
.future-explainer-grid {
  display: grid;
  gap: 0.65rem;
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.future-explainer-grid article {
  display: grid;
  gap: 0.35rem;
  border: 1px solid var(--operator-card-border);
  border-radius: 0.72rem;
  background:
    radial-gradient(circle at top right, var(--operator-card-accent-wash), transparent 30%),
    linear-gradient(180deg, var(--operator-card-gradient-top), var(--operator-card-gradient-bottom));
  padding: 0.72rem;
}

.future-explainer-grid span {
  color: var(--operator-accent-soft);
  font-size: 0.68rem;
  font-weight: 900;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.future-explainer-grid p {
  color: var(--operator-text-soft);
  font-size: 0.78rem;
  font-weight: 720;
  line-height: 1.42;
}

.future-explainer-lead {
  margin: 0;
}

.future-explainer-facts {
  display: flex;
  flex-wrap: wrap;
  gap: 0.36rem;
}

.future-explainer-facts span {
  display: inline-flex;
  align-items: center;
  border: 1px solid var(--operator-line-muted);
  border-radius: 999px;
  background: var(--operator-chip-surface-soft);
  color: var(--operator-text-bright);
  padding: 0.2rem 0.45rem;
  font-size: 0.67rem;
  font-weight: 820;
  letter-spacing: 0;
  line-height: 1.35;
  text-transform: none;
}

@media (max-width: 1320px) {
  .future-explainer-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .future-explainer-grid {
    grid-template-columns: 1fr;
  }
}
</style>
