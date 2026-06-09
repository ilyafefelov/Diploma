<script setup lang="ts">
import type {
  AcademicMvpGatePassportItem,
  StrategyReadinessItem,
  V13ReadinessItem
} from '~/utils/operatorFutureStack'
import type { OperatorPreviewSourceId } from '~/utils/operatorShadowPreview'

interface FutureStatusCard {
  label: string
  value: string
  meta: string
}

defineProps<{
  selectedPreviewSourceId: OperatorPreviewSourceId
  shadowPreviewLabel: string
  shadowPreviewStatus: string | null | undefined
  statusCards: FutureStatusCard[]
  strategyReadinessItems: StrategyReadinessItem[]
  v13ReadinessItems: V13ReadinessItem[]
  academicMvpGatePassportItems: AcademicMvpGatePassportItem[]
}>()
</script>

<template>
  <div
    v-if="selectedPreviewSourceId !== 'best_valid'"
    class="shadow-preview-boundary-strip"
  >
    <span>Manual preview: {{ shadowPreviewLabel }}</span>
    <span>{{ shadowPreviewStatus || 'loading shadow packet' }}</span>
    <span>Not promoted</span>
    <span>No market execution</span>
    <span>V2+ remains comparator/fallback</span>
  </div>

  <div class="future-status-grid">
    <article
      v-for="card in statusCards"
      :key="card.label"
      class="future-status-card"
    >
      <span>{{ card.label }}</span>
      <strong>{{ card.value }}</strong>
      <small>{{ card.meta }}</small>
    </article>
  </div>

  <div
    v-if="strategyReadinessItems.length"
    class="strategy-readiness-strip"
  >
    <article
      v-for="item in strategyReadinessItems"
      :key="item.strategyId"
      :class="{ 'strategy-readiness-strip__item--blocked': item.status === 'blocked' }"
    >
      <span>{{ item.label }}</span>
      <strong>{{ item.status }}</strong>
      <small>{{ item.reason }}</small>
    </article>
  </div>

  <div class="v13-readiness-strip">
    <article
      v-for="item in v13ReadinessItems"
      :key="item.label"
      :class="{ 'v13-readiness-strip__item--blocked': item.status === 'blocked' }"
    >
      <span>{{ item.label }}</span>
      <strong>{{ item.value }}</strong>
      <small>{{ item.reason }}</small>
    </article>
  </div>

  <div class="academic-mvp-gate-strip">
    <article
      v-for="item in academicMvpGatePassportItems"
      :key="item.label"
      :class="{ 'academic-mvp-gate-strip__item--blocked': item.status === 'blocked' }"
    >
      <span>{{ item.label }}</span>
      <strong>{{ item.value }}</strong>
      <small>{{ item.reason }}</small>
    </article>
  </div>
</template>

<style scoped>
.future-status-grid,
.strategy-readiness-strip,
.v13-readiness-strip,
.academic-mvp-gate-strip {
  display: grid;
  gap: 0.65rem;
}

.future-status-grid,
.v13-readiness-strip,
.academic-mvp-gate-strip {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.strategy-readiness-strip {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.shadow-preview-boundary-strip {
  display: flex;
  flex-wrap: wrap;
  gap: 0.38rem;
}

.shadow-preview-boundary-strip span {
  border: 1px solid var(--operator-warning-border-muted);
  border-radius: 999px;
  background: var(--operator-warning-surface-muted);
  color: var(--operator-warning);
  padding: 0.22rem 0.5rem;
  font-size: 0.68rem;
  font-weight: 900;
}

.future-status-card,
.strategy-readiness-strip article,
.v13-readiness-strip article,
.academic-mvp-gate-strip article {
  border: 1px solid var(--operator-card-border);
  border-radius: 0.72rem;
  background:
    radial-gradient(circle at top right, var(--operator-card-accent-wash), transparent 30%),
    linear-gradient(180deg, var(--operator-card-gradient-top), var(--operator-card-gradient-bottom));
  padding: 0.72rem;
}

.future-status-card,
.strategy-readiness-strip article,
.v13-readiness-strip article,
.academic-mvp-gate-strip article {
  display: grid;
  gap: 0.18rem;
  min-width: 0;
}

.future-status-card {
  gap: 0.28rem;
}

.strategy-readiness-strip__item--blocked,
.v13-readiness-strip__item--blocked,
.academic-mvp-gate-strip__item--blocked {
  border-color: var(--operator-warning-border-strong) !important;
  background:
    radial-gradient(circle at top right, var(--operator-warning-glow), transparent 30%),
    linear-gradient(180deg, var(--operator-warning-gradient-top), var(--operator-warning-surface)) !important;
}

.future-status-card span,
.strategy-readiness-strip span,
.v13-readiness-strip span,
.academic-mvp-gate-strip span {
  color: var(--operator-accent-soft);
  font-weight: 900;
  text-transform: uppercase;
}

.future-status-card span,
.strategy-readiness-strip span {
  font-size: 0.68rem;
  letter-spacing: 0.12em;
}

.v13-readiness-strip span {
  font-size: 0.68rem;
  letter-spacing: 0;
}

.academic-mvp-gate-strip span {
  font-size: 0.62rem;
  letter-spacing: 0;
}

.future-status-card strong {
  overflow-wrap: anywhere;
  color: var(--operator-positive);
  font-size: 1.06rem;
  line-height: 1.08;
}

.strategy-readiness-strip strong,
.v13-readiness-strip strong,
.academic-mvp-gate-strip strong {
  overflow-wrap: anywhere;
  color: var(--operator-surface-foreground);
  line-height: 1.08;
}

.strategy-readiness-strip strong {
  font-size: 1rem;
  text-transform: capitalize;
}

.v13-readiness-strip strong {
  font-size: 1rem;
  text-transform: none;
}

.academic-mvp-gate-strip strong {
  font-size: 0.9rem;
  text-transform: none;
}

.future-status-card small,
.strategy-readiness-strip small,
.v13-readiness-strip small,
.academic-mvp-gate-strip small {
  overflow-wrap: anywhere;
  color: var(--operator-text-soft);
  font-size: 0.78rem;
  font-weight: 720;
  line-height: 1.42;
}

@media (max-width: 1320px) {
  .future-status-grid,
  .strategy-readiness-strip,
  .v13-readiness-strip,
  .academic-mvp-gate-strip {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .future-status-grid,
  .strategy-readiness-strip,
  .v13-readiness-strip,
  .academic-mvp-gate-strip {
    grid-template-columns: 1fr;
  }
}
</style>
