<script setup lang="ts">
const boundaryItems = [
  { label: 'Preview only', icon: 'i-lucide-eye' },
  { label: 'No ProposedBid', icon: 'i-lucide-ban' },
  { label: 'No market payload', icon: 'i-lucide-file-x-2' },
  { label: 'Human review required', icon: 'i-lucide-user-check' }
]
</script>

<template>
  <section
    class="operator-boundary-strip"
    aria-label="Operator preview boundary"
  >
    <div class="operator-boundary-strip__title">
      <UIcon name="i-lucide-shield-check" />
      <div>
        <p>Preview boundary</p>
        <span>DAM/IDM hourly read model</span>
      </div>
    </div>

    <div
      class="operator-boundary-strip__items"
      aria-label="Current execution boundary"
    >
      <span
        v-for="item in boundaryItems"
        :key="item.label"
        class="operator-boundary-strip__item"
      >
        <UIcon :name="item.icon" />
        {{ item.label }}
      </span>
    </div>

    <div class="operator-boundary-strip__meta">
      <span>Read-model evidence</span>
      <UButton
        class="operator-boundary-strip__link"
        to="/defense"
        icon="i-lucide-presentation"
        label="Defense path"
        color="info"
        variant="ghost"
      />
    </div>
  </section>
</template>

<style scoped>
.operator-boundary-strip {
  display: grid;
  grid-template-columns: minmax(12rem, 0.9fr) minmax(18rem, 1.5fr) minmax(13rem, 0.8fr);
  align-items: center;
  gap: 0.7rem;
  min-width: 0;
  border: 1px solid var(--operator-responsive-panel-border);
  border-radius: 0.8rem;
  background:
    radial-gradient(circle at top left, var(--operator-rail-panel-accent-wash), transparent 34%),
    linear-gradient(180deg, var(--operator-responsive-panel-gradient-top), var(--operator-responsive-panel-gradient-bottom));
  box-shadow:
    var(--operator-responsive-panel-shadow),
    var(--operator-responsive-panel-inset-top),
    var(--operator-responsive-panel-inset-bottom);
  color: var(--operator-control-foreground);
  padding: 0.62rem 0.78rem;
}

.operator-boundary-strip__title,
.operator-boundary-strip__items,
.operator-boundary-strip__item,
.operator-boundary-strip__meta {
  min-width: 0;
}

.operator-boundary-strip__title {
  display: flex;
  align-items: center;
  gap: 0.55rem;
}

.operator-boundary-strip__title > span,
.operator-boundary-strip__title svg {
  color: var(--operator-accent);
}

.operator-boundary-strip__title p {
  margin: 0;
  font-size: 0.86rem;
  font-weight: 900;
  line-height: 1.1;
}

.operator-boundary-strip__title span,
.operator-boundary-strip__meta span {
  color: var(--operator-text-muted);
  font-size: 0.72rem;
  font-weight: 800;
  line-height: 1.25;
  overflow-wrap: anywhere;
}

.operator-boundary-strip__items {
  display: flex;
  flex-wrap: wrap;
  gap: 0.38rem;
}

.operator-boundary-strip__item {
  display: inline-flex;
  align-items: center;
  gap: 0.32rem;
  border: 1px solid var(--operator-rail-chip-border);
  border-radius: 999px;
  background: var(--operator-rail-chip-surface);
  color: var(--operator-text-readable);
  padding: 0.34rem 0.5rem;
  font-size: 0.72rem;
  font-weight: 900;
  line-height: 1.1;
  white-space: nowrap;
}

.operator-boundary-strip__item svg {
  color: var(--operator-accent);
}

.operator-boundary-strip__meta {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 0.5rem;
}

.operator-boundary-strip__meta span:first-child {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.operator-boundary-strip__link {
  flex: 0 0 auto;
}

@media (max-width: 1040px) {
  .operator-boundary-strip {
    grid-template-columns: 1fr;
    align-items: stretch;
  }

  .operator-boundary-strip__meta {
    grid-template-columns: minmax(0, 1fr);
  }

  .operator-boundary-strip__link {
    grid-column: 1 / -1;
    justify-self: start;
  }
}

@media (max-width: 520px) {
  .operator-boundary-strip__items {
    display: grid;
    grid-template-columns: 1fr;
  }

  .operator-boundary-strip__item {
    white-space: normal;
  }
}
</style>
