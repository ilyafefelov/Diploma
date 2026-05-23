<script setup lang="ts">
import type { OperatorGatekeeperAction } from '~/types/operator-dashboard'

defineProps<{
  actions: OperatorGatekeeperAction[]
  activeAlertCount: number
}>()
</script>

<template>
  <section
    id="operator-gatekeeper"
    class="surface-panel gatekeeper-panel"
  >
    <div class="rail-heading">
      <div>
        <p class="eyebrow">
          Preview scorer
        </p>
        <h2 class="rail-title">
          DAM delivery-hour preference
        </h2>
      </div>
      <UIcon
        class="rail-heading__icon"
        name="i-lucide-shield-check"
      />
    </div>

    <p class="gatekeeper-copy">
      Scores explain the selected DAM delivery-hour preference. They are not market bids or dispatch commands. A future
      Pydantic Bid Gatekeeper validates ProposedBid payloads before any market submission path exists.
    </p>

    <div class="gatekeeper-grid">
      <UButton
        v-for="action in actions"
        :key="action.label"
        class="gatekeeper-action"
        :class="{ 'gatekeeper-action-active': action.active }"
        :icon="action.icon"
        color="info"
        variant="ghost"
        tabindex="0"
      >
        <span>{{ action.label }}</span>
        <strong>{{ action.score }}</strong>
        <span
          class="action-tooltip"
          role="tooltip"
        >
          <span class="action-tooltip__title">{{ action.tooltipTitle }}</span>
          <span class="action-tooltip__body">{{ action.tooltipBody }}</span>
          <span class="action-tooltip__formula">{{ action.tooltipFormula }}</span>
        </span>
      </UButton>
    </div>

    <div
      class="regret-ring"
      tabindex="0"
    >
      <span>Regret</span>
      <strong>{{ activeAlertCount === 0 ? '12%' : '28%' }}</strong>
      <small>{{ activeAlertCount === 0 ? 'Low regret' : 'Needs review' }}</small>
      <span
        class="regret-tooltip"
        role="tooltip"
      >
        <strong>{{ activeAlertCount === 0 ? 'Low review risk' : 'Needs review' }}</strong>
        <span>Regret means lost value versus oracle in research scoring. This ring is a compact operator cue; the full regret graph sits in Decision Evidence.</span>
      </span>
    </div>
  </section>
</template>

<style scoped>
.gatekeeper-copy {
  grid-column: 1 / -1;
  margin: 0;
  color: rgba(229, 249, 255, 0.78);
  font-size: 0.74rem;
  font-weight: 750;
  line-height: 1.4;
}
</style>
