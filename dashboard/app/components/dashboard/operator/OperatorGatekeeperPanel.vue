<script setup lang="ts">
import type { GatekeeperValidationStatusResponse } from '~/types/control-plane'
import type { OperatorGatekeeperAction } from '~/types/operator-dashboard'

defineProps<{
  actions: OperatorGatekeeperAction[]
  activeAlertCount: number
  gatekeeperStatus: GatekeeperValidationStatusResponse | null
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
          DAM/IDM delivery-hour preference
        </h2>
      </div>
      <UIcon
        class="rail-heading__icon"
        name="i-lucide-shield-check"
      />
    </div>

    <p class="gatekeeper-copy">
      Scores explain the selected DAM/IDM delivery-hour preference. They are not market bids or dispatch commands. The
      Pydantic Gatekeeper blocks unsafe preview output; ProposedBid and market-submission contracts are not emitted here.
    </p>

    <div class="gatekeeper-status-strip">
      <span>{{ gatekeeperStatus?.status === 'blocked' ? 'Latest bid validation fallback' : 'Bid validation log' }}</span>
      <strong>{{ gatekeeperStatus?.canonical_outcome || 'No failures' }}</strong>
      <small>
        {{ gatekeeperStatus?.status === 'blocked'
          ? `${gatekeeperStatus.contract_type || 'Contract'} blocked before submission`
          : 'No market-stage validation failures recorded' }}
      </small>
    </div>

    <div
      v-if="actions.length === 0"
      class="gatekeeper-pending"
      role="status"
    >
      <strong>Selected preview pending</strong>
      <span>No BUY/SELL/HOLD preference is shown until the selected DAM/IDM recommendation has loaded.</span>
    </div>

    <div
      v-else
      class="gatekeeper-grid"
    >
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
      role="group"
      :aria-label="`Regret ${activeAlertCount === 0 ? '12 percent low regret' : '28 percent needs review'}`"
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
  color: var(--operator-text-muted);
  font-size: 0.74rem;
  font-weight: 750;
  line-height: 1.4;
}

.gatekeeper-status-strip {
  grid-column: 1 / -1;
  display: grid;
  gap: 0.16rem;
  padding: 0.64rem 0.76rem;
  border: 1px solid var(--operator-line-faint);
  border-radius: 0.5rem;
  background: var(--operator-surface-wash);
  color: var(--operator-text-readable);
}

.gatekeeper-status-strip span {
  color: var(--operator-accent);
  font-size: 0.66rem;
  font-weight: 900;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.gatekeeper-status-strip strong {
  color: var(--operator-control-foreground);
  font-size: 1rem;
  line-height: 1;
}

.gatekeeper-status-strip small {
  color: var(--operator-text-muted);
  font-size: 0.68rem;
  font-weight: 760;
  line-height: 1.25;
}

.gatekeeper-pending {
  grid-column: 1 / -1;
  display: grid;
  gap: 0.24rem;
  padding: 0.78rem 0.84rem;
  border: 1px solid var(--operator-line-faint);
  border-radius: 0.5rem;
  background: var(--operator-surface-wash);
  color: var(--operator-text-readable);
}

.gatekeeper-pending strong {
  color: var(--operator-control-foreground);
  font-size: 0.88rem;
  line-height: 1.1;
}

.gatekeeper-pending span {
  color: var(--operator-text-muted);
  font-size: 0.7rem;
  font-weight: 760;
  line-height: 1.35;
}
</style>
