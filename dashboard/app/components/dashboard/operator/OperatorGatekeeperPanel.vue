<script setup lang="ts">
import type { OperatorGatekeeperAction } from '~/types/operator-dashboard'

defineProps<{
  actions: OperatorGatekeeperAction[]
  activeAlertCount: number
}>()
</script>

<template>
  <section class="surface-panel gatekeeper-panel">
    <div class="rail-heading">
      <div>
        <p class="eyebrow">
          Pydantic gatekeeper
        </p>
        <h2 class="rail-title">
          Action scores
        </h2>
      </div>
      <UIcon
        class="rail-heading__icon"
        name="i-lucide-shield-check"
      />
    </div>

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
        <strong>{{ activeAlertCount === 0 ? '3,588 UAH' : '8,371 UAH' }}</strong>
        <span>{{ activeAlertCount === 0 ? 'Estimated opportunity gap on the visible gross arbitrage value.' : 'Estimated review cost while alerts are active.' }}</span>
      </span>
    </div>
  </section>
</template>
