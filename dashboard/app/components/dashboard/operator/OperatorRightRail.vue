<script setup lang="ts">
import OperatorBatteryPanel from '~/components/dashboard/operator/OperatorBatteryPanel.vue'
import OperatorGatekeeperPanel from '~/components/dashboard/operator/OperatorGatekeeperPanel.vue'
import OperatorMoodPanel from '~/components/dashboard/operator/OperatorMoodPanel.vue'
import OperatorNotesPanel from '~/components/dashboard/operator/OperatorNotesPanel.vue'
import OperatorWeatherControlsPanel from '~/components/dashboard/operator/OperatorWeatherControlsPanel.vue'
import type {
  OperatorGatekeeperAction,
  OperatorMoodChip,
  OperatorMotiveItem
} from '~/types/operator-dashboard'

const includePriceHistory = defineModel<boolean>('includePriceHistory', {
  required: true
})

defineProps<{
  moodChips: OperatorMoodChip[]
  batteryStatusLabel: string
  batterySocPercent: number
  batterySocSourceLabel: string
  batterySocFormula: string
  batterySohProxyPercent: number
  batterySohSourceLabel: string
  batterySohFormula: string
  latestRecommendedPowerLabel: string
  gatekeeperActions: OperatorGatekeeperAction[]
  activeAlertCount: number
  statusLabel: string
  isPreparing: boolean
  isMaterializing: boolean
  hasSelectedTenant: boolean
  lastActionLabel: string
  weatherLocationLabel: string
  motiveItems: OperatorMotiveItem[]
  primaryBoundaryCopy: string
  nextStepsItems: string[]
  selectedRunConfigSnippet: string
}>()

const emit = defineEmits<{
  prepare: []
  materialize: []
}>()
</script>

<template>
  <aside class="operator-right-rail">
    <OperatorMoodPanel :chips="moodChips" />

    <OperatorBatteryPanel
      :status-label="batteryStatusLabel"
      :soc-percent="batterySocPercent"
      :soc-source-label="batterySocSourceLabel"
      :soc-formula="batterySocFormula"
      :soh-percent="batterySohProxyPercent"
      :soh-source-label="batterySohSourceLabel"
      :soh-formula="batterySohFormula"
      :power-label="latestRecommendedPowerLabel"
    />

    <OperatorGatekeeperPanel
      :actions="gatekeeperActions"
      :active-alert-count="activeAlertCount"
    />

    <OperatorWeatherControlsPanel
      v-model:include-price-history="includePriceHistory"
      :status-label="statusLabel"
      :is-preparing="isPreparing"
      :is-materializing="isMaterializing"
      :has-selected-tenant="hasSelectedTenant"
      :last-action-label="lastActionLabel"
      :weather-location-label="weatherLocationLabel"
      @prepare="emit('prepare')"
      @materialize="emit('materialize')"
    />

    <OperatorNotesPanel
      :motive-items="motiveItems"
      :primary-boundary-copy="primaryBoundaryCopy"
      :next-steps-items="nextStepsItems"
      :selected-run-config-snippet="selectedRunConfigSnippet"
    />
  </aside>
</template>
