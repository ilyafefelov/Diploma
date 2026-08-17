import { computed } from 'vue'

import type { useDefenseDashboard } from './useDefenseDashboard'

type DefenseDashboard = ReturnType<typeof useDefenseDashboard>

export const useDefenseDashboardPanelRefs = (defense: DefenseDashboard) => ({
  defenseAcademicMvpReadiness: computed(() => defense.academicMvpReadiness.value ?? null),
  defenseActiveErrorCount: computed(() => defense.activeErrorCount.value),
  defenseBatteryState: computed(() => defense.batteryState.value ?? null),
  defenseBenchmark: computed(() => defense.benchmark.value ?? null),
  defenseDecisionPolicyPreview: computed(() => defense.dtPolicyPreview.value ?? null),
  defenseExogenousSignals: computed(() => defense.exogenousSignals.value ?? null),
  defenseFutureStack: computed(() => defense.futureStack.value ?? null),
  defenseGatekeeperValidationStatus: computed(() => defense.gatekeeperValidationStatus.value ?? null),
  defenseIsLoading: computed(() => defense.isLoading.value),
  defenseLastLoadedLabel: computed(() => defense.lastLoadedLabel.value),
  defenseModelRows: computed(() => defense.modelRows.value),
  defenseSensitivity: computed(() => defense.sensitivity.value ?? null)
})
