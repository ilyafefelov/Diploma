<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE,
  CURRENT_REGRET_LADDER,
  CURRENT_TFT_PORTFOLIO_DIAGNOSTICS,
  CURRENT_TFT_PORTFOLIO_CLOSURE,
  formatPercent,
  formatUah,
  summarizeScheduleValuePromotionReadModel
} from '~/utils/defenseDataset'
import {
  buildAcademicMvpDtShadowComparisonRows,
  buildAcademicMvpGatePassportItems,
  formatRuntimeAccelerationLabel
} from '~/utils/operatorFutureStack'

const preferredTenantId = 'client_003_dnipro_factory'
const selectedTenantId = ref(preferredTenantId)
const registry = useControlPlaneRegistry()
const defense = useDefenseDashboard(selectedTenantId)
const pipelineInfographicUrl = '/design/v2-plus-pipeline-infographic.png'

const selectedTenant = computed(() => {
  return registry.tenants.value.find(tenant => tenant.tenant_id === selectedTenantId.value) || null
})

const futureForecastRows = computed(() => {
  return defense.futureStack.value?.forecast_series
    .filter(series => series.model_name.includes('nbeatsx') || series.model_name.includes('tft'))
    .map(series => ({
      modelName: series.model_name,
      modelFamily: series.model_family,
      sourceStatus: series.source_status,
      uncertaintyKind: series.uncertainty_kind,
      pointCount: series.points.length,
      firstForecast: series.points[0]?.forecast_price_uah_mwh ?? null,
      lastForecast: series.points.at(-1)?.forecast_price_uah_mwh ?? null,
      meanRegretUah: series.mean_regret_uah,
      winRate: series.win_rate
    })) ?? []
})

const futureBackendStatusText = computed(() => {
  const statusEntries = Object.entries(defense.futureStack.value?.backend_status ?? {})
  const runtimeText = formatRuntimeAccelerationLabel(defense.futureStack.value?.runtime_acceleration)
  if (statusEntries.length === 0) {
    return `official backend status not loaded / runtime ${runtimeText}`
  }

  return `${statusEntries.map(([name, status]) => `${name}: ${status}`).join(' / ')} / runtime ${runtimeText}`
})

const dtPolicySummary = computed(() => {
  const preview = defense.dtPolicyPreview.value
  if (!preview) {
    return null
  }

  return {
    readiness: preview.policy_readiness,
    rows: preview.row_count,
    violations: preview.constraint_violation_count,
    meanValueGap: preview.mean_value_gap_uah,
    valueVsHold: preview.total_value_vs_hold_uah,
    stateFeatures: preview.policy_state_features.join(', '),
    valueInterpretation: preview.policy_value_interpretation,
    operatorBoundary: preview.operator_boundary,
    boundary: preview.academic_scope
  }
})

const latestBatterySoc = computed(() => {
  const telemetrySoc = defense.batteryState.value?.latest_telemetry?.current_soc
  const hourlySoc = defense.batteryState.value?.hourly_snapshot?.soc_close

  if (typeof telemetrySoc === 'number') {
    return formatPercent(telemetrySoc)
  }

  if (typeof hourlySoc === 'number') {
    return formatPercent(hourlySoc)
  }

  return 'unavailable'
})

const academicMvpReadiness = computed(() => defense.academicMvpReadiness.value)

const academicMvpGatePassportRows = computed(() => (
  buildAcademicMvpGatePassportItems(academicMvpReadiness.value)
))

const dtShadowComparisonRows = computed(() => (
  buildAcademicMvpDtShadowComparisonRows(academicMvpReadiness.value)
))

const dtShadowGate = computed(() => (
  asDefenseRecord(academicMvpReadiness.value?.dt_research_shadow_gate)
))

const dtShadowStatusRows = computed(() => [
  {
    label: 'Backbone',
    value: formatStatusText(dtShadowGate.value.model_backbone),
    note: formatStatusText(dtShadowGate.value.model_backbone_selection_reason)
  },
  {
    label: 'Research rows',
    value: formatCount(dtShadowGate.value.research_shadow_training_rows),
    note: `${formatCount(dtShadowGate.value.promotable_v13_permitted_training_rows)} promotable rows`
  },
  {
    label: 'Split',
    value: formatStatusText(dtShadowGate.value.split_strategy),
    note: dtShadowGate.value.chronological_split_passed === true ? 'chronological split passed' : 'chronological split pending'
  },
  {
    label: 'Receipt gate',
    value: dtShadowGate.value.publication_receipt_verified === true ? 'verified' : 'blocked',
    note: formatStatusText(dtShadowGate.value.promotion_blocker, 'publication receipt not verified')
  }
])

const academicMvpPhaseRows = computed(() => {
  const readiness = asDefenseRecord(academicMvpReadiness.value?.prototype_phase_readiness)
  return [
    {
      label: 'Phase 0',
      value: formatStatusText(asDefenseRecord(readiness.phase_0_v13_source_readiness).status),
      note: 'V13 source readiness'
    },
    {
      label: 'Phase 1',
      value: formatStatusText(asDefenseRecord(readiness.phase_1_lava_npz_smoke).status),
      note: 'LAVA NPZ smoke'
    },
    {
      label: 'Phase 2',
      value: formatStatusText(asDefenseRecord(readiness.phase_2_v13_gated_teacher_contract).status),
      note: 'teacher contract'
    },
    {
      label: 'Phase 3',
      value: formatStatusText(asDefenseRecord(readiness.phase_3_offline_challenger).status),
      note: 'offline challenger'
    },
    {
      label: 'Phase 4',
      value: formatStatusText(asDefenseRecord(readiness.phase_4_full_schedule_dfl).status),
      note: 'full schedule-level DFL'
    }
  ]
})

const thesisEvidence = computed(() => [
  {
    label: 'Offline V2+',
    value: formatUah(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.meanRegretUah),
    note: `${formatPercent(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.improvementVsStrict)} vs strict / ${CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.rollingPassCount}/${CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.rollingWindowCount} rolling`,
    tooltipTitle: 'Offline Strategy Promotion headline',
    tooltipBody: 'Current strongest thesis evidence: Ukrainian-only official global-panel NBEATSx Schedule/Value Learner V2+. This card is evidence/read-model language, not live dispatch.',
    tooltipFormula: 'promotion = strict LP/oracle regret gate, market_execution_enabled=false'
  },
  {
    label: 'Control baseline',
    value: formatUah(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.strictMeanRegretUah),
    note: 'strict_similar_day mean regret',
    tooltipTitle: 'Control baseline',
    tooltipBody: 'Frozen strict similar-day comparator from the current V2+ evidence packet. It remains the default fallback/control.',
    tooltipFormula: 'mean_regret = avg(oracle_value_uah - decision_value_uah)'
  },
  {
    label: 'TFT portfolio',
    value: `${CURRENT_TFT_PORTFOLIO_CLOSURE.rollingPassCount}/${CURRENT_TFT_PORTFOLIO_CLOSURE.rollingWindowCount}`,
    note: `${CURRENT_TFT_PORTFOLIO_CLOSURE.tftBetterCandidateCount}/${CURRENT_TFT_PORTFOLIO_CLOSURE.latestTenantAnchors} local opportunities`,
    tooltipTitle: 'Latest closed TFT portfolio test',
    tooltipBody: CURRENT_TFT_PORTFOLIO_CLOSURE.interpretation,
    tooltipFormula: `candidate_portfolio_rows=${CURRENT_TFT_PORTFOLIO_CLOSURE.candidatePortfolioRows}`
  },
  {
    label: 'Observed anchors',
    value: defense.benchmarkSummary.value ? `${defense.benchmarkSummary.value.anchorCount}` : 'unavailable',
    note: defense.benchmarkSummary.value?.dataQualityTier || 'not materialized',
    tooltipTitle: 'Observed anchors',
    tooltipBody: 'Count of rolling-origin evaluation timestamps with observed DAM and required exogenous coverage.',
    tooltipFormula: 'anchor_count = count(unique forecast origins with thesis-grade rows)'
  },
  {
    label: 'Battery truth',
    value: latestBatterySoc.value,
    note: defense.batteryState.value?.fallback_reason || defense.batteryState.value?.hourly_snapshot?.telemetry_freshness || 'live telemetry',
    tooltipTitle: 'Battery truth',
    tooltipBody: 'Physical battery state from telemetry when available, otherwise latest hourly Silver snapshot.',
    tooltipFormula: 'SOC = latest_telemetry.current_soc ?? hourly_snapshot.soc_close'
  }
])

const offlinePromotionRows = computed(() => defense.offlineStrategyPromotion.value?.rows ?? [])

const offlinePromotionReadModelLabel = computed(() => (
  summarizeScheduleValuePromotionReadModel(defense.offlineStrategyPromotion.value)
))

const regretLadderMax = computed(() => Math.max(
  ...CURRENT_REGRET_LADDER.map(point => point.meanRegretUah),
  1
))

const regretLadderRows = computed(() => CURRENT_REGRET_LADDER.map(point => ({
  ...point,
  barWidthPercent: Math.max(8, Math.round((point.meanRegretUah / regretLadderMax.value) * 100))
})))

const tftPortfolioRows = computed(() => CURRENT_TFT_PORTFOLIO_DIAGNOSTICS.map(point => ({
  ...point,
  percentLabel: point.denominator === 0 ? 'n/a' : formatPercent(point.numerator / point.denominator),
  barWidthPercent: point.denominator === 0 ? 0 : Math.max(4, Math.round((point.numerator / point.denominator) * 100))
})))

const narrativeSteps = [
  {
    label: '1. Headline',
    text: 'V2+ is the current Ukrainian-only Offline Strategy Promotion result: 174.77 UAH mean regret and 4/4 rolling windows.'
  },
  {
    label: '2. Control',
    text: 'strict_similar_day remains the frozen fallback and comparator. The dashboard does not switch live strategy defaults.'
  },
  {
    label: '3. TFT portfolio',
    text: 'TFT contributed 24/90 local post-hoc schedule opportunities, but the prior-only selector could not safely pick them and rolling replay failed 0/4.'
  },
  {
    label: '4. Evidence path',
    text: 'Forecasts become feasible schedules, schedules are strict-scored by LP/oracle regret, and claims stay offline/read-model only.'
  },
  {
    label: '5. Next',
    text: 'The next research branch is DT/LAVA-style candidate or schedule-neighbor supervision against V2+, not another dashboard default.'
  }
]

const claimBoundaries = [
  'thesis-grade only when source rows are observed and complete',
  'strict_similar_day remains default comparator',
  'V2+ is offline/read-model evidence, not live market execution',
  'TFT schedules are candidate evidence only until a prior-only selector beats V2+ robustly',
  'DT/LAVA work is next research, not deployed policy'
]

const errorRows = computed(() => {
  return Object.entries(defense.errors.value).map(([key, message]) => ({
    key,
    message
  }))
})

const asDefenseRecord = (value: unknown): Record<string, unknown> => {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return {}
  }

  return value as Record<string, unknown>
}

const formatStatusText = (value: unknown, fallback = 'pending'): string => {
  if (typeof value !== 'string' || value.length === 0) {
    return fallback
  }

  return value.replaceAll('_', ' ')
}

const formatCount = (value: unknown): string => (
  typeof value === 'number' ? value.toLocaleString('en-GB') : '0'
)

const refresh = async (): Promise<void> => {
  await defense.loadDefenseDashboard()
}

onMounted(async () => {
  await registry.loadTenants()
  const selectedTenantExists = registry.tenants.value.some(tenant => tenant.tenant_id === selectedTenantId.value)
  if (!selectedTenantExists && registry.tenants.value[0]) {
    selectedTenantId.value = registry.tenants.value[0].tenant_id
  }

  await refresh()
})

useHead({
  title: 'Research Defense Dashboard | Smart Arbitrage'
})
</script>

<template>
  <div class="defense-shell">
    <DefenseTopbar
      v-model:active-tenant-id="selectedTenantId"
      :is-loading="defense.isLoading.value"
      :tenants="registry.tenants.value"
      @refresh="refresh"
    />

    <DefenseHeroSection
      :active-error-count="defense.activeErrorCount.value"
      :last-loaded-label="defense.lastLoadedLabel.value"
      :metrics="thesisEvidence"
      :selected-tenant-id="selectedTenantId"
      :selected-tenant-name="selectedTenant?.name"
      :selected-tenant-type="selectedTenant?.type"
    />

    <DefenseNarrativeBand :steps="narrativeSteps" />
    <DefensePipelineVisualPanel :infographic-url="pipelineInfographicUrl" />
    <DefenseBilingualExplainerPanel />
    <DefenseOfflinePromotionPanel
      :read-model-label="offlinePromotionReadModelLabel"
      :rows="offlinePromotionRows"
    />
    <DefenseEvidenceChartsPanel
      :regret-rows="regretLadderRows"
      :tft-portfolio-rows="tftPortfolioRows"
    />
    <DefenseLatestExperimentPanel />
    <DefenseDtShadowPanel
      :comparison-rows="dtShadowComparisonRows"
      :gate-passport-rows="academicMvpGatePassportRows"
      :phase-rows="academicMvpPhaseRows"
      :readiness="academicMvpReadiness"
      :status-rows="dtShadowStatusRows"
    />
    <DefenseBenchmarkContextSection
      :claim-boundaries="claimBoundaries"
      :model-rows="defense.modelRows.value"
      :source-mode="defense.benchmarkSummary.value?.sourceMode || 'FastAPI pending'"
    />
    <DefenseForecastEvidenceSection
      :backend-status-text="futureBackendStatusText"
      :dt-policy-summary="dtPolicySummary"
      :future-forecast-rows="futureForecastRows"
      :selected-forecast-model="defense.futureStack.value?.selected_forecast_model || 'forecast stack pending'"
    />
    <DefenseForecastDiagnosticsSection
      :bucket-rows="defense.sensitivity.value?.bucket_summary ?? []"
      :readiness-rows="defense.researchReadinessRows.value"
      :source-strategy-kind="defense.sensitivity.value?.source_strategy_kind || 'not loaded'"
    />
    <DefenseLiveContextSection
      :battery-state="defense.batteryState.value"
      :error-rows="errorRows"
      :exogenous-signals="defense.exogenousSignals.value"
      :latest-battery-soc="latestBatterySoc"
    />
  </div>
</template>

<style src="../assets/css/defense.shell.css"></style>

<style src="../assets/css/defense.pipeline.css"></style>

<style src="../assets/css/defense.research.css"></style>

<style src="../assets/css/defense.charts.css"></style>

<style src="../assets/css/defense.read-model.css"></style>

<style src="../assets/css/defense.responsive.css"></style>
