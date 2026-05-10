import type { DecisionPolicyPreviewResponse } from '~/types/control-plane'

type ControlPlaneQuery = Record<string, string | number | boolean | null | undefined>

export default defineEventHandler(async (event): Promise<DecisionPolicyPreviewResponse> => {
  const runtimeConfig = useRuntimeConfig()
  const apiBase = String(runtimeConfig.apiBase || 'http://127.0.0.1:8010')
  const query = getQuery(event) as ControlPlaneQuery
  const tenantId = String(query.tenant_id || 'unknown')

  try {
    return await $fetch<DecisionPolicyPreviewResponse>(`${apiBase}/dashboard/decision-policy-preview`, {
      query
    })
  } catch {
    return {
      tenant_id: tenantId,
      row_count: 0,
      policy_run_id: 'not_materialized',
      created_at: new Date().toISOString(),
      policy_readiness: 'not_materialized',
      live_policy_claim: false,
      market_execution_enabled: false,
      constraint_violation_count: 0,
      mean_value_gap_uah: 0,
      total_value_vs_hold_uah: 0,
      forecast_context_source: 'not_materialized',
      forecast_context_row_count: 0,
      forecast_context_coverage_ratio: 0,
      forecast_context_warning: 'Decision Transformer policy preview is not materialized for this demo.',
      policy_state_features: ['SOC', 'forecast_vector', 'battery_limits', 'market_context'],
      policy_value_interpretation: 'Future DT metric will compare chosen policy value against the best feasible counterfactual action.',
      operator_boundary: 'DT is shown as a roadmap/readiness item only; operator route must not treat it as live control.',
      academic_scope: 'not live policy',
      rows: []
    }
  }
})
