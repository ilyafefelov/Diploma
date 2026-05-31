// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

describe('operator shadow preview integration', () => {
  it('keeps a Nuxt read-model proxy for the FastAPI shadow endpoint', () => {
    const routePath = fileURLToPath(
      new URL('../../server/api/control-plane/dashboard/shadow-recommendation-preview.get.ts', import.meta.url)
    )
    const route = readFileSync(routePath, 'utf8')

    expect(route).toContain('/dashboard/shadow-recommendation-preview')
    expect(route).toContain('ShadowRecommendationPreviewResponse')
    expect(route).toContain('fetchControlPlane')
  })

  it('keeps strategy comparison chart anchored to the best-valid recommendation', () => {
    const page = readFileSync(fileURLToPath(new URL('../pages/operator.vue', import.meta.url)), 'utf8')
    const panel = readFileSync(fileURLToPath(new URL('../components/dashboard/operator/OperatorFutureStackPanel.vue', import.meta.url)), 'utf8')
    const panelModel = readFileSync(fileURLToPath(new URL('../composables/useOperatorFutureStackPanelModel.ts', import.meta.url)), 'utf8')
    const presentation = readFileSync(fileURLToPath(new URL('./operatorFutureStackPresentation.ts', import.meta.url)), 'utf8')
    const strategyChartOptions = readFileSync(
      fileURLToPath(new URL('../lib/operator-future/chart-options/operatorFutureStrategyChartOptions.ts', import.meta.url)),
      'utf8'
    )

    expect(panel).toContain('bestValidRecommendation: OperatorRecommendationResponse | null')
    expect(panelModel).toContain('buildStrategyComparisonRows(')
    expect(panelModel).toContain('input.bestValidRecommendation')
    expect(presentation).toContain('\'dt_v2_plus_safe_switch_selector_shadow\'')
    expect(presentation).toContain('return \'DT V2+ safe-switch\'')
    expect(strategyChartOptions).toContain('interval: 0')
    expect(strategyChartOptions).toContain('formatter: formatStrategyAxisLabel')
    expect(page).toContain(':best-valid-recommendation="operatorRecommendation"')
  })

  it('presents one schedule source switch and keeps strict baseline as context', () => {
    const panel = readFileSync(fileURLToPath(new URL('../components/dashboard/operator/OperatorFutureStackPanel.vue', import.meta.url)), 'utf8')
    const panelModel = readFileSync(fileURLToPath(new URL('../composables/useOperatorFutureStackPanelModel.ts', import.meta.url)), 'utf8')
    const header = readFileSync(fileURLToPath(new URL('../components/dashboard/operator/OperatorFutureHeaderControls.vue', import.meta.url)), 'utf8')
    const dock = readFileSync(fileURLToPath(new URL('../components/dashboard/operator/OperatorScheduleDock.vue', import.meta.url)), 'utf8')
    const previewSources = readFileSync(
      fileURLToPath(new URL('../lib/operator-future/operatorFuturePreviewSources.ts', import.meta.url)),
      'utf8'
    )

    expect(panel).toContain('<OperatorFutureHeaderControls')
    expect(header).toContain('Delivery-day schedule preview and evidence gates')
    expect(header).toContain('label="Schedule shown"')
    expect(header).toContain('aria-label="Select schedule source preview"')
    expect(header).toContain('future-baseline-context')
    expect(header).toContain('Strict similar-day baseline')
    expect(previewSources).toContain('Regret-aware V2+ selector')
    expect(previewSources).toContain('DT V2+ safe-switch selector')
    expect(panel).toContain('shadowModelStoryItems')
    expect(panelModel).toContain('Value shortfall vs strict (UAH)')
    expect(panelModel).toContain('strict LP/reference value')
    expect(panel).not.toContain('Shadow regret/value gap')
    expect(panel).not.toContain('@update:model-value="updateSelectedStrategy"')
    expect(dock).toContain('aria-label="Hourly recommendation table"')
    expect(dock).toContain('Regret / value gap')
    expect(dock).not.toContain('aria-label="Shadow hourly recommendation table"')
  })
})
