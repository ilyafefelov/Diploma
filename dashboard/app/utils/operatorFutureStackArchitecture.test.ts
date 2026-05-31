// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'

import { describe, expect, it } from 'vitest'
import { readDesignTokenBundle } from './test-fixtures/operatorHudTestFixtures'

const componentPath = (name: string): string => fileURLToPath(
  new URL(`../components/dashboard/operator/${name}`, import.meta.url)
)
const futureChartComponentPath = (name: string): string => fileURLToPath(
  new URL(`../components/dashboard/operator/future-charts/${name}`, import.meta.url)
)
const futureStackUtilPath = fileURLToPath(new URL('./operatorFutureStack.ts', import.meta.url))
const futureChartCssPath = fileURLToPath(new URL('../assets/css/operator-future-chart-grid.css', import.meta.url))
const futureStackModuleUrls = [
  '../lib/operator-future/operatorFutureStackCore.ts',
  '../lib/operator-future/operatorFutureStackReadiness.ts',
  '../lib/operator-future/operatorAcademicMvpGatePassport.ts',
  '../lib/operator-future/operatorFutureStackPolicyContext.ts',
  '../lib/operator-future/operatorFutureForecastPanelModel.ts'
]

const approxTokens = (text: string): number => Math.ceil(text.length / 4)

describe('operator future-stack architecture', () => {
  it('keeps the future stack panel as orchestration, not a monolithic markup/styles module', () => {
    const panel = readFileSync(componentPath('OperatorFutureStackPanel.vue'), 'utf8')

    expect(panel).toContain('useOperatorFutureStackPanelModel')
    expect(panel).toContain('<OperatorFutureHeaderControls')
    expect(panel).toContain('<OperatorFutureReadinessStrips')
    expect(panel).toContain('<OperatorFutureChartGrid')
    expect(panel).toContain('<OperatorFutureExplainerGrid')
    expect(panel).toContain('from \'./OperatorFutureHeaderControls.vue\'')
    expect(panel).toContain('from \'./OperatorFutureReadinessStrips.vue\'')
    expect(panel).toContain('from \'./OperatorFutureChartGrid.vue\'')
    expect(panel).toContain('from \'./OperatorFutureExplainerGrid.vue\'')
    expect(panel).not.toContain('class="future-control-stack"')
    expect(panel).not.toContain('class="future-status-grid"')
    expect(panel).not.toContain('class="future-chart-grid"')
    expect(panel).not.toContain('class="future-explainer-grid"')
    expect(approxTokens(panel)).toBeLessThan(3200)
  })

  it('stores extracted future-stack regions in focused Vue modules', () => {
    for (const fileName of [
      'OperatorFutureHeaderControls.vue',
      'OperatorFutureReadinessStrips.vue',
      'OperatorFutureChartGrid.vue',
      'OperatorFutureExplainerGrid.vue'
    ]) {
      const path = componentPath(fileName)
      expect(existsSync(path), `${fileName} should exist`).toBe(true)
      const source = readFileSync(path, 'utf8')
      expect(source).toContain('<script setup lang="ts">')
      expect(source).toContain('<template>')
      expect(approxTokens(source)).toBeLessThan(5000)
    }
  })

  it('keeps the future chart grid as a chart-card orchestrator', () => {
    const chartGrid = readFileSync(componentPath('OperatorFutureChartGrid.vue'), 'utf8')

    expect(chartGrid).toContain('<OperatorForecastStackChartCard')
    expect(chartGrid).toContain('<OperatorPolicyValueChartCard')
    expect(chartGrid).toContain('<OperatorStrategyComparisonChartCard')
    expect(chartGrid).toContain('<style src="../../../assets/css/operator-future-chart-grid.css"></style>')
    expect(chartGrid).not.toContain('<ClientVChart')
    expect(chartGrid).not.toContain('<article class="future-chart-card"')
    expect(approxTokens(chartGrid), 'OperatorFutureChartGrid.vue should stay orchestration-only').toBeLessThan(2200)

    for (const fileName of [
      'OperatorForecastStackChartCard.vue',
      'OperatorPolicyValueChartCard.vue',
      'OperatorStrategyComparisonChartCard.vue'
    ]) {
      const path = futureChartComponentPath(fileName)

      expect(existsSync(path), `${fileName} should exist`).toBe(true)

      const source = readFileSync(path, 'utf8')
      expect(source).toContain('<ClientVChart')
      expect(source).toContain('class="future-chart-card')
      expect(approxTokens(source), `${fileName} should stay focused`).toBeLessThan(2600)
    }

    expect(existsSync(futureChartCssPath), 'operator-future-chart-grid.css should exist').toBe(true)
  })

  it('keeps chart client-only behavior localized inside the shared chart adapter', () => {
    const chartGrid = readFileSync(componentPath('OperatorFutureChartGrid.vue'), 'utf8')
    const chartCards = [
      'OperatorForecastStackChartCard.vue',
      'OperatorPolicyValueChartCard.vue',
      'OperatorStrategyComparisonChartCard.vue'
    ].map(fileName => readFileSync(futureChartComponentPath(fileName), 'utf8')).join('\n')

    expect(chartCards).toContain('<ClientVChart')
    expect(chartGrid).not.toContain('<ClientOnly>')
    expect(chartGrid).not.toContain('</ClientOnly>')
  })

  it('keeps the future-stack utility as a bounded public barrel', () => {
    const source = readFileSync(futureStackUtilPath, 'utf8')

    expect(approxTokens(source), 'operatorFutureStack.ts should stay below 5000 approx tokens').toBeLessThan(5000)
    expect(source).toContain('operatorFutureStackCore')
    expect(source).toContain('operatorFutureStackReadiness')
    expect(source).toContain('operatorFutureStackPolicyContext')
  })

  it('keeps the future stack panel view model behind a focused composable seam', () => {
    const modelPath = fileURLToPath(new URL('../composables/useOperatorFutureStackPanelModel.ts', import.meta.url))

    expect(existsSync(modelPath), 'useOperatorFutureStackPanelModel.ts should exist').toBe(true)

    const source = readFileSync(modelPath, 'utf8')

    expect(source).toContain('export const useOperatorFutureStackPanelModel')
    expect(source).toContain('buildOperatorFutureForecastPanelModel')
    expect(source).not.toContain('selectOperatorForecastChartSource')
    expect(source).toContain('buildPolicyOption')
    expect(source).toContain('setPolicyValueMode')
    expect(approxTokens(source), 'future-stack panel model should stay below 3800 approx tokens').toBeLessThan(3800)
  })

  it('stores future-stack utility logic in bounded internal modules', () => {
    for (const moduleUrl of futureStackModuleUrls) {
      const modulePath = fileURLToPath(new URL(moduleUrl, import.meta.url))

      expect(existsSync(modulePath), `${moduleUrl} should exist`).toBe(true)

      const source = readFileSync(modulePath, 'utf8')

      expect(approxTokens(source), `${moduleUrl} should stay below 5000 approx tokens`).toBeLessThan(5000)
    }
  })

  it('keeps academic MVP gate passport assembly out of the generic readiness module', () => {
    const readinessPath = fileURLToPath(
      new URL('../lib/operator-future/operatorFutureStackReadiness.ts', import.meta.url)
    )
    const gatePassportPath = fileURLToPath(
      new URL('../lib/operator-future/operatorAcademicMvpGatePassport.ts', import.meta.url)
    )

    expect(existsSync(gatePassportPath), 'operatorAcademicMvpGatePassport.ts should exist').toBe(true)

    const readinessSource = readFileSync(readinessPath, 'utf8')
    const gatePassportSource = readFileSync(gatePassportPath, 'utf8')

    expect(readinessSource).not.toContain('buildAcademicMvpGatePassportItems')
    expect(readinessSource).not.toContain('prototype_phase_readiness')
    expect(gatePassportSource).toContain('buildAcademicMvpGatePassportItems')
    expect(gatePassportSource).toContain('prototype_phase_readiness')
    expect(approxTokens(readinessSource), 'readiness module should stay focused').toBeLessThan(2600)
    expect(approxTokens(gatePassportSource), 'gate passport module should stay focused').toBeLessThan(3200)
  })

  it('keeps future-stack visual colors behind operator design tokens', () => {
    const designTokensCss = readDesignTokenBundle()
    const futureStackStyles = [
      'OperatorFutureHeaderControls.vue',
      'OperatorFutureReadinessStrips.vue',
      'OperatorFutureChartGrid.vue',
      'OperatorFutureExplainerGrid.vue'
    ]
      .map(fileName => readFileSync(componentPath(fileName), 'utf8'))
      .concat(readFileSync(futureChartCssPath, 'utf8'))
      .join('\n')

    for (const token of [
      '--operator-accent-soft',
      '--operator-accent-faint',
      '--operator-accent-wash',
      '--operator-line-subtle',
      '--operator-line-faint',
      '--operator-card-border',
      '--operator-card-border-strong',
      '--operator-card-accent-wash',
      '--operator-card-gradient-top',
      '--operator-card-gradient-bottom',
      '--operator-control-surface-muted',
      '--operator-control-surface',
      '--operator-control-surface-strong',
      '--operator-control-foreground',
      '--operator-control-foreground-muted',
      '--operator-chip-surface',
      '--operator-chip-surface-muted',
      '--operator-chip-surface-soft',
      '--operator-active-text',
      '--operator-warning-border-muted',
      '--operator-warning-border',
      '--operator-warning-border-strong',
      '--operator-warning-glow',
      '--operator-warning-surface-muted',
      '--operator-warning-surface-soft',
      '--operator-warning-gradient-top',
      '--operator-text-soft',
      '--operator-text-muted',
      '--operator-positive'
    ]) {
      expect(designTokensCss, `${token} should be declared globally`).toContain(`${token}:`)
      expect(futureStackStyles, `${token} should be consumed by future-stack styles`).toContain(`var(${token})`)
    }

    for (const rawColor of [
      'rgba(215, 255, 79',
      'rgba(202, 249, 255',
      'rgba(229, 249, 255',
      'rgba(255, 255, 255',
      'rgba(184, 255, 50',
      'rgba(13, 151, 218',
      'rgba(6, 82, 147',
      'rgba(4, 67, 119',
      'rgba(255, 191, 82',
      'rgba(151, 82, 8',
      'rgba(119, 65, 9',
      'rgba(183, 100, 17',
      '#d7ff4f',
      '#b8ff32',
      '#f2fbff',
      '#fff0c7',
      '#eaff6b',
      '#f2f2f2'
    ]) {
      expect(futureStackStyles, `${rawColor} should stay inside design tokens`).not.toContain(rawColor)
    }
  })
})
