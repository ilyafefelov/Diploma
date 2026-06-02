import { describe, expect, it } from 'vitest'

import {
  getSelectorBlock,
  readDashboardFixture,
  readOperatorHudCss
} from './test-fixtures/operatorHudTestFixtures'

describe('operator HUD chart architecture', () => {
  it('renders decision evidence charts without blank client-only gaps', () => {
    const decisionPanel = readDashboardFixture('../components/dashboard/operator/OperatorDecisionEvidencePanel.vue')
    const css = readOperatorHudCss()

    expect(decisionPanel).toContain('class="decision-chart"')
    expect(decisionPanel).toContain('class="decision-chart decision-chart-compact"')
    expect(decisionPanel).not.toContain('<ClientOnly>')
    expect(getSelectorBlock(css, '.operator-frame .surface-panel')).toMatch(/overflow:\s*visible/)
  })

  it('keeps operator chart components SSR-safe by routing vue-echarts through a client wrapper', () => {
    const chartComponentUrls = [
      '../components/dashboard/baseline/HudBaselineChartGrid.vue',
      '../components/dashboard/HudSignalCharts.vue',
      '../components/dashboard/operator/OperatorDecisionEvidencePanel.vue',
      '../components/dashboard/operator/future-charts/OperatorForecastStackChartCard.vue',
      '../components/dashboard/operator/future-charts/OperatorPolicyValueChartCard.vue',
      '../components/dashboard/operator/future-charts/OperatorStrategyComparisonChartCard.vue',
      '../components/dashboard/operator/OperatorMarketSignalHero.vue'
    ]

    for (const componentUrl of chartComponentUrls) {
      const component = readDashboardFixture(componentUrl)

      expect(component).not.toContain('vue-echarts')
      expect(component).toContain('<ClientVChart')
    }
  })

  it('keeps chart sizing classes on the client chart DOM root', () => {
    const clientChart = readDashboardFixture('../components/dashboard/ClientVChart.vue')

    expect(clientChart).toContain('class="client-v-chart-shell"')
    expect(clientChart).toContain('v-bind="$attrs"')
    expect(clientChart.indexOf('v-bind="$attrs"')).toBeLessThan(clientChart.indexOf('<ClientOnly>'))
  })

  it('registers the ECharts graphic component for blocked chart empty states', () => {
    const clientChart = readDashboardFixture('../components/dashboard/ClientVChart.vue')

    expect(clientChart).toContain('componentsModule.GraphicComponent')
  })

  it('keeps the shared chart shell from overriding caller-owned chart heights', () => {
    const clientChart = readDashboardFixture('../components/dashboard/ClientVChart.vue')
    const shellBlock = getSelectorBlock(clientChart, '.client-v-chart-shell')

    expect(shellBlock).not.toMatch(/(?:^|\n)\s*height:\s*100%/)
    expect(shellBlock).toContain('min-height: inherit')
  })

  it('shows selected source blockers in the market signal hero instead of preparing forever', () => {
    const marketSignalHero = readDashboardFixture('../components/dashboard/operator/OperatorMarketSignalHero.vue')

    expect(marketSignalHero).toContain('selectedPreviewHasNoSourceRows')
    expect(marketSignalHero).toContain('selectedPreviewBlockedMessage')
    expect(marketSignalHero).toContain('No trade preview is shown')
    expect(marketSignalHero).toContain('!selectedPreviewHasNoSourceRows.value')
  })

  it('gives every operator ECharts surface an explicit rendered height', () => {
    const chartSurfaces = [
      {
        componentUrl: '../components/dashboard/baseline/HudBaselineChartGrid.vue',
        selector: '.baseline-chart'
      },
      {
        componentUrl: '../components/dashboard/HudSignalCharts.vue',
        stylesheetUrl: '../assets/css/hud-signal-charts.css',
        selector: '.signal-chart'
      },
      {
        componentUrl: '../components/dashboard/operator/OperatorDecisionEvidencePanel.vue',
        stylesheetUrl: '../assets/css/operator-decision-evidence.css',
        selector: '.decision-chart'
      },
      {
        componentUrl: '../components/dashboard/operator/OperatorFutureChartGrid.vue',
        stylesheetUrl: '../assets/css/operator-future-chart-grid.css',
        selector: '.future-chart'
      }
    ]

    for (const chartSurface of chartSurfaces) {
      const component = readDashboardFixture(chartSurface.componentUrl)
      const styleSource = chartSurface.stylesheetUrl
        ? readDashboardFixture(chartSurface.stylesheetUrl)
        : component

      expect(getSelectorBlock(styleSource, chartSurface.selector)).toMatch(/(?:^|\n)\s*height:\s*(?:clamp|[0-9.]+rem)/)
    }
  })

  it('keeps decision evidence visual colors behind operator design tokens', () => {
    const decisionEvidenceCss = readDashboardFixture('../assets/css/operator-decision-evidence.css')
    const tokenCss = readDashboardFixture('../assets/css/design-tokens.css')

    for (const token of [
      '--operator-accent-border',
      '--operator-accent-glow',
      '--operator-accent-readable',
      '--operator-surface-wash',
      '--operator-surface-soft',
      '--operator-text-strong',
      '--operator-text-body',
      '--operator-text-bright',
      '--operator-tooltip-border'
    ]) {
      expect(tokenCss, `${token} should live in design tokens`).toContain(`${token}:`)
      expect(decisionEvidenceCss, `${token} should be consumed by decision evidence styles`).toContain(`var(${token})`)
    }

    for (const rawColor of ['rgba(215, 255, 79', 'rgba(202, 249, 255', '#d7ff4f', '#b8ff32']) {
      expect(decisionEvidenceCss, `${rawColor} should stay inside design tokens`).not.toContain(rawColor)
    }
  })
})
