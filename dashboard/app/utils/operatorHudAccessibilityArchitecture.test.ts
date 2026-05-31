import { describe, expect, it } from 'vitest'

import {
  getSelectorBlock,
  readDashboardFixture,
  readOperatorHudCss
} from './test-fixtures/operatorHudTestFixtures'

describe('operator HUD accessibility architecture', () => {
  it('provides a keyboard skip link to the operator content target', () => {
    const operatorPage = readDashboardFixture('../pages/operator.vue')
    const css = readOperatorHudCss()

    expect(operatorPage).toContain('class="operator-skip-link"')
    expect(operatorPage).toContain('href="#operator-content"')
    expect(operatorPage).toContain('<main')
    expect(operatorPage).toContain('id="operator-content"')
    expect(operatorPage).toContain('tabindex="-1"')
    expect(getSelectorBlock(css, '.operator-skip-link')).toMatch(/clip-path:\s*inset\(50%\)/)
    expect(getSelectorBlock(css, '.operator-skip-link:focus-visible')).toMatch(/position:\s*fixed/)
  })

  it('labels the operator shell landmarks for screen reader navigation', () => {
    const topBar = readDashboardFixture('../components/dashboard/operator/OperatorTopBar.vue')
    const sidebar = readDashboardFixture('../components/dashboard/operator/OperatorSidebar.vue')
    const rightRail = readDashboardFixture('../components/dashboard/operator/OperatorRightRail.vue')

    expect(topBar).toContain('<header')
    expect(sidebar).toContain('<aside')
    expect(sidebar).toContain('aria-label="Tenant selection and operator sections"')
    expect(sidebar).toContain('<nav')
    expect(sidebar).toContain('aria-label="Operator dashboard sections"')
    expect(rightRail).toContain('<aside')
    expect(rightRail).toContain('aria-label="Operator controls and readiness"')
  })

  it('keeps keyboard focus visible on operator HUD controls', () => {
    const css = readOperatorHudCss()
    const tenantMap = readDashboardFixture('../components/dashboard/operator/OperatorTenantMapCard.vue')
    const tenantMapCss = readDashboardFixture('../assets/css/operator-tenant-map.css')
    const motiveBars = readDashboardFixture('../components/dashboard/HudMotiveBars.vue')

    expect(`${css}\n${tenantMap}\n${tenantMapCss}\n${motiveBars}`).not.toMatch(/outline:\s*none/)
    expect(`${css}\n${tenantMap}\n${tenantMapCss}\n${motiveBars}`).toContain('outline-offset')
  })

  it('names non-native focusable operator info surfaces', () => {
    const focusableSurfaceComponents = [
      '../components/dashboard/HudMotiveBars.vue',
      '../components/dashboard/operator/OperatorDecisionEvidencePanel.vue',
      '../components/dashboard/operator/OperatorGatekeeperPanel.vue',
      '../components/dashboard/operator/OperatorMarketConsole.vue',
      '../components/dashboard/operator/OperatorMarketSignalHero.vue',
      '../components/dashboard/operator/OperatorMetricRibbon.vue',
      '../components/dashboard/operator/OperatorResearchPanel.vue',
      '../components/dashboard/operator/OperatorScheduleDock.vue',
      '../components/dashboard/operator/OperatorSidebar.vue'
    ]

    for (const componentUrl of focusableSurfaceComponents) {
      const component = readDashboardFixture(componentUrl)
      const customFocusTargets = [
        ...component.matchAll(/<(?:article|span|div)\b(?<attributes>[^>]*\btabindex="0"[^>]*)>/gm)
      ]

      for (const target of customFocusTargets) {
        const attributes = target.groups?.attributes ?? ''

        expect(attributes, `${componentUrl} custom focus target should expose an explicit role`).toMatch(/\srole=/)
        expect(attributes, `${componentUrl} custom focus target should expose an accessible name`).toMatch(/\saria-label=|\s:aria-label=|\saria-labelledby=|\s:aria-labelledby=/)
      }
    }
  })

  it('names operator controls through form-field or aria labels', () => {
    const sidebar = readDashboardFixture('../components/dashboard/operator/OperatorSidebar.vue')
    const futureHeader = readDashboardFixture('../components/dashboard/operator/OperatorFutureHeaderControls.vue')
    const weatherControls = readDashboardFixture('../components/dashboard/operator/OperatorWeatherControlsPanel.vue')

    expect(sidebar).toContain('<UFormField')
    expect(sidebar).toContain('label="Tenant / site"')
    expect(sidebar).toContain('aria-label="Select operator tenant"')
    expect(futureHeader).toContain('<UFormField')
    expect(futureHeader).toContain('label="Schedule shown"')
    expect(futureHeader).toContain('aria-label="Select schedule source preview"')
    expect(weatherControls).toContain('aria-label="Include DAM price history in weather materialization"')
  })

  it('renders the hourly recommendation as semantic tabular data', () => {
    const scheduleDock = readDashboardFixture('../components/dashboard/operator/OperatorScheduleDock.vue')

    expect(scheduleDock).toContain('<table class="shadow-hourly-table__table">')
    expect(scheduleDock).toMatch(/<caption class="sr-only">\s*Hourly recommendation table\s*<\/caption>/)
    expect(scheduleDock).toMatch(/<th scope="col">\s*Timestamp\s*<\/th>/)
    expect(scheduleDock).toContain('<td>{{ row.timestamp }}</td>')
  })
})
