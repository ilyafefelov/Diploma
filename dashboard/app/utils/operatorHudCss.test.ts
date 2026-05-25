// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const cssPath = fileURLToPath(new URL('../assets/css/operator-hud.css', import.meta.url))
const baselinePreviewPath = fileURLToPath(new URL('../components/dashboard/HudBaselinePreview.vue', import.meta.url))

const getSelectorBlock = (css: string, selector: string): string => {
  const escapedSelector = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const matches = [...css.matchAll(new RegExp(`${escapedSelector}\\s*\\{(?<body>[^}]*)\\}`, 'gm'))]
  return matches.map(match => match.groups?.body ?? '').join('\n')
}

describe('operator HUD CSS', () => {
  it('keeps the operator shell overflow visible so hover explanations are not clipped', () => {
    const css = readFileSync(cssPath, 'utf8')

    expect(getSelectorBlock(css, '.operator-frame')).toMatch(/overflow:\s*visible/)
    expect(getSelectorBlock(css, '.operator-main-stage')).toMatch(/overflow:\s*visible/)
    expect(getSelectorBlock(css, '.operator-frame .surface-panel')).toMatch(/overflow:\s*visible/)
  })

  it('keeps baseline preview from animating outside page width', () => {
    const baselinePreview = readFileSync(baselinePreviewPath, 'utf8')

    expect(baselinePreview).not.toContain('animation: slab-sheen')
    expect(baselinePreview).not.toContain('@keyframes slab-sheen')
  })

  it('keeps baseline comparator collapsed by default with explicit expand control', () => {
    const baselinePreview = readFileSync(baselinePreviewPath, 'utf8')

    expect(baselinePreview).toContain('isExpanded = ref(false)')
    expect(baselinePreview).toContain('Expand baseline')
    expect(baselinePreview).toContain('Collapse baseline')
    expect(baselinePreview).toContain('v-if="!isExpanded"')
    expect(baselinePreview).toContain('Compact view keeps baseline value')
  })

  it('surfaces baseline DAM delivery-window and no-execution metadata', () => {
    const baselinePreview = readFileSync(baselinePreviewPath, 'utf8')

    expect(baselinePreview).toContain('baselineBoundaryItems')
    expect(baselinePreview).toContain('target_delivery_window_start')
    expect(baselinePreview).toContain('target_delivery_window_end')
    expect(baselinePreview).toContain('anchor_timestamp')
    expect(baselinePreview).toContain('market_execution_enabled')
    expect(baselinePreview).toContain('proposed_bid_status')
    expect(baselinePreview).toContain('DAM delivery')
    expect(baselinePreview).toContain('No market execution')
  })

  it('keeps schedule dock in normal flow with one-row horizontal schedule scrolling', () => {
    const css = readFileSync(cssPath, 'utf8')

    expect(getSelectorBlock(css, '.operator-shell')).not.toMatch(/padding-bottom:\s*(?:12|15\.5)rem/)
    expect(getSelectorBlock(css, '.operator-shell')).toMatch(/overflow-x:\s*clip/)
    expect(getSelectorBlock(css, '.schedule-dock')).toMatch(/position:\s*relative/)
    expect(getSelectorBlock(css, '.schedule-dock')).not.toMatch(/bottom:\s*0\.75rem/)
    expect(getSelectorBlock(css, '.schedule-dock')).not.toMatch(/transform:\s*translateX/)
    expect(getSelectorBlock(css, '.schedule-dock')).toMatch(/width:\s*100%/)
    expect(getSelectorBlock(css, '.schedule-dock')).toMatch(/max-width:\s*100%/)
    expect(getSelectorBlock(css, '.schedule-track')).toMatch(/display:\s*flex/)
    expect(getSelectorBlock(css, '.schedule-track')).toMatch(/overflow-x:\s*auto/)
    expect(getSelectorBlock(css, '.schedule-track')).toMatch(/flex-wrap:\s*nowrap/)
    expect(getSelectorBlock(css, '.schedule-segment--orange')).toMatch(/background:\s*linear-gradient/)
  })

  it('renders schedule hover explanation outside the clipped horizontal scroller', () => {
    const css = readFileSync(cssPath, 'utf8')
    const scheduleDock = readFileSync(
      fileURLToPath(new URL('../components/dashboard/operator/OperatorScheduleDock.vue', import.meta.url)),
      'utf8'
    )

    expect(scheduleDock).toContain('schedule-dock__floating-tooltip')
    expect(scheduleDock).not.toContain('class="schedule-tooltip"')
    expect(getSelectorBlock(css, '.schedule-track')).not.toMatch(/overflow-y:\s*hidden/)
    expect(getSelectorBlock(css, '.schedule-dock__floating-tooltip')).toMatch(/position:\s*fixed/)
  })

  it('renders explicit price and forecast period labels in the operator market signal hero', () => {
    const marketSignalHero = readFileSync(
      fileURLToPath(new URL('../components/dashboard/operator/OperatorMarketSignalHero.vue', import.meta.url)),
      'utf8'
    )

    expect(marketSignalHero).toContain('latestPricePeriodLabel')
    expect(marketSignalHero).toContain('forecastWindowPeriodLabel')
    expect(marketSignalHero).not.toContain('Latest visible hour')
  })

  it('keeps the operator first viewport scoped to DAM planning preview copy', () => {
    const topBar = readFileSync(
      fileURLToPath(new URL('../components/dashboard/operator/OperatorTopBar.vue', import.meta.url)),
      'utf8'
    )
    const marketConsole = readFileSync(
      fileURLToPath(new URL('../components/dashboard/operator/OperatorMarketConsole.vue', import.meta.url)),
      'utf8'
    )
    const marketSignalHero = readFileSync(
      fileURLToPath(new URL('../components/dashboard/operator/OperatorMarketSignalHero.vue', import.meta.url)),
      'utf8'
    )
    const scheduleDock = readFileSync(
      fileURLToPath(new URL('../components/dashboard/operator/OperatorScheduleDock.vue', import.meta.url)),
      'utf8'
    )
    const signalCharts = readFileSync(
      fileURLToPath(new URL('../components/dashboard/HudSignalCharts.vue', import.meta.url)),
      'utf8'
    )
    const operatorPage = readFileSync(
      fileURLToPath(new URL('../pages/operator.vue', import.meta.url)),
      'utf8'
    )
    const batteryPanel = readFileSync(
      fileURLToPath(new URL('../components/dashboard/operator/OperatorBatteryPanel.vue', import.meta.url)),
      'utf8'
    )
    const gatekeeperPanel = readFileSync(
      fileURLToPath(new URL('../components/dashboard/operator/OperatorGatekeeperPanel.vue', import.meta.url)),
      'utf8'
    )
    const moodPanel = readFileSync(
      fileURLToPath(new URL('../components/dashboard/operator/OperatorMoodPanel.vue', import.meta.url)),
      'utf8'
    )

    expect(topBar).toContain('Operator Preview')
    expect(topBar).not.toContain('BESS Control')
    expect(topBar).toContain('Preview gaps')
    expect(marketConsole).toContain('DAM hourly planning preview')
    expect(marketConsole).not.toContain('DAM / IDM arbitrage surface')
    expect(marketSignalHero).toContain('DAM hourly')
    expect(marketSignalHero).toContain('IDM disabled')
    expect(marketSignalHero).toContain('DAM context price')
    expect(marketSignalHero).not.toContain('DAM delivery price')
    expect(marketSignalHero).not.toContain('label="IDM"')
    expect(marketSignalHero).not.toContain('label="Both"')
    expect(scheduleDock).toContain('DAM delivery day review')
    expect(scheduleDock).toContain('Review mode')
    expect(scheduleDock).not.toContain('Schedule timeline')
    expect(scheduleDock).not.toContain('Dispatch mode')
    expect(signalCharts).toContain('Selected schedule and value preview')
    expect(signalCharts).toContain('Review context for selected preview')
    expect(signalCharts).not.toContain('Use now: context for selected preview')
    expect(batteryPanel).toContain('First DAM action')
    expect(batteryPanel).toContain('DAM delivery-hour preview')
    expect(batteryPanel).not.toContain('Intent to dispatch')
    expect(gatekeeperPanel).toContain('Preview scorer')
    expect(gatekeeperPanel).toContain('DAM delivery-hour preference')
    expect(gatekeeperPanel).not.toContain('Pydantic gatekeeper')
    expect(moodPanel).toContain('Preview posture')
    expect(moodPanel).not.toContain('Operator mood')
    expect(moodPanel).not.toContain('Great')
    expect(operatorPage).toContain('DAM delivery-day preview / no ProposedBid / no market submission')
  })

  it('renders decision evidence charts without blank client-only gaps', () => {
    const decisionPanel = readFileSync(
      fileURLToPath(new URL('../components/dashboard/operator/OperatorDecisionEvidencePanel.vue', import.meta.url)),
      'utf8'
    )
    const css = readFileSync(cssPath, 'utf8')

    expect(decisionPanel).toContain('class="decision-chart"')
    expect(decisionPanel).toContain('class="decision-chart decision-chart-compact"')
    expect(decisionPanel).not.toContain('<ClientOnly>')
    expect(getSelectorBlock(css, '.operator-frame .surface-panel')).toMatch(/overflow:\s*visible/)
  })

  it('keeps operator chart components SSR-safe by routing vue-echarts through a client wrapper', () => {
    const chartComponentUrls = [
      '../components/dashboard/HudBaselinePreview.vue',
      '../components/dashboard/HudSignalCharts.vue',
      '../components/dashboard/operator/OperatorDecisionEvidencePanel.vue',
      '../components/dashboard/operator/OperatorFutureStackPanel.vue',
      '../components/dashboard/operator/OperatorMarketSignalHero.vue'
    ]

    for (const componentUrl of chartComponentUrls) {
      const component = readFileSync(fileURLToPath(new URL(componentUrl, import.meta.url)), 'utf8')

      expect(component).not.toContain('vue-echarts')
      expect(component).toContain('<ClientVChart')
    }
  })

  it('keeps chart sizing classes on the client chart DOM root', () => {
    const clientChart = readFileSync(
      fileURLToPath(new URL('../components/dashboard/ClientVChart.vue', import.meta.url)),
      'utf8'
    )

    expect(clientChart).toContain('class="client-v-chart-shell"')
    expect(clientChart).toContain('v-bind="$attrs"')
    expect(clientChart.indexOf('v-bind="$attrs"')).toBeLessThan(clientChart.indexOf('<ClientOnly>'))
  })

  it('gives every operator ECharts surface an explicit rendered height', () => {
    const chartSurfaces = [
      {
        componentUrl: '../components/dashboard/HudBaselinePreview.vue',
        selector: '.baseline-chart'
      },
      {
        componentUrl: '../components/dashboard/HudSignalCharts.vue',
        selector: '.signal-chart'
      },
      {
        componentUrl: '../components/dashboard/operator/OperatorDecisionEvidencePanel.vue',
        selector: '.decision-chart'
      },
      {
        componentUrl: '../components/dashboard/operator/OperatorFutureStackPanel.vue',
        selector: '.future-chart'
      }
    ]

    for (const chartSurface of chartSurfaces) {
      const component = readFileSync(fileURLToPath(new URL(chartSurface.componentUrl, import.meta.url)), 'utf8')

      expect(getSelectorBlock(component, chartSurface.selector)).toMatch(/(?:^|\n)\s*height:\s*(?:clamp|[0-9.]+rem)/)
    }
  })

  it('keeps future-stack policy value copy scoped to DAM delivery review', () => {
    const futurePanel = readFileSync(
      fileURLToPath(new URL('../components/dashboard/operator/OperatorFutureStackPanel.vue', import.meta.url)),
      'utf8'
    )

    expect(futurePanel).toContain('DAM delivery schedule review')
    expect(futurePanel).toContain('DAM delivery-hour')
    expect(futurePanel).toContain('no live IDM bid or market submission')
    expect(futurePanel).toContain('Selected DAM net power')
    expect(futurePanel).toContain('DAM price context')
    expect(futurePanel).not.toContain('Selected strategy schedule')
  })
})
