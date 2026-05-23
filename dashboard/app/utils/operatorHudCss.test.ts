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

  it('keeps schedule dock fixed above all operator panels with one-row horizontal schedule scrolling', () => {
    const css = readFileSync(cssPath, 'utf8')

    expect(getSelectorBlock(css, '.schedule-dock')).toMatch(/position:\s*fixed/)
    expect(getSelectorBlock(css, '.schedule-dock')).toMatch(/bottom:\s*0\.75rem/)
    expect(getSelectorBlock(css, '.schedule-dock')).toMatch(/z-index:\s*250/)
    expect(getSelectorBlock(css, '.schedule-track')).toMatch(/display:\s*flex/)
    expect(getSelectorBlock(css, '.schedule-track')).toMatch(/overflow-x:\s*auto/)
    expect(getSelectorBlock(css, '.schedule-track')).toMatch(/flex-wrap:\s*nowrap/)
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

    expect(topBar).toContain('Operator Preview')
    expect(topBar).not.toContain('BESS Control')
    expect(marketConsole).toContain('DAM hourly planning preview')
    expect(marketConsole).not.toContain('DAM / IDM arbitrage surface')
    expect(marketSignalHero).toContain('DAM hourly')
    expect(marketSignalHero).toContain('IDM future')
    expect(marketSignalHero).not.toContain('label="IDM"')
    expect(marketSignalHero).not.toContain('label="Both"')
    expect(scheduleDock).toContain('Review mode')
    expect(scheduleDock).not.toContain('Dispatch mode')
    expect(signalCharts).toContain('Selected schedule and value preview')
    expect(signalCharts).toContain('Review context for selected preview')
    expect(signalCharts).not.toContain('Use now: context for selected preview')
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
})
