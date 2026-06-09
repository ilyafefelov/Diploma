import { describe, expect, it } from 'vitest'

import {
  getSelectorBlock,
  readDashboardFixture,
  readOperatorHudCss
} from './test-fixtures/operatorHudTestFixtures'

describe('operator HUD scroll architecture', () => {
  it('keeps schedule dock in normal flow with one-row horizontal schedule scrolling', () => {
    const css = readOperatorHudCss()

    expect(getSelectorBlock(css, '.operator-shell')).not.toMatch(/padding-bottom:\s*(?:12|15\.5)rem/)
    expect(getSelectorBlock(css, '.operator-shell')).not.toMatch(/overflow-x:\s*clip/)
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
    const css = readOperatorHudCss()
    const scheduleDock = readDashboardFixture('../components/dashboard/operator/OperatorScheduleDock.vue')

    expect(scheduleDock).toContain('schedule-dock__floating-tooltip')
    expect(scheduleDock).not.toContain('class="schedule-tooltip"')
    expect(getSelectorBlock(css, '.schedule-track')).not.toMatch(/overflow-y:\s*hidden/)
    expect(getSelectorBlock(css, '.schedule-dock__floating-tooltip')).toMatch(/position:\s*fixed/)
  })

  it('clips the tenant map visual without creating a nested scroll trap', () => {
    const tenantMapCss = readDashboardFixture('../assets/css/operator-tenant-map.css')
    const mapSurfaceBlock = getSelectorBlock(tenantMapCss, '.tenant-card__ukraine-map-surface')

    expect(mapSurfaceBlock).toMatch(/overflow:\s*clip/)
    expect(mapSurfaceBlock).not.toMatch(/overflow:\s*hidden/)
  })

  it('keeps the operator route in native document scroll flow', () => {
    const operatorPage = readDashboardFixture('../pages/operator.vue')
    const css = readOperatorHudCss()
    const shellBlock = getSelectorBlock(css, '.operator-shell')

    expect(operatorPage).toContain('class="operator-shell"')
    expect(operatorPage).toContain('id="operator-content"')
    expect(operatorPage).toContain('<main')
    expect(operatorPage).not.toContain('<UDashboardGroup')
    expect(shellBlock).toMatch(/min-height:\s*100dvh/)
    expect(shellBlock).not.toMatch(/(?:^|\n)\s*height:\s*100(?:dvh|vh)/)
    expect(shellBlock).toMatch(/overflow:\s*visible/)
    expect(shellBlock).not.toMatch(/overflow-x:\s*(?:auto|scroll|hidden|clip)/)
    expect(shellBlock).not.toMatch(/overflow-y:\s*(?:auto|scroll|hidden|clip)/)
  })

  it('lets document scroll gestures pass through read-only chart canvases', () => {
    const clientChart = readDashboardFixture('../components/dashboard/ClientVChart.vue')
    const css = readOperatorHudCss()
    const chartShellBlock = getSelectorBlock(clientChart, '.client-v-chart-shell')

    expect(clientChart).toContain('touch-action: pan-y')
    expect(chartShellBlock).toContain('pointer-events: none')
    expect(clientChart).toContain('pointer-events: none')
    expect(getSelectorBlock(css, '.chart-fallback')).toContain('touch-action: pan-y')
    expect(getSelectorBlock(css, '.chart-fallback')).toContain('pointer-events: none')
  })
})
