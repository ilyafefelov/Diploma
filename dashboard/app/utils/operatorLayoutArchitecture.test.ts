import { describe, expect, it } from 'vitest'

import {
  getSelectorBlock,
  readDashboardFixture
} from './test-fixtures/operatorHudTestFixtures'

const layoutVisualTokens = [
  '--operator-card-border-strong',
  '--operator-rail-panel-accent-wash',
  '--operator-rail-panel-gradient-top',
  '--operator-rail-panel-gradient-bottom',
  '--operator-rail-panel-shadow',
  '--operator-rail-panel-inset',
  '--operator-control-foreground',
  '--operator-accent-soft',
  '--operator-accent',
  '--operator-accent-foreground',
  '--operator-rail-chip-highlight',
  '--operator-text-readable',
  '--operator-text-bright',
  '--operator-control-button-border',
  '--operator-rail-action-gradient-top',
  '--operator-rail-action-gradient-bottom',
  '--operator-topbar-chip-foreground',
  '--operator-rail-action-active-gradient-top',
  '--operator-rail-action-active-gradient-bottom',
  '--operator-rail-action-hover-shadow',
  '--operator-positive',
  '--operator-rail-tooltip-border-soft',
  '--operator-rail-tooltip-accent-wash',
  '--operator-rail-tooltip-gradient-top',
  '--operator-rail-tooltip-gradient-bottom',
  '--operator-rail-tooltip-text',
  '--operator-rail-chip-border',
  '--operator-rail-chip-surface',
  '--operator-control-surface-strong',
  '--operator-accent-faint'
]

describe('operator layout CSS architecture', () => {
  it('keeps sidebar and tenant layout visuals behind design tokens', () => {
    const layoutCss = readDashboardFixture('../assets/css/operator-hud.layout.css')
    const tokenCss = readDashboardFixture('../assets/css/design-tokens.css')

    for (const token of layoutVisualTokens) {
      expect(tokenCss, `${token} should live in design tokens`).toContain(`${token}:`)
      expect(layoutCss, `${token} should be consumed by layout styles`).toContain(`var(${token})`)
    }
  })

  it('keeps raw visual color literals out of the operator layout partial', () => {
    const layoutCss = readDashboardFixture('../assets/css/operator-hud.layout.css')

    expect(layoutCss).not.toMatch(/#[0-9a-fA-F]{3,8}\b/)
    expect(layoutCss).not.toContain('rgba(')
    expect(layoutCss).not.toContain('hsla(')
    expect(layoutCss).not.toMatch(/\bcolor:\s*(?:white|black)\b/)
  })

  it('isolates heavy main-stage panels for dense dashboard interaction performance', () => {
    const layoutCss = readDashboardFixture('../assets/css/operator-hud.layout.css')
    const containedPanelBlock = getSelectorBlock(layoutCss, '.operator-main-stage > .surface-panel')

    expect(containedPanelBlock).toContain('content-visibility: auto')
    expect(containedPanelBlock).toContain('contain-intrinsic-size:')
    expect(layoutCss).toContain('@supports not (content-visibility: auto)')
    expect(layoutCss).toContain('contain: layout style paint')
  })
})
