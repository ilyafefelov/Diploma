import { describe, expect, it } from 'vitest'

import { readDashboardFixture } from './test-fixtures/operatorHudTestFixtures'

const responsiveVisualTokens = [
  '--operator-status-pulse',
  '--operator-responsive-panel-border',
  '--operator-responsive-panel-gradient-top',
  '--operator-responsive-panel-gradient-bottom',
  '--operator-responsive-panel-shadow',
  '--operator-responsive-panel-inset-top',
  '--operator-responsive-panel-inset-bottom',
  '--operator-contrast-frame-border',
  '--operator-contrast-panel-border'
]

describe('operator responsive architecture', () => {
  it('keeps responsive visual colors behind design tokens', () => {
    const responsiveCss = readDashboardFixture('../assets/css/operator-hud.responsive.css')
    const tokenCss = readDashboardFixture('../assets/css/design-tokens.css')

    for (const token of responsiveVisualTokens) {
      expect(tokenCss, `${token} should live in design tokens`).toContain(`${token}:`)
      expect(responsiveCss, `${token} should be consumed by responsive styles`).toContain(`var(${token})`)
    }
  })

  it('keeps raw visual color literals out of the responsive partial', () => {
    const responsiveCss = readDashboardFixture('../assets/css/operator-hud.responsive.css')

    expect(responsiveCss).not.toMatch(/#[0-9a-fA-F]{3,8}\b/)
    expect(responsiveCss).not.toContain('rgba(')
    expect(responsiveCss).not.toMatch(/\bcolor:\s*(?:white|black)\b/)
  })
})
