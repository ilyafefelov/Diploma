import { describe, expect, it } from 'vitest'

import { readDashboardFixture } from './test-fixtures/operatorHudTestFixtures'

const marketVisualTokens = [
  '--operator-card-gradient-top',
  '--operator-card-gradient-bottom',
  '--operator-control-button-primary-gradient-top',
  '--operator-control-button-primary-gradient-bottom',
  '--operator-success-gradient-top',
  '--operator-success-gradient-bottom',
  '--operator-tooltip-gradient-top',
  '--operator-tooltip-gradient-bottom',
  '--operator-line-dim',
  '--operator-surface-soft',
  '--operator-control-foreground',
  '--operator-accent'
]

describe('operator market architecture', () => {
  it('keeps market visual colors behind design tokens', () => {
    const marketCss = readDashboardFixture('../assets/css/operator-hud.market.css')
    const tokenCss = readDashboardFixture('../assets/css/design-tokens.css')

    for (const token of marketVisualTokens) {
      expect(tokenCss, `${token} should live in design tokens`).toContain(`${token}:`)
      expect(marketCss, `${token} should be consumed by market styles`).toContain(`var(${token})`)
    }
  })

  it('keeps raw visual color literals out of the market partial', () => {
    const marketCss = readDashboardFixture('../assets/css/operator-hud.market.css')

    expect(marketCss).not.toMatch(/#[0-9a-fA-F]{3,8}\b/)
    expect(marketCss).not.toContain('rgba(')
    expect(marketCss).not.toMatch(/\bcolor:\s*(?:white|black)\b/)
  })
})
