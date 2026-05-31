import { describe, expect, it } from 'vitest'

import { readDashboardFixture } from './test-fixtures/operatorHudTestFixtures'

const decisionEvidenceVisualTokens = [
  '--operator-card-border',
  '--operator-card-accent-wash',
  '--operator-card-gradient-top',
  '--operator-card-gradient-bottom',
  '--operator-tone-green-gradient-top',
  '--operator-tone-green-gradient-bottom',
  '--operator-tone-orange-gradient-top',
  '--operator-tone-orange-gradient-bottom',
  '--operator-tone-mint-gradient-top',
  '--operator-tone-mint-gradient-bottom',
  '--operator-control-button-danger-gradient-top',
  '--operator-control-button-danger-gradient-bottom',
  '--operator-tooltip-gradient-top',
  '--operator-tooltip-gradient-bottom',
  '--operator-tooltip-shadow',
  '--operator-control-foreground'
]

describe('operator decision evidence CSS architecture', () => {
  it('keeps decision evidence visuals behind design tokens', () => {
    const css = readDashboardFixture('../assets/css/operator-decision-evidence.css')
    const tokens = readDashboardFixture('../assets/css/design-tokens.css')

    for (const token of decisionEvidenceVisualTokens) {
      expect(tokens, `${token} should live in design tokens`).toContain(`${token}:`)
      expect(css, `${token} should be consumed by decision evidence styles`).toContain(`var(${token})`)
    }
  })

  it('keeps raw visual color literals out of the decision evidence partial', () => {
    const css = readDashboardFixture('../assets/css/operator-decision-evidence.css')

    expect(css).not.toMatch(/#[0-9a-fA-F]{3,8}\b/)
    expect(css).not.toContain('rgba(')
    expect(css).not.toContain('hsla(')
    expect(css).not.toMatch(/\bcolor:\s*(?:white|black)\b/)
  })
})
