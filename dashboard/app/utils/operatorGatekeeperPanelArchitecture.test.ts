import { describe, expect, it } from 'vitest'

import { readDashboardFixture } from './test-fixtures/operatorHudTestFixtures'

describe('operator gatekeeper panel architecture', () => {
  it('keeps gatekeeper status colors behind operator design tokens', () => {
    const gatekeeperPanel = readDashboardFixture('../components/dashboard/operator/OperatorGatekeeperPanel.vue')
    const tokenCss = readDashboardFixture('../assets/css/design-tokens.css')

    for (const token of [
      '--operator-accent',
      '--operator-control-foreground',
      '--operator-line-faint',
      '--operator-surface-wash',
      '--operator-text-muted',
      '--operator-text-readable'
    ]) {
      expect(tokenCss, `${token} should live in design tokens`).toContain(`${token}:`)
      expect(gatekeeperPanel, `${token} should be consumed by gatekeeper styles`).toContain(`var(${token})`)
    }

    for (const rawColor of [
      'rgba(229, 249, 255',
      'rgba(160, 226, 255',
      'rgba(0, 56, 106',
      '#c9ff3d',
      '#ffffff',
      'color: white'
    ]) {
      expect(gatekeeperPanel, `${rawColor} should stay inside design tokens`).not.toContain(rawColor)
    }
  })
})
