import { describe, expect, it } from 'vitest'

import { readDashboardFixture } from './test-fixtures/operatorHudTestFixtures'

const shellVisualTokens = [
  '--operator-frame-border',
  '--operator-frame-gradient-top',
  '--operator-frame-gradient-bottom',
  '--operator-frame-accent-cyan',
  '--operator-frame-accent-green',
  '--operator-frame-shadow',
  '--operator-frame-inset',
  '--operator-frame-inner-border',
  '--operator-frame-scanline',
  '--operator-topbar-border',
  '--operator-topbar-accent-wash',
  '--operator-topbar-gradient-top',
  '--operator-topbar-gradient-bottom',
  '--operator-topbar-foreground',
  '--operator-topbar-shadow',
  '--operator-topbar-inset',
  '--operator-brand-orb-border',
  '--operator-brand-orb-highlight',
  '--operator-brand-orb-gradient-top',
  '--operator-brand-orb-gradient-bottom',
  '--operator-brand-orb-shadow',
  '--operator-brand-orb-inset',
  '--operator-brand-icon-shadow',
  '--operator-topbar-chip-border',
  '--operator-topbar-chip-surface',
  '--operator-topbar-chip-surface-hover',
  '--operator-topbar-chip-foreground',
  '--operator-status-pulse',
  '--operator-topbar-warning-border',
  '--operator-topbar-warning-surface',
  '--operator-metric-capsule-border',
  '--operator-metric-capsule-accent-wash',
  '--operator-metric-capsule-gradient-top',
  '--operator-metric-capsule-gradient-bottom',
  '--operator-metric-capsule-foreground',
  '--operator-metric-capsule-shadow',
  '--operator-metric-capsule-inset',
  '--operator-metric-capsule-hover-border',
  '--operator-metric-capsule-hover-shadow',
  '--operator-metric-capsule-hover-ring',
  '--operator-metric-capsule-hover-inset',
  '--operator-metric-capsule-focus-outline',
  '--operator-metric-icon-surface',
  '--operator-metric-label-text',
  '--operator-metric-value-text',
  '--operator-metric-value-shadow',
  '--operator-metric-meta-text',
  '--operator-metric-sparkline-stroke',
  '--operator-metric-sparkline-divider',
  '--operator-metric-sparkline-glow',
  '--operator-metric-tooltip-border',
  '--operator-metric-tooltip-accent-wash',
  '--operator-metric-tooltip-gradient-top',
  '--operator-metric-tooltip-gradient-bottom',
  '--operator-metric-tooltip-foreground',
  '--operator-metric-tooltip-shadow',
  '--operator-metric-tooltip-inset',
  '--operator-metric-tooltip-arrow-gradient-top',
  '--operator-metric-tooltip-arrow-gradient-bottom',
  '--operator-metric-tooltip-header-text',
  '--operator-metric-tooltip-gem-gradient-top',
  '--operator-metric-tooltip-gem-gradient-bottom',
  '--operator-metric-tooltip-gem-shadow',
  '--operator-metric-tooltip-body-text',
  '--operator-metric-tooltip-formula-text'
]

describe('operator shell architecture', () => {
  it('keeps shell, topbar, and metric ribbon colors behind design tokens', () => {
    const shellCss = readDashboardFixture('../assets/css/operator-hud.shell.css')
    const tokenCss = readDashboardFixture('../assets/css/design-tokens.css')

    for (const token of shellVisualTokens) {
      expect(tokenCss, `${token} should live in design tokens`).toContain(`${token}:`)
      expect(shellCss, `${token} should be consumed by shell styles`).toContain(`var(${token})`)
    }
  })

  it('keeps raw visual color literals out of the operator shell partial', () => {
    const shellCss = readDashboardFixture('../assets/css/operator-hud.shell.css')

    expect(shellCss).not.toMatch(/#[0-9a-fA-F]{3,8}\b/)
    expect(shellCss).not.toContain('rgba(')
    expect(shellCss).not.toMatch(/\bcolor:\s*(?:white|black)\b/)
  })
})
