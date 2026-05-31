import { describe, expect, it } from 'vitest'

import { readDashboardFixture } from './test-fixtures/operatorHudTestFixtures'

const scheduleVisualTokens = [
  '--operator-schedule-dock-border',
  '--operator-schedule-dock-gradient-top',
  '--operator-schedule-dock-gradient-bottom',
  '--operator-schedule-dock-shadow',
  '--operator-schedule-dock-inset',
  '--operator-schedule-divider',
  '--operator-schedule-chip-surface',
  '--operator-schedule-segment-gradient-top',
  '--operator-schedule-segment-gradient-bottom',
  '--operator-schedule-segment-hover-shadow',
  '--operator-schedule-scrollbar-thumb',
  '--operator-schedule-scrollbar-track',
  '--operator-schedule-table-border',
  '--operator-schedule-table-surface',
  '--operator-schedule-table-row-surface',
  '--operator-schedule-table-empty-text'
]

describe('operator schedule dock architecture', () => {
  it('keeps schedule dock visual colors behind design tokens', () => {
    const scheduleCss = readDashboardFixture('../assets/css/operator-hud.schedule.css')
    const responsiveCss = readDashboardFixture('../assets/css/operator-hud.responsive.css')
    const tokenCss = readDashboardFixture('../assets/css/design-tokens.css')

    for (const token of scheduleVisualTokens) {
      expect(tokenCss, `${token} should live in design tokens`).toContain(`${token}:`)
      expect(scheduleCss, `${token} should be consumed by schedule dock styles`).toContain(`var(${token})`)
    }

    expect(responsiveCss).not.toContain('scrollbar-color: rgba(215, 255, 79')
    expect(responsiveCss).not.toContain('scrollbar-color: #d7ff4f #00376e')
    expect(responsiveCss).toContain('var(--operator-responsive-panel-gradient-top)')
    expect(responsiveCss).toContain('var(--operator-responsive-panel-gradient-bottom)')
    expect(responsiveCss).toContain('var(--operator-responsive-panel-shadow)')
    expect(responsiveCss).toContain('border-bottom: 1px solid var(--operator-schedule-divider)')
    expect(responsiveCss).not.toContain('rgba(0, 118, 178')
    expect(responsiveCss).not.toContain('rgba(0, 43, 96, 0.93')
  })

  it('keeps raw visual color literals out of the schedule dock partial', () => {
    const scheduleCss = readDashboardFixture('../assets/css/operator-hud.schedule.css')

    expect(scheduleCss).not.toMatch(/#[0-9a-fA-F]{3,8}\b/)
    expect(scheduleCss).not.toContain('rgba(')
    expect(scheduleCss).not.toMatch(/\bcolor:\s*(?:white|black)\b/)
  })
})
