import { describe, expect, it } from 'vitest'

import { readDashboardFixture } from './test-fixtures/operatorHudTestFixtures'

describe('operator weather controls architecture', () => {
  it('keeps weather controls visual colors behind operator design tokens', () => {
    const weatherPanel = readDashboardFixture('../components/dashboard/operator/OperatorWeatherControlsPanel.vue')
    const railCss = readDashboardFixture('../assets/css/operator-hud.rail.css')
    const tokenCss = readDashboardFixture('../assets/css/design-tokens.css')
    const getSelectorBlock = (css: string, selector: string): string => {
      const escapedSelector = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      const matches = [...css.matchAll(new RegExp(`${escapedSelector}\\s*\\{(?<body>[^}]*)\\}`, 'gm'))]

      return matches.map(match => match.groups?.body ?? '').join('\n')
    }
    const sharedControlStyles = [
      '.check-toggle',
      '.check-toggle input',
      '.control-meta',
      '.control-button',
      '.control-button-primary',
      '.control-button-green',
      '.control-button-secondary'
    ].map(selector => getSelectorBlock(railCss, selector)).join('\n')

    for (const token of [
      '--operator-tooltip-border',
      '--operator-tooltip-gradient-top',
      '--operator-tooltip-gradient-bottom',
      '--operator-tooltip-shadow',
      '--operator-text-bright',
      '--operator-text-body',
      '--operator-line-soft',
      '--operator-surface-soft',
      '--operator-accent',
      '--operator-control-button-border',
      '--operator-control-button-primary-gradient-top',
      '--operator-control-button-primary-gradient-bottom',
      '--operator-control-button-primary-shadow',
      '--operator-control-button-success-gradient-top',
      '--operator-control-button-success-gradient-bottom',
      '--operator-control-button-success-shadow',
      '--operator-control-button-danger-gradient-top',
      '--operator-control-button-danger-gradient-bottom',
      '--operator-control-button-danger-shadow'
    ]) {
      expect(tokenCss, `${token} should live in design tokens`).toContain(`${token}:`)
    }

    for (const token of [
      '--operator-tooltip-border',
      '--operator-tooltip-gradient-top',
      '--operator-tooltip-gradient-bottom',
      '--operator-tooltip-shadow',
      '--operator-text-bright',
      '--operator-text-body',
      '--operator-line-soft',
      '--operator-surface-soft',
      '--operator-accent'
    ]) {
      expect(weatherPanel, `${token} should be consumed by weather controls styles`).toContain(`var(${token})`)
    }

    for (const token of [
      '--operator-control-button-border',
      '--operator-control-button-primary-gradient-top',
      '--operator-control-button-primary-gradient-bottom',
      '--operator-control-button-primary-shadow',
      '--operator-control-button-success-gradient-top',
      '--operator-control-button-success-gradient-bottom',
      '--operator-control-button-success-shadow',
      '--operator-control-button-danger-gradient-top',
      '--operator-control-button-danger-gradient-bottom',
      '--operator-control-button-danger-shadow',
      '--operator-control-foreground',
      '--operator-text-bright-muted',
      '--operator-text-muted',
      '--plumbob-green'
    ]) {
      expect(sharedControlStyles, `${token} should be consumed by shared weather rail controls`).toContain(`var(${token})`)
    }

    for (const rawColor of [
      'rgba(202, 249, 255',
      'rgba(0, 129, 204',
      'rgba(0, 56, 112',
      'rgba(238, 250, 255',
      'rgba(0, 39, 82',
      'rgba(255, 255, 255',
      'rgba(0, 61, 119',
      'rgba(229, 249, 255',
      '#d7ff4f',
      '#ffffff',
      'color: white'
    ]) {
      expect(weatherPanel, `${rawColor} should stay inside design tokens`).not.toContain(rawColor)
      expect(sharedControlStyles, `${rawColor} should stay inside design tokens`).not.toContain(rawColor)
    }
  })
})
