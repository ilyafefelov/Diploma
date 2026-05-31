import { describe, expect, it } from 'vitest'

import { readDashboardFixture } from './test-fixtures/operatorHudTestFixtures'

describe('operator research panel architecture', () => {
  it('keeps research panel visual colors behind operator design tokens', () => {
    const researchPanel = readDashboardFixture('../components/dashboard/operator/OperatorResearchPanel.vue')
    const tokenCss = readDashboardFixture('../assets/css/design-tokens.css')

    for (const token of [
      '--operator-success-gradient-top',
      '--operator-success-gradient-bottom',
      '--operator-research-card-border',
      '--operator-research-card-gradient-top',
      '--operator-research-card-gradient-bottom',
      '--operator-tone-green-gradient-top',
      '--operator-tone-green-gradient-bottom',
      '--operator-tone-orange-gradient-top',
      '--operator-tone-orange-gradient-bottom',
      '--operator-tone-mint-gradient-top',
      '--operator-tone-mint-gradient-bottom',
      '--operator-tone-lime-gradient-top',
      '--operator-tone-lime-gradient-bottom',
      '--operator-tone-purple-gradient-top',
      '--operator-tone-purple-gradient-bottom',
      '--operator-tooltip-border',
      '--operator-tooltip-gradient-top',
      '--operator-tooltip-gradient-bottom',
      '--operator-tooltip-shadow',
      '--operator-control-foreground',
      '--operator-text-muted',
      '--operator-text-body',
      '--operator-text-bright',
      '--operator-text-bright-muted',
      '--operator-accent',
      '--operator-positive'
    ]) {
      expect(tokenCss, `${token} should live in design tokens`).toContain(`${token}:`)
      expect(researchPanel, `${token} should be consumed by research panel styles`).toContain(`var(${token})`)
    }

    for (const rawColor of [
      'rgba(255, 255, 255',
      'rgba(13, 151, 218',
      'rgba(6, 82, 147',
      'rgba(229, 249, 255',
      'rgba(52, 164, 28',
      'rgba(22, 101, 34',
      'rgba(236, 134, 14',
      'rgba(166, 74, 5',
      'rgba(31, 180, 185',
      'rgba(13, 105, 132',
      'rgba(82, 178, 35',
      'rgba(36, 111, 28',
      'rgba(124, 58, 237',
      'rgba(76, 29, 149',
      'rgba(202, 249, 255',
      'rgba(0, 129, 204',
      'rgba(0, 56, 112',
      'rgba(238, 250, 255',
      'rgba(0, 39, 82',
      '#85ef41',
      '#2b9b18',
      '#b8ff32',
      '#d7ff4f',
      '#ffffff',
      'color: white'
    ]) {
      expect(researchPanel, `${rawColor} should stay inside design tokens`).not.toContain(rawColor)
    }
  })
})
