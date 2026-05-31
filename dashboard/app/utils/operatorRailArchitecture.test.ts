// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

import {
  approxTokens,
  readDashboardFixture
} from './test-fixtures/operatorHudTestFixtures'

const cssDirectoryUrl = new URL('../assets/css/', import.meta.url)

const expectedRailPartialUrls = [
  './operator-hud.rail-panels.css',
  './operator-hud.rail-metrics.css',
  './operator-hud.rail-gatekeeper.css',
  './operator-hud.rail-controls.css'
]

const railVisualTokens = [
  '--operator-rail-panel-highlight',
  '--operator-rail-panel-accent-wash',
  '--operator-rail-panel-gradient-top',
  '--operator-rail-panel-gradient-bottom',
  '--operator-rail-panel-shadow',
  '--operator-rail-mood-meter-red',
  '--operator-rail-mood-meter-yellow',
  '--operator-rail-mood-meter-lime',
  '--operator-rail-mood-meter-green',
  '--operator-rail-tooltip-border',
  '--operator-rail-tooltip-gradient-top',
  '--operator-rail-tooltip-gradient-bottom',
  '--operator-rail-action-gradient-top',
  '--operator-rail-action-gradient-bottom',
  '--operator-rail-action-active-gradient-top',
  '--operator-rail-action-active-gradient-bottom',
  '--operator-rail-regret-ring-core',
  '--operator-rail-regret-ring-tail',
  '--operator-rail-field-border',
  '--operator-rail-field-gradient-top',
  '--operator-rail-field-gradient-bottom',
  '--operator-rail-notes-card-gradient-top',
  '--operator-rail-notes-card-gradient-bottom',
  '--operator-rail-alert-gradient-top',
  '--operator-rail-alert-gradient-bottom'
]

const readRawRailCss = (): string => readFileSync(
  fileURLToPath(new URL('./operator-hud.rail.css', cssDirectoryUrl)),
  'utf8'
)

const readRawRailPartial = (partialUrl: string): string => readFileSync(
  fileURLToPath(new URL(partialUrl, cssDirectoryUrl)),
  'utf8'
)

describe('operator rail architecture', () => {
  it('keeps the rail stylesheet as a bounded topic manifest', () => {
    const railManifest = readRawRailCss()

    for (const partialUrl of expectedRailPartialUrls) {
      expect(railManifest).toContain(`@import "${partialUrl}";`)
    }

    expect(approxTokens(railManifest)).toBeLessThan(160)
  })

  it('keeps rail topic partials local and bounded', () => {
    for (const partialUrl of expectedRailPartialUrls) {
      const partialPath = fileURLToPath(new URL(partialUrl, cssDirectoryUrl))

      expect(existsSync(partialPath), `${partialUrl} should exist`).toBe(true)
      expect(approxTokens(readRawRailPartial(partialUrl)), `${partialUrl} should stay focused`).toBeLessThan(1_500)
    }
  })

  it('keeps rail visual colors behind design tokens', () => {
    const railCss = readDashboardFixture('../assets/css/operator-hud.rail.css')
    const tokenCss = readDashboardFixture('../assets/css/design-tokens.css')

    for (const token of railVisualTokens) {
      expect(tokenCss, `${token} should live in design tokens`).toContain(`${token}:`)
      expect(railCss, `${token} should be consumed by rail styles`).toContain(`var(${token})`)
    }
  })

  it('keeps raw visual color literals out of the rail partial', () => {
    const railCss = readDashboardFixture('../assets/css/operator-hud.rail.css')

    expect(railCss).not.toMatch(/#[0-9a-fA-F]{3,8}\b/)
    expect(railCss).not.toContain('rgba(')
    expect(railCss).not.toMatch(/\bcolor:\s*(?:white|black)\b/)
  })
})
