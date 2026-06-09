import { describe, expect, it } from 'vitest'
import {
  approxTokens,
  expectedOperatorHudPartials,
  getSelectorBlock,
  readOperatorHudCss,
  readOperatorHudPartial,
  readOperatorHudRootCss
} from './test-fixtures/operatorHudTestFixtures'

describe('operator HUD CSS', () => {
  it('keeps operator HUD styles split into bounded cascade partials', () => {
    const rootCss = readOperatorHudRootCss()

    expect(rootCss.trim().split(/\r?\n/)).toEqual([
      '/* Operator HUD cascade entrypoint. Keep imports ordered from shell to responsive overrides. */',
      ...expectedOperatorHudPartials.map(partial => `@import "${partial}";`)
    ])

    for (const partial of expectedOperatorHudPartials) {
      const partialCss = readOperatorHudPartial(partial)

      expect(approxTokens(partialCss), `${partial} should stay below 5000 approx tokens`).toBeLessThan(5000)
    }
  })

  it('keeps the operator shell overflow visible so hover explanations are not clipped', () => {
    const css = readOperatorHudCss()

    expect(getSelectorBlock(css, '.operator-frame')).toMatch(/overflow:\s*visible/)
    expect(getSelectorBlock(css, '.operator-main-stage')).toMatch(/overflow:\s*visible/)
    expect(getSelectorBlock(css, '.operator-frame .surface-panel')).toMatch(/overflow:\s*visible/)
  })

  it('adds responsive and contrast preference hooks to the operator HUD shell', () => {
    const css = readOperatorHudCss()

    expect(getSelectorBlock(css, '.operator-shell')).toMatch(/display:\s*block/)
    expect(getSelectorBlock(css, '.operator-frame')).toMatch(/display:\s*grid/)
    expect(getSelectorBlock(css, '.operator-frame')).toMatch(/width:\s*100%/)
    expect(getSelectorBlock(css, '.operator-frame')).toMatch(/container-type:\s*inline-size/)
    expect(getSelectorBlock(css, '.metric-tooltip')).toMatch(/visibility:\s*hidden/)
    expect(css).toContain('visibility: visible')
    expect(css).toContain('@container operator-shell')
    expect(css).toContain('@media (prefers-contrast: more)')
    expect(css).toContain('scrollbar-color')
  })

  it('scopes reduced-motion overrides to the operator HUD instead of global universal selectors', () => {
    const css = readOperatorHudCss()

    expect(css).toContain('@media (prefers-reduced-motion: reduce)')
    expect(css).not.toMatch(/@media\s*\(prefers-reduced-motion:\s*reduce\)\s*\{\s*\*,\s*\*::before,\s*\*::after/)
    expect(css).toContain('.operator-frame,')
    expect(css).toContain('.operator-frame *::after')
    expect(css).toContain('animation: none !important')
  })
})
