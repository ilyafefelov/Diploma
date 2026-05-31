// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const mainCssPath = fileURLToPath(new URL('../assets/css/main.css', import.meta.url))
const designTokensCssPath = fileURLToPath(new URL('../assets/css/design-tokens.css', import.meta.url))
const appVuePath = fileURLToPath(new URL('../app.vue', import.meta.url))
const designTokenModulePaths = [
  fileURLToPath(new URL('../assets/css/design-tokens.foundation.css', import.meta.url)),
  fileURLToPath(new URL('../assets/css/design-tokens.operator-core.css', import.meta.url)),
  fileURLToPath(new URL('../assets/css/design-tokens.operator-metrics.css', import.meta.url)),
  fileURLToPath(new URL('../assets/css/design-tokens.operator-rail.css', import.meta.url)),
  fileURLToPath(new URL('../assets/css/design-tokens.operator-map.css', import.meta.url))
]

const readMainCss = (): string => readFileSync(mainCssPath, 'utf8')
const readDesignTokensCss = (): string => readFileSync(designTokensCssPath, 'utf8')
const readDesignTokenBundle = (): string => [
  readDesignTokensCss(),
  ...designTokenModulePaths.map(path => readFileSync(path, 'utf8'))
].join('\n')
const readAppShell = (): string => readFileSync(appVuePath, 'utf8')
const approxTokens = (text: string): number => Math.ceil(text.length / 4)
const rawColorPattern = /#[0-9a-fA-F]{3,8}\b|rgba\(|hsla\(|\bcolor:\s*(?:white|black)\b/

const getSelectorBlock = (css: string, selector: string): string => {
  const escapedSelector = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const matches = [...css.matchAll(new RegExp(`${escapedSelector}\\s*\\{(?<body>[^}]*)\\}`, 'gm'))]
  return matches.map(match => match.groups?.body ?? '').join('\n')
}

describe('dashboard global CSS', () => {
  it('keeps design tokens behind a dedicated stylesheet seam', () => {
    const css = readMainCss()
    const manifestCss = readDesignTokensCss()
    const tokenCss = readDesignTokenBundle()

    expect(css).toContain('@import "./design-tokens.css";')
    expect(css).not.toContain('@theme static')
    expect(css).not.toContain(':root {')
    expect(manifestCss).toContain('@import "./design-tokens.foundation.css";')
    expect(manifestCss).toContain('@import "./design-tokens.operator-core.css";')
    expect(manifestCss).toContain('@import "./design-tokens.operator-metrics.css";')
    expect(manifestCss).toContain('@import "./design-tokens.operator-rail.css";')
    expect(manifestCss).toContain('@import "./design-tokens.operator-map.css";')
    expect(manifestCss).not.toMatch(rawColorPattern)
    expect(approxTokens(manifestCss), 'design token manifest should stay tiny').toBeLessThan(100)
    expect(tokenCss).toContain('@theme static')
    expect(tokenCss).toContain(':root {')
    expect(tokenCss).toContain('--operator-accent-soft:')
    expect(tokenCss).toContain('--operator-line-subtle:')
    expect(tokenCss).toContain('@media (prefers-contrast: more)')
  })

  it('keeps design-token topic modules reviewable and present', () => {
    for (const path of designTokenModulePaths) {
      expect(existsSync(path), `${path} should exist`).toBe(true)
      expect(approxTokens(readFileSync(path, 'utf8')), `${path} should stay below 1500 approx tokens`)
        .toBeLessThan(1500)
    }
  })

  it('uses structural chrome instead of global decorative orb or bokeh backgrounds', () => {
    const css = readMainCss()
    const htmlBlock = getSelectorBlock(css, 'html')
    const bodyBeforeBlock = getSelectorBlock(css, 'body::before')

    expect(css).not.toContain('body::after')
    expect(htmlBlock).not.toContain('radial-gradient')
    expect(htmlBlock).toContain('color-mix(in oklab')
    expect(bodyBeforeBlock).not.toContain('radial-gradient')
    expect(bodyBeforeBlock).toContain('linear-gradient')
    expect(bodyBeforeBlock).toContain('background-size')
    expect(bodyBeforeBlock).toContain('color-mix(in oklab')
  })

  it('keeps global chrome color values behind the design-token seam', () => {
    const css = readMainCss()

    expect(css).toContain('var(--panel-strong)')
    expect(css).toContain('var(--accent-cyan)')
    expect(css).toContain('var(--line-soft)')
    expect(css).toContain('color-mix(in oklab')
    expect(css).not.toMatch(rawColorPattern)
  })

  it('defines global scroll, contrast, and keyboard focus affordances', () => {
    const css = readMainCss()
    const htmlBlock = getSelectorBlock(css, 'html')
    const bodyBlock = getSelectorBlock(css, 'body')
    const nuxtRootBlock = getSelectorBlock(css, '#__nuxt')

    expect(htmlBlock).toContain('color-scheme: light')
    expect(htmlBlock).toContain('accent-color: var(--native-accent)')
    expect(htmlBlock).toContain('overflow-y: auto')
    expect(htmlBlock).toContain('scrollbar-gutter: stable')
    expect(bodyBlock).toContain('margin: 0')
    expect(bodyBlock).not.toContain('overflow-y')
    expect(bodyBlock).not.toContain('overflow: hidden')
    expect(nuxtRootBlock).toContain('min-height: 100dvh')
    expect(css).toContain('scrollbar-color')
    expect(css).toContain('@media (prefers-contrast: more)')
    expect(css).toContain(':focus-visible')
    expect(css).toContain('outline-offset')
  })

  it('declares the browser color-scheme before first paint', () => {
    const appShell = readAppShell()

    expect(appShell).toContain('name: \'color-scheme\'')
    expect(appShell).toContain('content: \'light\'')
  })
})
