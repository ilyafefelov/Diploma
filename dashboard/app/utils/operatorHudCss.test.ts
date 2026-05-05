// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const cssPath = fileURLToPath(new URL('../assets/css/operator-hud.css', import.meta.url))
const baselinePreviewPath = fileURLToPath(new URL('../components/dashboard/HudBaselinePreview.vue', import.meta.url))

const getSelectorBlock = (css: string, selector: string): string => {
  const escapedSelector = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const matches = [...css.matchAll(new RegExp(`${escapedSelector}\\s*\\{(?<body>[^}]*)\\}`, 'gm'))]
  return matches.map(match => match.groups?.body ?? '').join('\n')
}

describe('operator HUD CSS', () => {
  it('keeps the operator shell overflow visible so hover explanations are not clipped', () => {
    const css = readFileSync(cssPath, 'utf8')

    expect(getSelectorBlock(css, '.operator-frame')).toMatch(/overflow:\s*visible/)
    expect(getSelectorBlock(css, '.operator-main-stage')).toMatch(/overflow:\s*visible/)
    expect(getSelectorBlock(css, '.operator-frame .surface-panel')).toMatch(/overflow:\s*visible/)
  })

  it('keeps baseline preview from animating outside page width', () => {
    const baselinePreview = readFileSync(baselinePreviewPath, 'utf8')

    expect(baselinePreview).not.toContain('animation: slab-sheen')
    expect(baselinePreview).not.toContain('@keyframes slab-sheen')
  })

  it('keeps schedule dock fixed above all operator panels with one-row horizontal schedule scrolling', () => {
    const css = readFileSync(cssPath, 'utf8')

    expect(getSelectorBlock(css, '.schedule-dock')).toMatch(/position:\s*fixed/)
    expect(getSelectorBlock(css, '.schedule-dock')).toMatch(/bottom:\s*0\.75rem/)
    expect(getSelectorBlock(css, '.schedule-dock')).toMatch(/z-index:\s*250/)
    expect(getSelectorBlock(css, '.schedule-track')).toMatch(/display:\s*flex/)
    expect(getSelectorBlock(css, '.schedule-track')).toMatch(/overflow-x:\s*auto/)
    expect(getSelectorBlock(css, '.schedule-track')).toMatch(/flex-wrap:\s*nowrap/)
  })
})
