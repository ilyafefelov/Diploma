// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'

import { describe, expect, it } from 'vitest'

const componentPath = (name: string): string => fileURLToPath(
  new URL(`../components/dashboard/operator/${name}`, import.meta.url)
)

const approxTokens = (text: string): number => Math.ceil(text.length / 4)
const rawColorPattern = /#[0-9a-fA-F]{3,8}\b|rgba\(|hsla\(|\bcolor:\s*(?:white|black)\b/

const scopedStyleBlock = (component: string): string => {
  const match = component.match(/<style scoped>\s*([\s\S]*?)\s*<\/style>/)
  return match?.[1] ?? ''
}

describe('operator sidebar architecture', () => {
  it('keeps the sidebar shell separate from the tenant map implementation', () => {
    const sidebar = readFileSync(componentPath('OperatorSidebar.vue'), 'utf8')
    const mapPath = componentPath('OperatorTenantMapCard.vue')

    expect(sidebar).toContain('<OperatorTenantMapCard')
    expect(sidebar).not.toContain('tenant-card__ukraine-map-surface')
    expect(sidebar).not.toContain('ukraineMapBounds')
    expect(approxTokens(sidebar)).toBeLessThan(5000)
    expect(existsSync(mapPath), 'OperatorTenantMapCard.vue should exist').toBe(true)
  })

  it('keeps root scroll recovery out of the sidebar implementation', () => {
    const sidebar = readFileSync(componentPath('OperatorSidebar.vue'), 'utf8')

    expect(sidebar).not.toContain('forwardRootWheel')
    expect(sidebar).not.toContain('@wheel.capture.prevent')
    expect(sidebar).not.toContain('window.scrollBy')
  })

  it('keeps sidebar scoped visuals token-driven instead of raw color literals', () => {
    const sidebar = readFileSync(componentPath('OperatorSidebar.vue'), 'utf8')
    const scopedStyles = scopedStyleBlock(sidebar)

    expect(scopedStyles).toContain('--operator-tenant-map-card-border')
    expect(scopedStyles).toContain('--operator-tenant-map-surface-top')
    expect(scopedStyles).toContain('--operator-tenant-map-surface-bottom')
    expect(scopedStyles).toContain('--operator-tenant-map-card-highlight')
    expect(scopedStyles).toContain('--operator-tenant-map-surface-glow')
    expect(scopedStyles).toContain('--operator-tenant-map-card-shadow')
    expect(scopedStyles).not.toMatch(rawColorPattern)
  })

  it('stores tenant map markup and styles behind focused modules', () => {
    const mapCard = readFileSync(componentPath('OperatorTenantMapCard.vue'), 'utf8')
    const mapCss = readFileSync(fileURLToPath(new URL('../assets/css/operator-tenant-map.css', import.meta.url)), 'utf8')

    expect(mapCard).toContain('<script setup lang="ts">')
    expect(mapCard).toContain('aria-label="Active tenant and sites on Ukraine map"')
    expect(mapCard).toContain('tenant-card__tenant-marker')
    expect(mapCard).toContain(':src="\'/design/ukraine-outline.svg\'"')
    expect(mapCard).toContain('<style scoped src="../../../assets/css/operator-tenant-map.css"></style>')
    expect(mapCss).toContain('.tenant-card__ukraine-map-surface')
    expect(approxTokens(mapCard)).toBeLessThan(5000)
    expect(approxTokens(mapCss)).toBeLessThan(5000)
  })
})
