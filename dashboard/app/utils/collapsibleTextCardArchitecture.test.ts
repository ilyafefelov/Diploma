// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const collapsibleTextCardPath = fileURLToPath(
  new URL('../components/dashboard/CollapsibleTextCard.vue', import.meta.url)
)

const rawColorPattern = /#[0-9a-fA-F]{3,8}\b|rgba\(|hsla\(|\bcolor:\s*(?:white|black)\b/

describe('collapsible text card architecture', () => {
  it('keeps the shared collapsible card primitive token-driven', () => {
    const component = readFileSync(collapsibleTextCardPath, 'utf8')

    expect(component).toContain('var(--operator-card-gradient-top)')
    expect(component).toContain('var(--panel-strong)')
    expect(component).toContain('color-mix(in oklab')
    expect(component).not.toMatch(rawColorPattern)
  })
})
