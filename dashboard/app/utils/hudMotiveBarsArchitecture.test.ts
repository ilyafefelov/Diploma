// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const hudMotiveBarsPath = fileURLToPath(
  new URL('../components/dashboard/HudMotiveBars.vue', import.meta.url)
)

const rawColorPattern = /#[0-9a-fA-F]{3,8}\b|rgba\(|hsla\(|\bcolor:\s*(?:white|black)\b/

describe('HUD motive bars architecture', () => {
  it('keeps motive bar visuals token-driven and reduced-motion aware', () => {
    const component = readFileSync(hudMotiveBarsPath, 'utf8')

    expect(component).toContain('var(--operator-rail-mini-meter-blue-top)')
    expect(component).toContain('var(--operator-tooltip-gradient-top)')
    expect(component).toContain('@media (prefers-reduced-motion: reduce)')
    expect(component).not.toMatch(rawColorPattern)
  })
})
