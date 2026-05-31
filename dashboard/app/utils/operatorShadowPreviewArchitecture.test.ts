// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const barrelPath = fileURLToPath(new URL('./operatorShadowPreview.ts', import.meta.url))
const moduleDirectoryUrl = new URL('../lib/operator-shadow-preview/', import.meta.url)
const operatorPagePath = fileURLToPath(new URL('../pages/operator.vue', import.meta.url))
const recommendationPreviewModelPath = fileURLToPath(
  new URL('../composables/useOperatorRecommendationPreviewModel.ts', import.meta.url)
)
const expectedModules = [
  'operatorShadowPreviewSources.ts',
  'operatorShadowPreviewAdapter.ts',
  'operatorShadowPreviewTables.ts'
]

const approxTokens = (text: string): number => Math.ceil(text.length / 4)

describe('operator shadow preview architecture', () => {
  it('keeps the public utility as a thin stable export surface', () => {
    const barrel = readFileSync(barrelPath, 'utf8')

    expect(approxTokens(barrel), 'operatorShadowPreview.ts should stay below 5000 approx tokens').toBeLessThan(5000)
    expect(barrel).toContain('../lib/operator-shadow-preview/operatorShadowPreviewSources')
    expect(barrel).toContain('../lib/operator-shadow-preview/operatorShadowPreviewAdapter')
    expect(barrel).toContain('../lib/operator-shadow-preview/operatorShadowPreviewTables')
    expect(barrel).not.toContain('minimalBaseRecommendation')
    expect(barrel).not.toContain('buildBestValidComparisonRow')
  })

  it('keeps shadow preview implementation modules bounded', () => {
    for (const moduleName of expectedModules) {
      const modulePath = fileURLToPath(new URL(moduleName, moduleDirectoryUrl))

      expect(existsSync(modulePath), `${moduleName} should exist`).toBe(true)

      const source = readFileSync(modulePath, 'utf8')
      expect(approxTokens(source), `${moduleName} should stay below 5000 approx tokens`).toBeLessThan(5000)
    }
  })

  it('keeps operator recommendation/shadow preview state behind a page-level model seam', () => {
    expect(existsSync(recommendationPreviewModelPath), 'useOperatorRecommendationPreviewModel.ts should exist').toBe(true)

    const page = readFileSync(operatorPagePath, 'utf8')
    const model = readFileSync(recommendationPreviewModelPath, 'utf8')

    expect(page).toContain('useOperatorRecommendationPreviewModel')
    expect(page).not.toContain('useOperatorRecommendation(')
    expect(page).not.toContain('useShadowRecommendationPreview(')
    expect(page).not.toContain('useShadowRecommendationComparison(')
    expect(model).toContain('adaptShadowPreviewToOperatorRecommendation')
    expect(model).toContain('buildOperatorHourlyRecommendationRows')
    expect(model).toContain('buildShadowHourlyRecommendationRows')
    expect(model).toContain('refreshVisibleRecommendation')
    expect(approxTokens(model), 'recommendation preview model should stay below 5000 approx tokens').toBeLessThan(5000)
  })
})
