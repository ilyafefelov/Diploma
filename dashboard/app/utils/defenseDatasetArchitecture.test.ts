// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const barrelPath = fileURLToPath(new URL('./defenseDataset.ts', import.meta.url))
const moduleDirectoryUrl = new URL('../lib/defense-dataset/', import.meta.url)
const expectedModules = [
  'defenseDatasetTypes.ts',
  'defenseDatasetConstants.ts',
  'defenseDatasetHeadlineConstants.ts',
  'defenseDatasetTftEvidenceConstants.ts',
  'defenseDatasetNarrativeConstants.ts',
  'defenseDatasetBuilders.ts'
]

const approxTokens = (text: string): number => Math.ceil(text.length / 4)

describe('defense dataset architecture', () => {
  it('keeps the public utility as a thin stable export surface', () => {
    const barrel = readFileSync(barrelPath, 'utf8')

    expect(approxTokens(barrel), 'defenseDataset.ts should stay below 5000 approx tokens').toBeLessThan(5000)
    expect(barrel).toContain('../lib/defense-dataset/defenseDatasetTypes')
    expect(barrel).toContain('../lib/defense-dataset/defenseDatasetConstants')
    expect(barrel).toContain('../lib/defense-dataset/defenseDatasetBuilders')
    expect(barrel).not.toContain('CURRENT_BILINGUAL_STRATEGY_EXPLAINER')
    expect(barrel).not.toContain('const modelRole')
  })

  it('keeps defense evidence data and builders in bounded implementation modules', () => {
    for (const moduleName of expectedModules) {
      const modulePath = fileURLToPath(new URL(moduleName, moduleDirectoryUrl))

      expect(existsSync(modulePath), `${moduleName} should exist`).toBe(true)

      const source = readFileSync(modulePath, 'utf8')
      expect(approxTokens(source), `${moduleName} should stay below 5000 approx tokens`).toBeLessThan(5000)
    }
  })

  it('keeps the defense constants module as a stable thin interface', () => {
    const constantsBarrelPath = fileURLToPath(new URL('defenseDatasetConstants.ts', moduleDirectoryUrl))
    const constantsBarrel = readFileSync(constantsBarrelPath, 'utf8')

    expect(approxTokens(constantsBarrel), 'defenseDatasetConstants.ts should stay thin').toBeLessThan(500)
    expect(constantsBarrel).toContain('./defenseDatasetHeadlineConstants')
    expect(constantsBarrel).toContain('./defenseDatasetTftEvidenceConstants')
    expect(constantsBarrel).toContain('./defenseDatasetNarrativeConstants')
    expect(constantsBarrel).not.toContain('CURRENT_BILINGUAL_STRATEGY_EXPLAINER:')
    expect(constantsBarrel).not.toContain('CURRENT_TFT_SAFE_SELECTION_EXPLAINER:')
  })
})
