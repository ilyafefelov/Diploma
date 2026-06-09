// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'

import { describe, expect, it } from 'vitest'

const rawColorPattern = /#[0-9a-fA-F]{3,8}\b|rgba\(|hsla\(|\b(?:white|black)\b/

const chartOptionModules = [
  'dashboardBaselineChartOptions.ts',
  'dashboardMarketSignalHeroChart.ts',
  'dashboardSignalMarketPulseChart.ts',
  'dashboardSignalDispatchChart.ts',
  'dashboardTenantChart.ts',
  'operatorDecisionEvidenceChartOptions.ts'
]

const readChartFile = (fileName: string): string => {
  return readFileSync(fileURLToPath(new URL(fileName, import.meta.url)), 'utf8')
}

const approxTokens = (source: string): number => Math.ceil(source.length / 4)

describe('dashboard chart option architecture', () => {
  it('keeps chart visual literals behind the chart token seam', () => {
    for (const moduleName of chartOptionModules) {
      const source = readChartFile(moduleName)

      expect(source, `${moduleName} should use the shared chart token Module`).toContain('dashboardChartTokens')
      expect(source, `${moduleName} should not own raw chart color literals`).not.toMatch(rawColorPattern)
      expect(approxTokens(source), `${moduleName} should stay below 5000 approx tokens`).toBeLessThan(5000)
    }
  })

  it('keeps dashboardChartCore as the only chart color literal seam', () => {
    const core = readChartFile('dashboardChartCore.ts')

    expect(core).toContain('export const dashboardChartTokens')
    expect(core).toMatch(rawColorPattern)
  })
})
