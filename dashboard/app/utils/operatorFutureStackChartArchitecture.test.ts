// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const barrelPath = fileURLToPath(new URL('./operatorFutureStackChartOptions.ts', import.meta.url))
const moduleDirectoryUrl = new URL('../lib/operator-future/chart-options/', import.meta.url)
const expectedModules = [
  'operatorFutureChartTypes.ts',
  'operatorFutureForecastChartOptions.ts',
  'operatorFuturePolicyChartOptions.ts',
  'operatorFutureStrategyChartOptions.ts'
]

const approxTokens = (text: string): number => Math.ceil(text.length / 4)

describe('operator future-stack chart option architecture', () => {
  it('keeps the public chart utility as a stable barrel', () => {
    const barrel = readFileSync(barrelPath, 'utf8')

    expect(approxTokens(barrel), 'operatorFutureStackChartOptions.ts should be thin').toBeLessThan(700)
    expect(barrel).toContain('operatorFutureChartTypes')
    expect(barrel).toContain('operatorFutureForecastChartOptions')
    expect(barrel).toContain('operatorFuturePolicyChartOptions')
    expect(barrel).toContain('operatorFutureStrategyChartOptions')
    expect(barrel).not.toContain('buildForecastOption(input')
    expect(barrel).not.toContain('buildPolicyOption(input')
    expect(barrel).not.toContain('buildStrategyComparisonOption(input')
  })

  it('stores chart option implementation in focused bounded modules', () => {
    for (const moduleName of expectedModules) {
      const modulePath = fileURLToPath(new URL(moduleName, moduleDirectoryUrl))

      expect(existsSync(modulePath), `${moduleName} should exist`).toBe(true)

      const source = readFileSync(modulePath, 'utf8')
      expect(approxTokens(source), `${moduleName} should stay below 5000 approx tokens`).toBeLessThan(5000)
    }
  })
})
