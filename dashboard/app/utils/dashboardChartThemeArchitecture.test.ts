// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const barrelPath = fileURLToPath(new URL('./dashboardChartTheme.ts', import.meta.url))
const tenantRegistryScatterPath = fileURLToPath(
  new URL('../components/dashboard/TenantRegistryScatter.vue', import.meta.url)
)
const moduleUrls = [
  '../lib/charts/dashboardChartCore.ts',
  '../lib/charts/dashboardTenantChart.ts',
  '../lib/charts/dashboardSignalChartOptions.ts',
  '../lib/charts/dashboardSignalMarketPulseChart.ts',
  '../lib/charts/dashboardSignalDispatchChart.ts',
  '../lib/charts/dashboardBaselineChartOptions.ts',
  '../lib/charts/dashboardMarketSignalHeroChart.ts'
]

const rawColorPattern = /#[0-9a-fA-F]{3,8}\b|rgba\(|hsla\(|\bcolor:\s*(?:white|black)\b/
const approxTokens = (text: string): number => Math.ceil(text.length / 4)

describe('dashboard chart theme architecture', () => {
  it('keeps the legacy chart-theme import path as a bounded barrel', () => {
    const barrel = readFileSync(barrelPath, 'utf8')

    expect(approxTokens(barrel), 'dashboardChartTheme.ts should stay below 5000 approx tokens').toBeLessThan(5000)
    expect(barrel).toContain('dashboardChartCore')
    expect(barrel).toContain('dashboardTenantChart')
    expect(barrel).toContain('dashboardSignalChartOptions')
    expect(barrel).toContain('dashboardBaselineChartOptions')
    expect(barrel).toContain('dashboardMarketSignalHeroChart')
  })

  it('keeps signal chart option path as a thin public barrel', () => {
    const signalBarrelPath = fileURLToPath(new URL('../lib/charts/dashboardSignalChartOptions.ts', import.meta.url))
    const source = readFileSync(signalBarrelPath, 'utf8')

    expect(approxTokens(source), 'dashboardSignalChartOptions.ts should stay thin').toBeLessThan(700)
    expect(source).toContain('dashboardSignalMarketPulseChart')
    expect(source).toContain('dashboardSignalDispatchChart')
    expect(source).not.toContain('buildMarketPulseChartOption =')
    expect(source).not.toContain('buildSelectedStrategyDispatchChartOption =')
  })

  it('keeps chart option modules present and bounded', () => {
    for (const moduleUrl of moduleUrls) {
      const modulePath = fileURLToPath(new URL(moduleUrl, import.meta.url))

      expect(existsSync(modulePath), `${moduleUrl} should exist`).toBe(true)

      const moduleSource = readFileSync(modulePath, 'utf8')

      expect(approxTokens(moduleSource), `${moduleUrl} should stay below 5000 approx tokens`).toBeLessThan(5000)
    }
  })

  it('keeps tenant registry chart chrome token-driven', () => {
    const component = readFileSync(tenantRegistryScatterPath, 'utf8')

    expect(component).toContain('var(--panel-strong)')
    expect(component).toContain('color-mix(in oklab')
    expect(component).not.toMatch(rawColorPattern)
  })
})
