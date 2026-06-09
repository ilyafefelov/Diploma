// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const rootPath = fileURLToPath(new URL('../components/dashboard/HudBaselinePreview.vue', import.meta.url))
const baselineComponentUrls = [
  '../components/dashboard/baseline/HudBaselinePreviewHeader.vue',
  '../components/dashboard/baseline/HudBaselineMetricStrips.vue',
  '../components/dashboard/baseline/HudBaselineChartGrid.vue',
  '../components/dashboard/baseline/HudBaselineExplainerGrid.vue',
  '../components/dashboard/baseline/HudBaselinePlanningBoundary.vue'
]

const rawColorPattern = /#[0-9a-fA-F]{3,8}\b|rgba\(|hsla\(|\bcolor:\s*(?:white|black)\b/
const approxTokens = (text: string): number => Math.ceil(text.length / 4)

describe('HUD baseline preview architecture', () => {
  it('keeps the baseline preview root as a bounded orchestration facade', () => {
    const root = readFileSync(rootPath, 'utf8')

    expect(approxTokens(root), 'HudBaselinePreview.vue should stay below 5000 approx tokens').toBeLessThan(5000)
    expect(root).toContain('<HudBaselinePreviewHeader')
    expect(root).toContain('<HudBaselineMetricStrips')
    expect(root).toContain('<HudBaselineChartGrid')
    expect(root).toContain('<HudBaselineExplainerGrid')
    expect(root).toContain('<HudBaselinePlanningBoundary')
    expect(root).toContain('isExpanded = ref(false)')
    expect(root).toContain('var(--operator-topbar-gradient-top)')
    expect(root).toContain('color-mix(in oklab')
    expect(root).not.toMatch(rawColorPattern)
  })

  it('keeps extracted baseline preview modules present and bounded', () => {
    for (const componentUrl of baselineComponentUrls) {
      const componentPath = fileURLToPath(new URL(componentUrl, import.meta.url))

      expect(existsSync(componentPath), `${componentUrl} should exist`).toBe(true)

      const component = readFileSync(componentPath, 'utf8')

      expect(approxTokens(component), `${componentUrl} should stay below 5000 approx tokens`).toBeLessThan(5000)
    }
  })

  it('keeps baseline metric pills keyboard-readable with visible focus rings', () => {
    const metricStrips = readFileSync(
      fileURLToPath(new URL('../components/dashboard/baseline/HudBaselineMetricStrips.vue', import.meta.url)),
      'utf8'
    )

    expect(metricStrips).toContain('role="group"')
    expect(metricStrips).toContain(':aria-label="`Baseline economic metric ${item.label}: ${item.value}`"')
    expect(metricStrips).toContain(':aria-label="`Baseline feasible-plan metric ${item.label}: ${item.value}`"')
    expect(metricStrips).not.toContain('outline: none')
    expect(metricStrips).toMatch(/outline:\s*2px solid var\(--focus-ring\)/)
  })

  it('keeps baseline metric strips token-driven and reduced-motion aware', () => {
    const metricStrips = readFileSync(
      fileURLToPath(new URL('../components/dashboard/baseline/HudBaselineMetricStrips.vue', import.meta.url)),
      'utf8'
    )

    expect(metricStrips).toContain('var(--operator-card-gradient-top)')
    expect(metricStrips).toContain('var(--operator-positive)')
    expect(metricStrips).toContain('color-mix(in oklab')
    expect(metricStrips).toContain('@media (prefers-reduced-motion: reduce)')
    expect(metricStrips).not.toMatch(rawColorPattern)
  })

  it('keeps the baseline planning boundary token-driven', () => {
    const planningBoundary = readFileSync(
      fileURLToPath(new URL('../components/dashboard/baseline/HudBaselinePlanningBoundary.vue', import.meta.url)),
      'utf8'
    )

    expect(planningBoundary).toContain('var(--operator-card-gradient-top)')
    expect(planningBoundary).toContain('var(--ink-strong)')
    expect(planningBoundary).toContain('color-mix(in oklab')
    expect(planningBoundary).not.toMatch(rawColorPattern)
  })

  it('keeps the baseline chart grid token-driven', () => {
    const chartGrid = readFileSync(
      fileURLToPath(new URL('../components/dashboard/baseline/HudBaselineChartGrid.vue', import.meta.url)),
      'utf8'
    )

    expect(chartGrid).toContain('var(--operator-card-gradient-top)')
    expect(chartGrid).toContain('var(--operator-text-muted)')
    expect(chartGrid).toContain('color-mix(in oklab')
    expect(chartGrid).not.toMatch(rawColorPattern)
  })

  it('keeps the baseline preview header token-driven', () => {
    const header = readFileSync(
      fileURLToPath(new URL('../components/dashboard/baseline/HudBaselinePreviewHeader.vue', import.meta.url)),
      'utf8'
    )

    expect(header).toContain('var(--operator-card-gradient-top)')
    expect(header).toContain('var(--operator-accent-readable)')
    expect(header).toContain('color-mix(in oklab')
    expect(header).not.toMatch(rawColorPattern)
  })
})
