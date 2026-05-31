// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const pagePath = fileURLToPath(new URL('../pages/defense.vue', import.meta.url))
const cssDirectoryUrl = new URL('../assets/css/', import.meta.url)
const componentDirectoryUrl = new URL('../components/defense/', import.meta.url)
const expectedDefenseStylePartials = [
  'defense.shell.css',
  'defense.pipeline.css',
  'defense.research.css',
  'defense.charts.css',
  'defense.read-model.css',
  'defense.responsive.css'
]
const expectedDefenseComponents = [
  'DefenseTopbar.vue',
  'DefenseHeroSection.vue',
  'DefenseNarrativeBand.vue',
  'DefensePipelineVisualPanel.vue',
  'DefenseBilingualExplainerPanel.vue',
  'DefenseOfflinePromotionPanel.vue',
  'DefenseEvidenceChartsPanel.vue',
  'DefenseLatestExperimentPanel.vue',
  'DefenseDtShadowPanel.vue',
  'DefenseBenchmarkContextSection.vue',
  'DefenseForecastEvidenceSection.vue',
  'DefenseForecastDiagnosticsSection.vue',
  'DefenseLiveContextSection.vue'
]

const approxTokens = (text: string): number => Math.ceil(text.length / 4)
const rawColorPattern = /#[0-9a-fA-F]{3,8}\b|rgba\(|hsla\(|\bcolor:\s*(?:white|black)\b/

describe('defense page architecture', () => {
  it('keeps the route file as a bounded orchestrator', () => {
    const page = readFileSync(pagePath, 'utf8')

    expect(approxTokens(page), 'defense.vue should stay below 5000 approx tokens').toBeLessThan(5000)

    for (const component of expectedDefenseComponents) {
      const componentPath = fileURLToPath(new URL(component, componentDirectoryUrl))
      expect(existsSync(componentPath), `${component} should exist`).toBe(true)

      const componentSource = readFileSync(componentPath, 'utf8')
      expect(approxTokens(componentSource), `${component} should stay below 5000 approx tokens`).toBeLessThan(5000)
    }

    expect(page).toContain('<DefenseHeroSection')
    expect(page).toContain('<DefenseEvidenceChartsPanel')
    expect(page).toContain('<DefenseLiveContextSection')
  })

  it('keeps defense styles in bounded external files', () => {
    const page = readFileSync(pagePath, 'utf8')

    for (const partial of expectedDefenseStylePartials) {
      expect(page).toContain(`<style src="../assets/css/${partial}"></style>`)

      const partialPath = fileURLToPath(new URL(partial, cssDirectoryUrl))
      expect(existsSync(partialPath), `${partial} should exist`).toBe(true)

      const partialCss = readFileSync(partialPath, 'utf8')
      expect(approxTokens(partialCss), `${partial} should stay below 5000 approx tokens`).toBeLessThan(5000)
    }

    expect(page).not.toContain('<style scoped>\n.defense-shell')
    expect(page).not.toContain('<style scoped src="../assets/css/defense.')
  })

  it('keeps the page inside the app landmark and prevents idle tooltip overflow', () => {
    const page = readFileSync(pagePath, 'utf8')
    const shellCss = readFileSync(fileURLToPath(new URL('defense.shell.css', cssDirectoryUrl)), 'utf8')

    expect(page).toContain('<div class="defense-shell">')
    expect(page).not.toContain('<main class="defense-shell">')
    expect(shellCss).toMatch(/\.defense-tooltip\s*{[\s\S]*display: none;/)
    expect(shellCss).toMatch(/\.table-help:focus-visible \.defense-tooltip\s*{[\s\S]*display: grid;/)
  })

  it('keeps defense shell chrome token-driven and viewport-safe', () => {
    const shellCss = readFileSync(fileURLToPath(new URL('defense.shell.css', cssDirectoryUrl)), 'utf8')

    expect(shellCss).toContain('min-height: 100dvh')
    expect(shellCss).toContain('var(--ink-strong)')
    expect(shellCss).toContain('var(--panel-strong)')
    expect(shellCss).toContain('var(--line-soft)')
    expect(shellCss).toContain('color-mix(in oklab')
    expect(shellCss).not.toMatch(rawColorPattern)
  })

  it('keeps defense chart chrome token-driven for contrast and theme consistency', () => {
    const chartCss = readFileSync(fileURLToPath(new URL('defense.charts.css', cssDirectoryUrl)), 'utf8')

    expect(chartCss).toContain('--defense-chart-info')
    expect(chartCss).toContain('--defense-chart-success')
    expect(chartCss).toContain('--defense-chart-warning')
    expect(chartCss).toContain('var(--defense-border)')
    expect(chartCss).toContain('var(--defense-panel)')
    expect(chartCss).toContain('color-mix(in oklab')
    expect(chartCss).not.toMatch(rawColorPattern)
  })

  it('keeps defense read-model and research surfaces token-driven', () => {
    const readModelCss = readFileSync(fileURLToPath(new URL('defense.read-model.css', cssDirectoryUrl)), 'utf8')
    const researchCss = readFileSync(fileURLToPath(new URL('defense.research.css', cssDirectoryUrl)), 'utf8')

    expect(readModelCss).toContain('var(--defense-text-muted)')
    expect(readModelCss).toContain('var(--defense-accent)')
    expect(readModelCss).toContain('var(--defense-border)')
    expect(readModelCss).not.toMatch(rawColorPattern)

    expect(researchCss).toContain('--defense-research-purple')
    expect(researchCss).toContain('var(--defense-border)')
    expect(researchCss).toContain('color-mix(in oklab')
    expect(researchCss).not.toMatch(rawColorPattern)
  })

  it('keeps defense pipeline visuals token-driven across hero, bilingual, and experiment cards', () => {
    const pipelineCss = readFileSync(fileURLToPath(new URL('defense.pipeline.css', cssDirectoryUrl)), 'utf8')

    expect(pipelineCss).toContain('--defense-pipeline-hero-surface')
    expect(pipelineCss).toContain('--defense-pipeline-success')
    expect(pipelineCss).toContain('--defense-pipeline-warning')
    expect(pipelineCss).toContain('--defense-pipeline-muted-tint')
    expect(pipelineCss).toContain('var(--defense-border)')
    expect(pipelineCss).toContain('var(--defense-panel)')
    expect(pipelineCss).toContain('color-mix(in oklab')
    expect(pipelineCss).not.toMatch(rawColorPattern)
  })
})
