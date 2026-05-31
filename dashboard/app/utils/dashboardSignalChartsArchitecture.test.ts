// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const signalChartsPath = fileURLToPath(new URL('../components/dashboard/HudSignalCharts.vue', import.meta.url))
const signalChartsCssPath = fileURLToPath(new URL('../assets/css/hud-signal-charts.css', import.meta.url))
const marketExplainersPath = fileURLToPath(new URL('../components/dashboard/signal/HudSignalMarketExplainers.vue', import.meta.url))
const scheduleExplainersPath = fileURLToPath(new URL('../components/dashboard/signal/HudSignalScheduleExplainers.vue', import.meta.url))

const getSelectorBlock = (css: string, selector: string): string => {
  const escapedSelector = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const matches = [...css.matchAll(new RegExp(`${escapedSelector}\\s*\\{(?<body>[^}]*)\\}`, 'gm'))]
  return matches.map(match => match.groups?.body ?? '').join('\n')
}

const rawColorPattern = /#[0-9a-fA-F]{3,8}\b|rgba\(|hsla\(|\bcolor:\s*(?:white|black)\b/

describe('HUD signal charts architecture', () => {
  it('keeps signal-chart presentation CSS behind a bounded stylesheet', () => {
    const component = readFileSync(signalChartsPath, 'utf8')
    const stylesheet = readFileSync(signalChartsCssPath, 'utf8')

    expect(component).toContain('<style scoped src="../../assets/css/hud-signal-charts.css"></style>')
    expect(component).not.toContain('<style scoped>')
    expect(Math.ceil(stylesheet.length / 4)).toBeLessThan(5000)
    expect(getSelectorBlock(stylesheet, '.signal-chart')).toMatch(/height:\s*21rem/)
    expect(getSelectorBlock(stylesheet, '.signal-grid')).toMatch(/display:\s*grid/)
    expect(stylesheet).toContain('@media (min-width: 960px)')
  })

  it('keeps long signal explanation copy in focused modules', () => {
    const component = readFileSync(signalChartsPath, 'utf8')
    const marketExplainers = readFileSync(marketExplainersPath, 'utf8')
    const scheduleExplainers = readFileSync(scheduleExplainersPath, 'utf8')

    expect(component).toContain('<HudSignalMarketExplainers')
    expect(component).toContain('<HudSignalScheduleExplainers')
    expect(component).not.toContain('How the selected market context price is calculated')
    expect(component).not.toContain('How value gap is calculated now')
    expect(Math.ceil(component.length / 4), 'HudSignalCharts.vue should stay focused').toBeLessThan(2500)
    expect(marketExplainers).toContain('How the selected market context price is calculated')
    expect(scheduleExplainers).toContain('How value gap is calculated now')
    expect(Math.ceil(marketExplainers.length / 4)).toBeLessThan(1800)
    expect(Math.ceil(scheduleExplainers.length / 4)).toBeLessThan(1800)
  })

  it('keeps signal-chart visuals token-driven and scroll-safe', () => {
    const stylesheet = readFileSync(signalChartsCssPath, 'utf8')
    const cardBlock = getSelectorBlock(stylesheet, '.signal-card')
    const fallbackBlock = getSelectorBlock(stylesheet, '.signal-chart-fallback')

    expect(stylesheet).not.toMatch(rawColorPattern)
    expect(cardBlock).toContain('--signal-card-border')
    expect(cardBlock).toContain('var(--operator-rail-panel-gradient-top)')
    expect(cardBlock).toContain('color-mix(in oklab')
    expect(stylesheet).toContain('@media (prefers-contrast: more)')
    expect(fallbackBlock).toContain('pointer-events: none')
    expect(fallbackBlock).toContain('touch-action: pan-y')
  })
})
