// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const composablePath = fileURLToPath(new URL('./useOperatorDashboardViewModel.ts', import.meta.url))
const timelineModelPath = fileURLToPath(new URL('../lib/operator-dashboard/useOperatorTimelineModel.ts', import.meta.url))
const signalModelPath = fileURLToPath(new URL('../lib/operator-dashboard/operatorDashboardSignalModel.ts', import.meta.url))

const approxTokens = (text: string): number => Math.ceil(text.length / 4)

describe('useOperatorDashboardViewModel architecture', () => {
  it('keeps the dashboard view-model composable as a bounded orchestrator', () => {
    const source = readFileSync(composablePath, 'utf8')

    expect(approxTokens(source), 'useOperatorDashboardViewModel.ts should stay below 5000 approx tokens').toBeLessThan(5000)
    expect(source).toContain('../lib/operator-dashboard/useOperatorTimelineModel')
    expect(source).toContain('../lib/operator-dashboard/operatorDashboardSignalModel')
    expect(source).not.toContain('const timelineSegments = computed')
    expect(source).not.toContain('const gatekeeperActions = computed')
    expect(source).not.toContain('const headlineMetrics = computed')
    expect(source).not.toContain('const moodChips = computed')
    expect(source).not.toContain('const marketRegimeChips = computed')
    expect(source).not.toContain('const motiveItems = computed')
  })

  it('keeps DAM timeline and gatekeeper logic in a bounded implementation module', () => {
    expect(existsSync(timelineModelPath), 'useOperatorTimelineModel.ts should exist').toBe(true)

    const source = readFileSync(timelineModelPath, 'utf8')
    expect(approxTokens(source), 'useOperatorTimelineModel.ts should stay below 5000 approx tokens').toBeLessThan(5000)
    expect(source).toContain('No market payload')
    expect(source).toContain('no ProposedBid')
  })

  it('keeps dashboard signal copy and status heuristics in a focused implementation module', () => {
    expect(existsSync(signalModelPath), 'operatorDashboardSignalModel.ts should exist').toBe(true)

    const source = readFileSync(signalModelPath, 'utf8')

    expect(source).toContain('buildOperatorHeadlineMetrics')
    expect(source).toContain('buildOperatorMoodChips')
    expect(source).toContain('buildOperatorMarketRegimeChips')
    expect(source).toContain('buildOperatorMotiveItems')
    expect(source).toContain('Read-model health')
    expect(source).toContain('DAM volatility')
    expect(approxTokens(source), 'operatorDashboardSignalModel.ts should stay below 5000 approx tokens').toBeLessThan(5000)
  })
})
