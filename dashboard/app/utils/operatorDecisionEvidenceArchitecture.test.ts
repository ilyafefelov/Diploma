// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const decisionEvidencePath = fileURLToPath(new URL('./operatorDecisionEvidence.ts', import.meta.url))
const fallbackModulePath = fileURLToPath(new URL('../lib/operator-decision/operatorDecisionEvidenceFallbacks.ts', import.meta.url))
const stateReadinessModulePath = fileURLToPath(new URL('../lib/operator-decision/operatorDecisionStateReadiness.ts', import.meta.url))
const approxTokens = (text: string): number => Math.ceil(text.length / 4)

describe('operator decision evidence architecture', () => {
  it('keeps fallback thesis evidence behind a focused internal module', () => {
    const source = readFileSync(decisionEvidencePath, 'utf8')

    expect(source).toContain('buildFallbackStrategyEvidenceRows')
    expect(source).toContain('buildFallbackControlRegretTimeline')
    expect(source).toContain('buildFallbackSensitivityEvidenceRows')
    expect(source).not.toContain('CURRENT_REGRET_LADDER')
    expect(approxTokens(source), 'decision evidence public module should stay focused').toBeLessThan(3_600)
    expect(existsSync(fallbackModulePath), 'fallback module should exist').toBe(true)
    expect(approxTokens(readFileSync(fallbackModulePath, 'utf8')), 'fallback module should stay bounded').toBeLessThan(1_400)
  })

  it('keeps physical/planning/grid readiness model behind a focused internal module', () => {
    const source = readFileSync(decisionEvidencePath, 'utf8')

    expect(source).toContain('buildOperatorDecisionStateCards')
    expect(source).toContain('buildOperatorDecisionReadinessItems')
    expect(source).not.toContain('planningReadinessDetail')
    expect(source).not.toContain('collectGridFlags')
    expect(source).not.toContain('summarizeSourceFreshness')
    expect(source).not.toContain('CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE')
    expect(existsSync(stateReadinessModulePath), 'state readiness module should exist').toBe(true)
    expect(approxTokens(readFileSync(stateReadinessModulePath, 'utf8')), 'state readiness module should stay bounded').toBeLessThan(2_200)
  })
})
