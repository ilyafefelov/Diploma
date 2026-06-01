// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

import {
  approxTokens,
  readDashboardFixture
} from './test-fixtures/operatorHudTestFixtures'

const narrativeModelPath = fileURLToPath(
  new URL('../composables/useOperatorPageNarrativeModel.ts', import.meta.url)
)

describe('operator page narrative architecture', () => {
  it('keeps schedule boundary copy and research metrics behind a page model seam', () => {
    const operatorPage = readDashboardFixture('../pages/operator.vue')

    expect(operatorPage).toContain('useOperatorPageNarrativeModel')
    expect(operatorPage).not.toContain('buildOperatorResearchMetrics')
    expect(operatorPage).not.toContain('formatStrategyId')
    expect(operatorPage).not.toContain('The dashboard reads FastAPI evidence')
    expect(approxTokens(operatorPage), 'operator.vue should stay reviewable after model extraction').toBeLessThan(4100)

    expect(existsSync(narrativeModelPath), 'useOperatorPageNarrativeModel.ts should exist').toBe(true)

    const narrativeModel = readFileSync(narrativeModelPath, 'utf8')

    expect(narrativeModel).toContain('buildOperatorResearchMetrics')
    expect(narrativeModel).toContain('scheduleMarketBoundaryLabel')
    expect(narrativeModel).toContain('DAM/IDM hourly preview / no ProposedBid / no market submission')
    expect(approxTokens(narrativeModel), 'narrative model should stay bounded').toBeLessThan(2000)
  })
})
