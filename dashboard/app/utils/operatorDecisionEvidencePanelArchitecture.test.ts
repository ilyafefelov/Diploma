import { describe, expect, it } from 'vitest'

import {
  approxTokens,
  readDashboardFixture
} from './test-fixtures/operatorHudTestFixtures'

describe('operator decision evidence panel architecture', () => {
  it('keeps decision evidence presentation model behind a composable seam', () => {
    const panel = readDashboardFixture('../components/dashboard/operator/OperatorDecisionEvidencePanel.vue')

    expect(panel).toContain('useOperatorDecisionEvidencePanelModel')
    expect(panel).toContain('decision-chart-summary')
    expect(panel).not.toContain('buildDecisionStrategyEvidenceOption')
    expect(panel).not.toContain('buildOperatorDecisionReadinessItems')
    expect(approxTokens(panel)).toBeLessThan(2_300)
  })
})
