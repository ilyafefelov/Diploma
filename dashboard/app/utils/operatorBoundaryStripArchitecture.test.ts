import { describe, expect, it } from 'vitest'

import { readDashboardFixture } from './test-fixtures/operatorHudTestFixtures'

describe('operator boundary strip architecture', () => {
  it('keeps the first-viewport execution boundary explicit and non-executing', () => {
    const boundaryStrip = readDashboardFixture('../components/dashboard/operator/OperatorBoundaryStrip.vue')
    const operatorPage = readDashboardFixture('../pages/operator.vue')

    expect(boundaryStrip).toContain('Preview only')
    expect(boundaryStrip).toContain('No ProposedBid')
    expect(boundaryStrip).toContain('No market payload')
    expect(boundaryStrip).toContain('Human review required')
    expect(boundaryStrip).toContain('Defense path')
    expect(boundaryStrip).toContain('to="/defense"')
    expect(boundaryStrip).not.toContain('market-submittable')
    expect(boundaryStrip).not.toContain('dispatch command')
    expect(operatorPage).toContain('<OperatorBoundaryStrip')
  })
})
