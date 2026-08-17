import { describe, expect, it } from 'vitest'

import { readDashboardFixture } from './test-fixtures/operatorHudTestFixtures'

describe('dashboard icon bundle architecture', () => {
  it('bundles local icon collections for static prerendering', () => {
    const nuxtConfig = readDashboardFixture('../../nuxt.config.ts')

    expect(nuxtConfig).toContain('collections: [\'lucide\', \'simple-icons\']')
    expect(nuxtConfig).toContain('clientBundle: {')
    expect(nuxtConfig).toContain('scan: true')
  })
})
