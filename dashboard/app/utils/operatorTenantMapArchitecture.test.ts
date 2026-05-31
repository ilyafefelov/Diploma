import { describe, expect, it } from 'vitest'

import { readDashboardFixture } from './test-fixtures/operatorHudTestFixtures'

describe('operator tenant map architecture', () => {
  it('keeps tenant map visual colors behind operator design tokens', () => {
    const tenantMap = readDashboardFixture('../components/dashboard/operator/OperatorTenantMapCard.vue')
    const tenantMapCss = readDashboardFixture('../assets/css/operator-tenant-map.css')
    const tokenCss = readDashboardFixture('../assets/css/design-tokens.css')

    for (const token of [
      '--operator-tenant-map-surface-top',
      '--operator-tenant-map-surface-bottom',
      '--operator-tenant-map-marker-fill',
      '--operator-tenant-map-active-gradient-top',
      '--operator-tenant-map-card-surface',
      '--operator-tenant-map-weather-gradient-top',
      '--operator-positive',
      '--focus-ring'
    ]) {
      expect(tokenCss, `${token} should live in design tokens`).toContain(`${token}:`)
      expect(tenantMapCss, `${token} should be consumed by tenant map styles`).toContain(`var(${token})`)
    }

    expect(tenantMap).toContain('<style scoped src="../../../assets/css/operator-tenant-map.css"></style>')
    expect(tenantMapCss).not.toMatch(/#[0-9a-fA-F]{3,8}\b/)
    expect(tenantMapCss).not.toContain('rgba(')
    expect(tenantMapCss).not.toContain('color: white')
  })

  it('keeps active tenant map marker motion reduced-motion aware', () => {
    const tenantMapCss = readDashboardFixture('../assets/css/operator-tenant-map.css')

    expect(tenantMapCss).toContain('@media (prefers-reduced-motion: reduce)')
    expect(tenantMapCss).toContain('.tenant-card__tenant-marker--active')
    expect(tenantMapCss).toContain('animation: none')
    expect(tenantMapCss).toContain('transition: none')
  })

  it('keeps tenant map projection and readout logic behind a composable seam', () => {
    const tenantMap = readDashboardFixture('../components/dashboard/operator/OperatorTenantMapCard.vue')
    const model = readDashboardFixture('../composables/useOperatorTenantMapCardModel.ts')

    expect(tenantMap).toContain('useOperatorTenantMapCardModel(props)')
    expect(tenantMap).not.toContain('mercatorY')
    expect(tenantMap).not.toContain('ukraineMapBounds')
    expect(model).toContain('export const useOperatorTenantMapCardModel')
    expect(model).toContain('tenantMarkers')
    expect(model).toContain('weatherUpliftValue')
    expect(model).toContain('currentMarketPrice')
  })
})
