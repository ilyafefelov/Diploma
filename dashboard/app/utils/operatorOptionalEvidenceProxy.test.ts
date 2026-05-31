// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const serverUtilsPath = fileURLToPath(new URL('../../server/utils/controlPlaneProxy.ts', import.meta.url))
const operatorRecommendationRoutePath = fileURLToPath(
  new URL('../../server/api/control-plane/dashboard/operator-recommendation.get.ts', import.meta.url)
)
const baselinePreviewRoutePath = fileURLToPath(
  new URL('../../server/api/control-plane/dashboard/baseline-lp-preview.get.ts', import.meta.url)
)

const optionalEvidenceRouteUrls = [
  '../../server/api/control-plane/dashboard/real-data-benchmark.get.ts',
  '../../server/api/control-plane/dashboard/calibrated-ensemble-benchmark.get.ts',
  '../../server/api/control-plane/dashboard/risk-adjusted-value-gate.get.ts',
  '../../server/api/control-plane/dashboard/forecast-dispatch-sensitivity.get.ts',
  '../../server/api/control-plane/dashboard/dfl-relaxed-pilot.get.ts',
  '../../server/api/control-plane/dashboard/dfl-schedule-value-production-gate.get.ts',
  '../../server/api/control-plane/dashboard/decision-transformer-trajectories.get.ts',
  '../../server/api/control-plane/dashboard/simulated-live-trading.get.ts',
  '../../server/api/control-plane/dashboard/operator-status.get.ts'
]

describe('operator optional evidence proxy', () => {
  it('maps expected missing evidence to null instead of noisy HTTP failures', () => {
    const proxyUtils = readFileSync(serverUtilsPath, 'utf8')

    expect(proxyUtils).toContain('proxyOptionalControlPlane')
    expect(proxyUtils).toContain('isControlPlaneNotFound')
    expect(proxyUtils).toContain('statusCode === 404')
    expect(proxyUtils).toContain('return null')

    for (const routeUrl of optionalEvidenceRouteUrls) {
      const routePath = fileURLToPath(new URL(routeUrl, import.meta.url))
      const route = readFileSync(routePath, 'utf8')

      expect(route).toContain('proxyOptionalControlPlane')
    }
  })

  it('preserves source-backed blocker details for required market preview routes', () => {
    const proxyUtils = readFileSync(serverUtilsPath, 'utf8')
    const operatorRecommendationRoute = readFileSync(operatorRecommendationRoutePath, 'utf8')
    const baselinePreviewRoute = readFileSync(baselinePreviewRoutePath, 'utf8')

    expect(proxyUtils).toContain('toControlPlaneProxyError')
    expect(proxyUtils).toContain('controlPlaneErrorMessage')
    expect(proxyUtils).toContain('data?.detail')
    expect(proxyUtils).toContain('return statusCode === null || Number.isNaN(statusCode)')

    for (const route of [operatorRecommendationRoute, baselinePreviewRoute]) {
      expect(route).toContain('proxyControlPlane')
      expect(route).not.toContain('statusCode: 502')
    }
  })
})
