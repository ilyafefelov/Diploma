import type { RealDataBenchmarkResponse } from '~/types/control-plane'
import { proxyOptionalControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<RealDataBenchmarkResponse | null> => {
  return proxyOptionalControlPlane<RealDataBenchmarkResponse>(
    event,
    '/dashboard/risk-adjusted-value-gate',
    'Failed to load risk-adjusted value gate from the control plane.'
  )
})
