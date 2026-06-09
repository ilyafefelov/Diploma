import type { RealDataBenchmarkResponse } from '~/types/control-plane'
import { proxyOptionalControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<RealDataBenchmarkResponse | null> => {
  return proxyOptionalControlPlane<RealDataBenchmarkResponse>(
    event,
    '/dashboard/calibrated-ensemble-benchmark',
    'Failed to load calibrated ensemble benchmark from the control plane.'
  )
})
