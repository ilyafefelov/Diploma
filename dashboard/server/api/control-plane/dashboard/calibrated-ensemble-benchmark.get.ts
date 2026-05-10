import type { RealDataBenchmarkResponse } from '~/types/control-plane'
import { proxyControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<RealDataBenchmarkResponse> => {
  return proxyControlPlane<RealDataBenchmarkResponse>(
    event,
    '/dashboard/calibrated-ensemble-benchmark',
    'Failed to load calibrated ensemble benchmark from the control plane.'
  )
})
