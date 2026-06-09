import type { RealDataBenchmarkResponse } from '~/types/control-plane'
import { proxyOptionalControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<RealDataBenchmarkResponse | null> => {
  return proxyOptionalControlPlane<RealDataBenchmarkResponse>(
    event,
    '/dashboard/real-data-benchmark',
    'Failed to load real-data benchmark from the control plane.'
  )
})
