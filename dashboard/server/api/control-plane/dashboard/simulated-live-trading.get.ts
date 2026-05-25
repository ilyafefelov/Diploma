import type { SimulatedLiveTradingResponse } from '~/types/control-plane'
import { proxyOptionalControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<SimulatedLiveTradingResponse | null> => {
  return proxyOptionalControlPlane<SimulatedLiveTradingResponse>(
    event,
    '/dashboard/simulated-live-trading',
    'Failed to load simulated live-trading rows from the control plane.'
  )
})
