import type { SimulatedLiveTradingResponse } from '~/types/control-plane'
import { proxyControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<SimulatedLiveTradingResponse> => {
  return proxyControlPlane<SimulatedLiveTradingResponse>(
    event,
    '/dashboard/simulated-live-trading',
    'Failed to load simulated live-trading rows from the control plane.'
  )
})
