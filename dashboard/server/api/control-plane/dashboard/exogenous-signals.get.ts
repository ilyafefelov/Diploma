import type { DashboardExogenousSignalsResponse } from '~/types/control-plane'
import { proxyControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<DashboardExogenousSignalsResponse> => {
  return proxyControlPlane<DashboardExogenousSignalsResponse>(
    event,
    '/dashboard/exogenous-signals',
    'Failed to load exogenous signals from the control plane.'
  )
})
