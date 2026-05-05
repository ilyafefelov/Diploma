import type { DashboardBatteryStateResponse } from '~/types/control-plane'
import { proxyControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<DashboardBatteryStateResponse> => {
  return proxyControlPlane<DashboardBatteryStateResponse>(
    event,
    '/dashboard/battery-state',
    'Failed to load battery telemetry state from the control plane.'
  )
})
