import type { DflScheduleValueProductionGateResponse } from '~/types/control-plane'
import { proxyControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<DflScheduleValueProductionGateResponse> => {
  return proxyControlPlane<DflScheduleValueProductionGateResponse>(
    event,
    '/dashboard/dfl-schedule-value-production-gate',
    'Failed to load DFL schedule/value production gate from the control plane.'
  )
})
