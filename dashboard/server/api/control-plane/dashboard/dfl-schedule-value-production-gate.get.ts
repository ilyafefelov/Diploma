import type { DflScheduleValueProductionGateResponse } from '~/types/control-plane'
import { proxyOptionalControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<DflScheduleValueProductionGateResponse | null> => {
  return proxyOptionalControlPlane<DflScheduleValueProductionGateResponse>(
    event,
    '/dashboard/dfl-schedule-value-production-gate',
    'Failed to load DFL schedule/value production gate from the control plane.'
  )
})
