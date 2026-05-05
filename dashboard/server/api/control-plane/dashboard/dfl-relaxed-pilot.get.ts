import type { DflRelaxedPilotResponse } from '~/types/control-plane'
import { proxyControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<DflRelaxedPilotResponse> => {
  return proxyControlPlane<DflRelaxedPilotResponse>(
    event,
    '/dashboard/dfl-relaxed-pilot',
    'Failed to load relaxed DFL pilot from the control plane.'
  )
})
