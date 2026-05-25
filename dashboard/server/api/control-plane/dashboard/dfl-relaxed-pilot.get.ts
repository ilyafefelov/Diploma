import type { DflRelaxedPilotResponse } from '~/types/control-plane'
import { proxyOptionalControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<DflRelaxedPilotResponse | null> => {
  return proxyOptionalControlPlane<DflRelaxedPilotResponse>(
    event,
    '/dashboard/dfl-relaxed-pilot',
    'Failed to load relaxed DFL pilot from the control plane.'
  )
})
