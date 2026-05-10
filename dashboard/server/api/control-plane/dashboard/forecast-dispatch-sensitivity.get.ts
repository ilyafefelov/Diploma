import type { ForecastDispatchSensitivityResponse } from '~/types/control-plane'
import { proxyControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<ForecastDispatchSensitivityResponse> => {
  return proxyControlPlane<ForecastDispatchSensitivityResponse>(
    event,
    '/dashboard/forecast-dispatch-sensitivity',
    'Failed to load forecast-dispatch sensitivity from the control plane.'
  )
})
