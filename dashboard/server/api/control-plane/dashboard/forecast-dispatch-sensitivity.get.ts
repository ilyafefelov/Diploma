import type { ForecastDispatchSensitivityResponse } from '~/types/control-plane'
import { proxyOptionalControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<ForecastDispatchSensitivityResponse | null> => {
  return proxyOptionalControlPlane<ForecastDispatchSensitivityResponse>(
    event,
    '/dashboard/forecast-dispatch-sensitivity',
    'Failed to load forecast-dispatch sensitivity from the control plane.'
  )
})
