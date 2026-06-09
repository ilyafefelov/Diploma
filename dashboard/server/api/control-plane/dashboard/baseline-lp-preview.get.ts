import type { BaselineLpPreview } from '~/types/control-plane'
import { proxyControlPlane } from '../../../utils/controlPlaneProxy'

export default defineEventHandler(async (event): Promise<BaselineLpPreview> => {
  return proxyControlPlane<BaselineLpPreview>(
    event,
    '/dashboard/baseline-lp-preview',
    'Failed to load baseline LP preview from the control plane.'
  )
})
