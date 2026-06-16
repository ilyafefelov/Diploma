import { ref } from 'vue'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { useBaselinePreview } from './useBaselinePreview'
import { useOperatorRecommendation } from './useOperatorRecommendation'
import { useShadowRecommendationPreview } from './useShadowRecommendationPreview'

describe('operator market preview request concurrency', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('keeps the latest operator recommendation response when an older request resolves late', async () => {
    const firstRequest = deferred<{ target_delivery_date: string }>()
    const secondRequest = deferred<{ target_delivery_date: string }>()
    const fetchMock = vi.fn()
      .mockReturnValueOnce(firstRequest.promise)
      .mockReturnValueOnce(secondRequest.promise)
    vi.stubGlobal('$fetch', fetchMock)

    const { operatorRecommendation, loadOperatorRecommendation } = useOperatorRecommendation(
      ref('client_003_dnipro_factory'),
      ref('schedule_value_learner_v2_plus'),
      ref('DAM'),
      ref('2026-06-03')
    )

    const firstLoad = loadOperatorRecommendation()
    const secondLoad = loadOperatorRecommendation()

    secondRequest.resolve({ target_delivery_date: '2026-06-03' })
    await secondLoad
    expect(operatorRecommendation.value?.target_delivery_date).toBe('2026-06-03')

    firstRequest.resolve({ target_delivery_date: '2026-06-01' })
    await firstLoad
    expect(operatorRecommendation.value?.target_delivery_date).toBe('2026-06-03')
  })

  it('keeps the latest baseline preview response when an older request resolves late', async () => {
    const firstRequest = deferred<{ target_delivery_date: string }>()
    const secondRequest = deferred<{ target_delivery_date: string }>()
    const fetchMock = vi.fn()
      .mockReturnValueOnce(firstRequest.promise)
      .mockReturnValueOnce(secondRequest.promise)
    vi.stubGlobal('$fetch', fetchMock)

    const { baselinePreview, loadBaselinePreview } = useBaselinePreview(
      ref('client_003_dnipro_factory'),
      ref('DAM'),
      ref('2026-06-03')
    )

    const firstLoad = loadBaselinePreview()
    const secondLoad = loadBaselinePreview()

    secondRequest.resolve({ target_delivery_date: '2026-06-03' })
    await secondLoad
    expect(baselinePreview.value?.target_delivery_date).toBe('2026-06-03')

    firstRequest.resolve({ target_delivery_date: '2026-06-01' })
    await firstLoad
    expect(baselinePreview.value?.target_delivery_date).toBe('2026-06-03')
  })

  it('keeps the latest shadow preview response when an older request resolves late', async () => {
    const firstRequest = deferred<{ target_delivery_window_start: string }>()
    const secondRequest = deferred<{ target_delivery_window_start: string }>()
    const fetchMock = vi.fn()
      .mockReturnValueOnce(firstRequest.promise)
      .mockReturnValueOnce(secondRequest.promise)
    vi.stubGlobal('$fetch', fetchMock)

    const { shadowPreview, loadShadowRecommendationPreview } = useShadowRecommendationPreview(
      ref('client_003_dnipro_factory'),
      ref('hf_live_safe_switch_value_aligned_shadow'),
      ref(null),
      ref('DAM'),
      ref('2026-06-03')
    )

    const firstLoad = loadShadowRecommendationPreview()
    const secondLoad = loadShadowRecommendationPreview()

    secondRequest.resolve({ target_delivery_window_start: '2026-06-03T00:00:00' })
    await secondLoad
    expect(shadowPreview.value?.target_delivery_window_start).toBe('2026-06-03T00:00:00')

    firstRequest.resolve({ target_delivery_window_start: '2026-06-01T00:00:00' })
    await firstLoad
    expect(shadowPreview.value?.target_delivery_window_start).toBe('2026-06-03T00:00:00')
  })
})

const deferred = <T>() => {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve
    reject = promiseReject
  })

  return { promise, resolve, reject }
}
