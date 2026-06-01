import { ref } from 'vue'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { useBaselinePreview } from './useBaselinePreview'
import { useOperatorRecommendation } from './useOperatorRecommendation'

describe('operator market preview request wiring', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('sends DAM/IDM venue and explicit target date to the operator recommendation endpoint', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      market_venue: 'IDM',
      target_delivery_date: '2026-05-20'
    })
    vi.stubGlobal('$fetch', fetchMock)

    const { loadOperatorRecommendation } = useOperatorRecommendation(
      ref('client_003_dnipro_factory'),
      ref('schedule_value_learner_v2_plus'),
      ref('IDM'),
      ref('2026-05-20')
    )

    await loadOperatorRecommendation()

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/control-plane/dashboard/operator-recommendation',
      {
        query: {
          tenant_id: 'client_003_dnipro_factory',
          strategy_id: 'schedule_value_learner_v2_plus',
          market_venue: 'IDM',
          target_delivery_date: '2026-05-20'
        }
      }
    )
  })

  it('sends the selected market venue to the baseline LP preview and omits empty target dates', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      market_venue: 'IDM',
      target_delivery_date: null
    })
    vi.stubGlobal('$fetch', fetchMock)

    const { loadBaselinePreview } = useBaselinePreview(
      ref('client_003_dnipro_factory'),
      ref('IDM'),
      ref(null)
    )

    await loadBaselinePreview()

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/control-plane/dashboard/baseline-lp-preview',
      {
        query: {
          tenant_id: 'client_003_dnipro_factory',
          market_venue: 'IDM'
        }
      }
    )
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
