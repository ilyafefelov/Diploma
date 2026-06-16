import { nextTick, ref } from 'vue'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { useBaselinePreview } from './useBaselinePreview'
import { useOperatorRecommendation } from './useOperatorRecommendation'
import { useOperatorRecommendationPreviewModel } from './useOperatorRecommendationPreviewModel'
import { useShadowRecommendationComparison } from './useShadowRecommendationComparison'
import { useShadowRecommendationPreview } from './useShadowRecommendationPreview'
import { hfLiveSafeSwitchValueAlignedPreview } from '../utils/test-fixtures/operatorShadowPreviewFixtures'

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

  it('sends market venue and target date to the HF live shadow preview endpoint', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      preview_source_id: 'hf_live_safe_switch_shadow',
      recommendation_schedule: []
    })
    vi.stubGlobal('$fetch', fetchMock)

    const { loadShadowRecommendationPreview } = useShadowRecommendationPreview(
      ref('client_003_dnipro_factory'),
      ref('hf_live_safe_switch_shadow'),
      ref(null),
      ref('IDM'),
      ref('2026-06-03')
    )

    await loadShadowRecommendationPreview()

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/control-plane/dashboard/shadow-recommendation-preview',
      {
        query: {
          tenant_id: 'client_003_dnipro_factory',
          preview_source: 'hf_live_safe_switch_shadow',
          market_venue: 'IDM',
          target_delivery_date: '2026-06-03'
        }
      }
    )
  })

  it('does not use the value-aligned HF proof switch day for normal source selection without a target date', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      preview_source_id: 'hf_live_safe_switch_value_aligned_shadow',
      recommendation_schedule: []
    })
    vi.stubGlobal('$fetch', fetchMock)

    const { loadShadowRecommendationPreview } = useShadowRecommendationPreview(
      ref('client_003_dnipro_factory'),
      ref('hf_live_safe_switch_value_aligned_shadow'),
      ref(null),
      ref('DAM'),
      ref(null)
    )

    await loadShadowRecommendationPreview()

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/control-plane/dashboard/shadow-recommendation-preview',
      {
        query: {
          tenant_id: 'client_003_dnipro_factory',
          preview_source: 'hf_live_safe_switch_value_aligned_shadow',
          market_venue: 'DAM'
        }
      }
    )
  })

  it('does not call the LP-backed operator recommendation endpoint for manual HF live shadow refresh', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      preview_source_id: 'hf_live_safe_switch_shadow',
      preview_source_label: 'HF live safe-switch shadow',
      preview_status: 'live_shadow_not_promoted',
      recommendation_schedule: [],
      available_preview_sources: []
    })
    vi.stubGlobal('$fetch', fetchMock)

    const model = useOperatorRecommendationPreviewModel({
      selectedTenantId: ref('client_003_dnipro_factory'),
      selectedMarketVenue: ref('DAM'),
      selectedTargetDeliveryDate: ref('2026-06-03'),
      baselinePreview: ref(null)
    })
    model.selectedPreviewSourceId.value = 'hf_live_safe_switch_shadow'

    await model.refreshVisibleRecommendation()

    expect(fetchMock).not.toHaveBeenCalledWith(
      '/api/control-plane/dashboard/operator-recommendation',
      expect.anything()
    )
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/control-plane/dashboard/shadow-recommendation-preview',
      {
        query: {
          tenant_id: 'client_003_dnipro_factory',
          preview_source: 'hf_live_safe_switch_shadow',
          market_venue: 'DAM',
          target_delivery_date: '2026-06-03'
        }
      }
    )
  })

  it('clears stale LP-backed recommendation errors when auto loading is disabled for live HF shadow', async () => {
    const fetchMock = vi
      .fn()
      .mockRejectedValueOnce(new Error('Official OREE DAM row is not published for target_delivery_date=2026-06-04'))
    vi.stubGlobal('$fetch', fetchMock)
    const shouldAutoLoad = ref(true)
    const { error, loadOperatorRecommendation } = useOperatorRecommendation(
      ref('client_003_dnipro_factory'),
      ref('schedule_value_learner_v2_plus'),
      ref('DAM'),
      ref('2026-06-04'),
      shouldAutoLoad
    )

    await loadOperatorRecommendation()
    expect(error.value).toContain('Official OREE DAM row is not published')

    shouldAutoLoad.value = false
    await nextTick()

    expect(error.value).toBe('')
  })

  it('does not call the LP-backed operator recommendation endpoint for manual value-aligned HF live shadow refresh', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      preview_source_id: 'hf_live_safe_switch_value_aligned_shadow',
      preview_source_label: 'HF live safe-switch value-aligned shadow',
      preview_status: 'value_aligned_shadow_not_promoted',
      recommendation_schedule: [],
      available_preview_sources: []
    })
    vi.stubGlobal('$fetch', fetchMock)

    const model = useOperatorRecommendationPreviewModel({
      selectedTenantId: ref('client_003_dnipro_factory'),
      selectedMarketVenue: ref('DAM'),
      selectedTargetDeliveryDate: ref('2026-06-03'),
      baselinePreview: ref(null)
    })
    model.selectedPreviewSourceId.value = 'hf_live_safe_switch_value_aligned_shadow'

    await model.refreshVisibleRecommendation()

    expect(fetchMock).not.toHaveBeenCalledWith(
      '/api/control-plane/dashboard/operator-recommendation',
      expect.anything()
    )
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/control-plane/dashboard/shadow-recommendation-preview',
      {
        query: {
          tenant_id: 'client_003_dnipro_factory',
          preview_source: 'hf_live_safe_switch_value_aligned_shadow',
          market_venue: 'DAM',
          target_delivery_date: '2026-06-03'
        }
      }
    )
  })

  it('explains missing source-backed rows for blocked value-aligned HF live shadow packets', async () => {
    vi.stubGlobal('$fetch', vi.fn().mockResolvedValue({
      preview_source_id: 'hf_live_safe_switch_value_aligned_shadow',
      preview_source_label: 'HF live safe-switch value-aligned shadow',
      preview_status: 'blocked_missing_source_backed_price_context',
      recommendation_schedule: [],
      available_preview_sources: []
    }))

    const model = useOperatorRecommendationPreviewModel({
      selectedTenantId: ref('client_003_dnipro_factory'),
      selectedMarketVenue: ref('DAM'),
      selectedTargetDeliveryDate: ref('2026-06-04'),
      baselinePreview: ref(null)
    })
    model.selectedPreviewSourceId.value = 'hf_live_safe_switch_value_aligned_shadow'

    await model.refreshVisibleRecommendation()

    expect(model.hourlyRecommendationRows.value).toEqual([])
    expect(model.hourlyRecommendationEmptyMessage.value).toBe(
      'HF shadow is blocked for the selected delivery date because no source-backed official or forecast price rows are available. No trade preview is shown.'
    )
  })

  it('loads value-aligned HF demo scenarios through the shadow endpoint only', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      preview_source_id: 'hf_live_safe_switch_value_aligned_shadow',
      preview_source_label: 'HF live safe-switch value-aligned shadow',
      preview_status: 'value_aligned_shadow_not_promoted',
      recommendation_schedule: [],
      available_preview_sources: []
    })
    vi.stubGlobal('$fetch', fetchMock)
    const selectedMarketVenue = ref<'DAM' | 'IDM'>('IDM')
    const selectedTargetDeliveryDate = ref<string | null>(null)

    const model = useOperatorRecommendationPreviewModel({
      selectedTenantId: ref('client_003_dnipro_factory'),
      selectedMarketVenue,
      selectedTargetDeliveryDate,
      baselinePreview: ref(null)
    })

    await model.selectValueAlignedHfShadowDemoScenario('forecast_dam_action')

    expect(model.selectedPreviewSourceId.value).toBe('hf_live_safe_switch_value_aligned_shadow')
    expect(selectedMarketVenue.value).toBe('DAM')
    expect(selectedTargetDeliveryDate.value).toBe('2026-06-02')
    expect(fetchMock).not.toHaveBeenCalledWith(
      '/api/control-plane/dashboard/operator-recommendation',
      expect.anything()
    )
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/control-plane/dashboard/shadow-recommendation-preview',
      {
        query: {
          tenant_id: 'client_003_dnipro_factory',
          preview_source: 'hf_live_safe_switch_value_aligned_shadow',
          market_venue: 'DAM',
          target_delivery_date: '2026-06-02'
        }
      }
    )
  })

  it('preserves the operator-selected market and date when value-aligned HF is selected from the source dropdown', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      preview_source_id: 'hf_live_safe_switch_value_aligned_shadow',
      preview_source_label: 'HF live safe-switch value-aligned shadow',
      preview_status: 'value_aligned_shadow_not_promoted',
      recommendation_schedule: [],
      available_preview_sources: []
    })
    vi.stubGlobal('$fetch', fetchMock)
    const selectedMarketVenue = ref<'DAM' | 'IDM'>('IDM')
    const selectedTargetDeliveryDate = ref<string | null>('2026-06-04')

    const model = useOperatorRecommendationPreviewModel({
      selectedTenantId: ref('client_003_dnipro_factory'),
      selectedMarketVenue,
      selectedTargetDeliveryDate,
      baselinePreview: ref(null)
    })

    await model.selectPreviewSource('hf_live_safe_switch_value_aligned_shadow')

    expect(model.selectedPreviewSourceId.value).toBe('hf_live_safe_switch_value_aligned_shadow')
    expect(selectedMarketVenue.value).toBe('IDM')
    expect(selectedTargetDeliveryDate.value).toBe('2026-06-04')
    expect(fetchMock).not.toHaveBeenCalledWith(
      '/api/control-plane/dashboard/operator-recommendation',
      expect.anything()
    )
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/control-plane/dashboard/shadow-recommendation-preview',
      {
        query: {
          tenant_id: 'client_003_dnipro_factory',
          preview_source: 'hf_live_safe_switch_value_aligned_shadow',
          market_venue: 'IDM',
          target_delivery_date: '2026-06-04'
        }
      }
    )
  })

  it('clears stale selected shadow rows while loading a new shadow preview', async () => {
    const pendingFetch = new Promise(resolve => {
      setTimeout(() => resolve({
        preview_source_id: 'hf_live_safe_switch_value_aligned_shadow',
        preview_source_label: 'HF live safe-switch value-aligned shadow',
        preview_status: 'value_aligned_shadow_not_promoted',
        recommendation_schedule: [],
        available_preview_sources: []
      }), 0)
    })
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(hfLiveSafeSwitchValueAlignedPreview())
      .mockReturnValueOnce(pendingFetch)
    vi.stubGlobal('$fetch', fetchMock)

    const { shadowPreview, loadShadowRecommendationPreview } = useShadowRecommendationPreview(
      ref('client_003_dnipro_factory'),
      ref('hf_live_safe_switch_value_aligned_shadow'),
      ref(null),
      ref('DAM'),
      ref('2026-06-02')
    )

    await loadShadowRecommendationPreview()
    expect(shadowPreview.value?.preview_source_id).toBe('hf_live_safe_switch_value_aligned_shadow')

    const loadingPromise = loadShadowRecommendationPreview()

    expect(shadowPreview.value).toBeNull()

    await loadingPromise
  })

  it('uses the matching value-aligned HF comparison preview when the primary selected shadow response is absent', async () => {
    const selectedPreview = {
      ...hfLiveSafeSwitchValueAlignedPreview(),
      target_delivery_window_start: '2026-06-02T00:00:00',
      target_delivery_window_end: '2026-06-03T00:00:00',
      recommendation_schedule: hfLiveSafeSwitchValueAlignedPreview().recommendation_schedule.map((point, index) => ({
        ...point,
        interval_start: `2026-06-02T${String(index).padStart(2, '0')}:00:00`
      }))
    }
    let selectedHfCallCount = 0
    const fetchMock = vi.fn((_url, options) => {
      const previewSource = options.query.preview_source
      if (previewSource === 'hf_live_safe_switch_value_aligned_shadow') {
        selectedHfCallCount += 1
        return selectedHfCallCount === 1
          ? Promise.reject(new Error('primary selected shadow did not settle'))
          : Promise.resolve(selectedPreview)
      }
      if (previewSource === 'dt_v2_plus_distillation_shadow') {
        return Promise.resolve({ preview_source_id: 'dt_v2_plus_distillation_shadow', recommendation_schedule: [] })
      }
      if (previewSource === 'dt_v2_plus_safe_switch_selector_shadow') {
        return Promise.resolve({ preview_source_id: 'dt_v2_plus_safe_switch_selector_shadow', recommendation_schedule: [] })
      }
      return Promise.resolve({ preview_source_id: previewSource, recommendation_schedule: [] })
    })
    vi.stubGlobal('$fetch', fetchMock)

    const model = useOperatorRecommendationPreviewModel({
      selectedTenantId: ref('client_003_dnipro_factory'),
      selectedMarketVenue: ref('DAM'),
      selectedTargetDeliveryDate: ref(null),
      baselinePreview: ref(null)
    })

    await model.selectValueAlignedHfShadowDemoScenario('forecast_dam_action')

    expect(model.visibleOperatorRecommendation.value?.selected_strategy_id).toBe(
      'hf_live_safe_switch_value_aligned_shadow'
    )
    expect(model.visibleOperatorRecommendation.value?.target_delivery_date).toBe('2026-06-02')
    expect(model.hourlyRecommendationRows.value).toHaveLength(selectedPreview.recommendation_schedule.length)
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

  it('clears stale baseline preview errors when baseline auto loading is disabled for live HF shadow', async () => {
    const fetchMock = vi
      .fn()
      .mockRejectedValueOnce(new Error('point-in-time forecast metadata rejected for nbeatsx_official_v0'))
    vi.stubGlobal('$fetch', fetchMock)
    const shouldAutoLoad = ref(true)
    const { error, loadBaselinePreview } = useBaselinePreview(
      ref('client_003_dnipro_factory'),
      ref('DAM'),
      ref('2026-06-02'),
      shouldAutoLoad
    )

    await loadBaselinePreview()
    expect(error.value).toContain('point-in-time forecast metadata rejected')

    shouldAutoLoad.value = false
    await nextTick()

    expect(error.value).toBe('')

    await loadBaselinePreview()

    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(error.value).toBe('')
  })

  it('loads same-window HF value-aligned strategy comparisons with selected venue and target date', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      preview_source_id: 'dt_v2_plus_distillation_shadow',
      recommendation_schedule: []
    })
    vi.stubGlobal('$fetch', fetchMock)

    const { loadShadowComparisonPreviews } = useShadowRecommendationComparison(
      ref('client_003_dnipro_factory'),
      ref(null),
      ref('IDM'),
      ref('2026-06-03'),
      ref('hf_live_safe_switch_value_aligned_shadow')
    )

    await loadShadowComparisonPreviews()

    const queries = fetchMock.mock.calls.map(([, options]) => options.query)
    expect(queries.map(query => query.preview_source)).toEqual([
      'dt_v2_plus_distillation_shadow',
      'dt_v2_plus_safe_switch_selector_shadow',
      'hf_live_safe_switch_value_aligned_shadow'
    ])
    for (const query of queries) {
      expect(query).toEqual(expect.objectContaining({
        tenant_id: 'client_003_dnipro_factory',
        market_venue: 'IDM',
        target_delivery_date: '2026-06-03'
      }))
    }
    expect(queries.some(query => 'target_delivery_window_start' in query)).toBe(false)
  })
})
