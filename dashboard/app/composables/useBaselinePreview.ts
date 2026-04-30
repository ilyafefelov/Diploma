import { computed, ref, watch } from 'vue'

import type { BaselineLpPreview } from '~/types/control-plane'

export const useBaselinePreview = (selectedTenantId: Readonly<{ value: string }>) => {
  const baselinePreview = ref<BaselineLpPreview | null>(null)
  const isLoading = ref(false)
  const error = ref('')
  const lastLoadedAt = ref<number | null>(null)

  const lastLoadedLabel = computed(() => {
    if (!lastLoadedAt.value) {
      return 'Not loaded yet'
    }

    return new Date(lastLoadedAt.value).toLocaleTimeString('en-GB', {
      hour: '2-digit',
      minute: '2-digit'
    })
  })

  const loadBaselinePreview = async (): Promise<void> => {
    if (!selectedTenantId.value) {
      baselinePreview.value = null
      return
    }

    isLoading.value = true
    error.value = ''

    try {
      baselinePreview.value = await $fetch<BaselineLpPreview>('/api/control-plane/dashboard/baseline-lp-preview', {
        query: {
          tenant_id: selectedTenantId.value
        }
      })
      lastLoadedAt.value = Date.now()
    } catch (unknownError) {
      baselinePreview.value = null
      error.value = unknownError instanceof Error ? unknownError.message : 'Unable to load baseline LP preview.'
    } finally {
      isLoading.value = false
    }
  }

  watch(selectedTenantId, async () => {
    await loadBaselinePreview()
  })

  return {
    baselinePreview,
    isLoading,
    error,
    lastLoadedLabel,
    loadBaselinePreview
  }
}