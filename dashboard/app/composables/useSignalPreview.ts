import { computed, ref, watch } from 'vue'

import type { SignalPreview } from '~/types/control-plane'

export const useSignalPreview = (selectedTenantId: Readonly<{ value: string }>) => {
  const signalPreview = ref<SignalPreview | null>(null)
  const isLoading = ref(false)
  const error = ref('')
  const lastLoadedAt = ref<number | null>(null)

  const clearError = (): void => {
    error.value = ''
  }

  const lastLoadedLabel = computed(() => {
    if (!lastLoadedAt.value) {
      return 'Not loaded yet'
    }

    return new Date(lastLoadedAt.value).toLocaleTimeString('en-GB', {
      hour: '2-digit',
      minute: '2-digit'
    })
  })

  const loadSignalPreview = async (): Promise<void> => {
    if (!selectedTenantId.value) {
      signalPreview.value = null
      return
    }

    isLoading.value = true
    error.value = ''

    try {
      signalPreview.value = await $fetch<SignalPreview>('/api/control-plane/dashboard/signal-preview', {
        query: {
          tenant_id: selectedTenantId.value
        }
      })
      lastLoadedAt.value = Date.now()
    } catch (unknownError) {
      signalPreview.value = null
      error.value = unknownError instanceof Error ? unknownError.message : 'Unable to load dashboard signal preview.'
    } finally {
      isLoading.value = false
    }
  }

  watch(selectedTenantId, async () => {
    await loadSignalPreview()
  })

  return {
    signalPreview,
    isLoading,
    error,
    clearError,
    lastLoadedLabel,
    loadSignalPreview
  }
}