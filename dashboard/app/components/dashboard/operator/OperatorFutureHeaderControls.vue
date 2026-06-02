<script setup lang="ts">
import { computed } from 'vue'

import type { ShadowRecommendationPreviewResponse } from '~/types/control-plane'
import type { OperatorPreviewSourceId } from '~/utils/operatorShadowPreview'
import {
  buildPreviewSourceSelectItems,
  VALUE_ALIGNED_HF_SHADOW_DEMO_SCENARIOS,
  type ValueAlignedHfShadowDemoScenarioId
} from '~/utils/operatorFutureStackPresentation'

const props = defineProps<{
  selectedPreviewSourceId: OperatorPreviewSourceId
  shadowPreview: ShadowRecommendationPreviewResponse | null
  shadowPreviewLastLoadedLabel: string
  activeErrorCount: number
  isLoading: boolean
}>()

const emit = defineEmits<{
  'update:selectedPreviewSourceId': [value: OperatorPreviewSourceId]
  'refresh:shadowPreview': []
  'select:hf-demo-scenario': [value: ValueAlignedHfShadowDemoScenarioId]
}>()

const readModelBadgeLabel = computed(() => {
  if (props.isLoading) {
    return 'Refreshing'
  }

  return props.activeErrorCount > 0 ? `${props.activeErrorCount} read-model gap(s)` : 'FastAPI read model'
})

const previewSourceSelectItems = computed(() => buildPreviewSourceSelectItems(props.shadowPreview))
const hfDemoScenarioItems = VALUE_ALIGNED_HF_SHADOW_DEMO_SCENARIOS

const updateSelectedPreviewSource = (value: string | number | boolean | Record<string, unknown>): void => {
  if (typeof value === 'string') {
    emit('update:selectedPreviewSourceId', value as OperatorPreviewSourceId)
    return
  }

  if (typeof value === 'object' && value !== null && typeof value.value === 'string') {
    emit('update:selectedPreviewSourceId', value.value as OperatorPreviewSourceId)
  }
}
</script>

<template>
  <div class="console-heading">
    <div>
      <p class="eyebrow">
        Forecast evidence / read model
      </p>
      <h2 class="section-title">
        Delivery-day schedule preview and evidence gates
      </h2>
    </div>
    <div class="future-control-stack">
      <UFormField
        label="Schedule shown"
        name="future-preview-source"
        class="future-schedule-source-control"
        :ui="{ label: 'future-schedule-source-control__label', container: 'future-schedule-source-control__field' }"
      >
        <USelect
          class="future-strategy-select"
          aria-label="Select schedule source preview"
          :model-value="selectedPreviewSourceId"
          :items="previewSourceSelectItems"
          value-key="value"
          label-key="label"
          color="info"
          variant="none"
          @update:model-value="updateSelectedPreviewSource"
        />
      </UFormField>
      <div
        class="future-baseline-context"
        aria-label="Default comparator context"
      >
        <span>Comparator baseline</span>
        <strong>Strict similar-day baseline</strong>
        <small>Frozen LP/oracle regret reference; not a second selector.</small>
      </div>
      <div
        class="future-baseline-context future-baseline-context--default"
        aria-label="Default fallback context"
      >
        <span>Default/fallback</span>
        <strong>V2+ schedule/value learner</strong>
        <small>DT and diagnostics stay manual preview only.</small>
      </div>
      <div
        class="future-demo-scenarios"
        aria-label="HF value-aligned demo scenarios"
      >
        <span>HF demo cases</span>
        <div class="future-demo-scenarios__buttons">
          <UButton
            v-for="scenario in hfDemoScenarioItems"
            :key="scenario.id"
            class="future-demo-scenario-button"
            icon="i-lucide-radio"
            :label="scenario.label"
            :title="scenario.boundaryCopy"
            color="info"
            variant="soft"
            size="xs"
            @click="emit('select:hf-demo-scenario', scenario.id)"
          />
        </div>
        <small>Manual proof/abstention presets; no market execution.</small>
      </div>
      <UButton
        class="future-refresh-button"
        icon="i-lucide-refresh-cw"
        :label="`Loaded ${shadowPreviewLastLoadedLabel}`"
        color="info"
        variant="soft"
        size="xs"
        @click="emit('refresh:shadowPreview')"
      />
      <UBadge
        class="status-badge"
        :label="readModelBadgeLabel"
        :color="activeErrorCount > 0 ? 'warning' : 'success'"
        variant="soft"
      />
    </div>
  </div>
</template>

<style scoped>
.future-control-stack {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  justify-content: flex-end;
  gap: 0.55rem;
  max-width: 100%;
  min-width: min(100%, 26rem);
}

.future-control-stack .future-schedule-source-control {
  display: grid;
  gap: 0.22rem;
  max-width: 100%;
  min-width: min(18rem, 100%);
}

.future-schedule-source-control {
  border: 4px solid var(--operator-card-border-strong);
  border-radius: 14px;
  color: var(--operator-control-foreground);
  font-weight: 600;
  padding: 4px 0.45rem 0.42rem 7px;
}

.future-baseline-context {
  display: grid;
  gap: 0.12rem;
  min-width: min(13.5rem, 100%);
  border: 1px solid var(--operator-line-dim);
  border-radius: 0.55rem;
  background: var(--operator-control-surface-muted);
  padding: 0.42rem 0.55rem;
}

.future-baseline-context--default {
  border-color: var(--operator-accent-faint);
}

.future-demo-scenarios {
  display: grid;
  gap: 0.2rem;
  min-width: min(18rem, 100%);
  max-width: 32rem;
  border: 1px solid var(--operator-line-dim);
  border-radius: 0.55rem;
  background: var(--operator-control-surface-muted);
  padding: 0.42rem 0.55rem;
}

.future-demo-scenarios__buttons {
  display: flex;
  flex-wrap: wrap;
  gap: 0.28rem;
}

.future-demo-scenario-button {
  min-height: 1.7rem;
  border-radius: 4px !important;
  font-size: 0.68rem;
  font-weight: 900;
  white-space: normal;
}

.future-baseline-context strong {
  overflow-wrap: anywhere;
  color: var(--operator-surface-foreground);
  font-size: 0.76rem;
  font-weight: 900;
  line-height: 1.15;
}

.future-baseline-context small {
  color: var(--operator-text-muted);
  font-size: 0.62rem;
  font-weight: 760;
  line-height: 1.24;
}

.future-demo-scenarios small {
  color: var(--operator-text-muted);
  font-size: 0.62rem;
  font-weight: 760;
  line-height: 1.24;
}

.future-control-stack span {
  color: var(--operator-accent-soft);
  font-size: 0.64rem;
  font-weight: 900;
  text-transform: uppercase;
}

.future-schedule-source-control__label {
  color: var(--operator-control-foreground);
  font-weight: 600;
}

.future-schedule-source-control :deep(*) {
  max-width: 100%;
  min-width: 0;
}

.future-schedule-source-control :deep(button) {
  width: 100%;
}

.future-strategy-select {
  min-height: 2.4rem;
  border: 1px solid var(--operator-line-subtle);
  border-radius: 0.55rem;
  background: var(--operator-control-surface-strong);
  color: var(--operator-control-foreground);
  font-size: clamp(1.25rem, 2vw, 2rem);
  font-weight: 900;
  line-height: 1.05;
  max-width: 100%;
}

.future-schedule-source-control :deep(.future-strategy-select) {
  min-height: 2.4rem;
  border: 1px solid var(--operator-line-subtle);
  border-radius: 0.55rem;
  background: var(--operator-control-surface-strong);
  color: var(--operator-control-foreground) !important;
  font-size: clamp(1.25rem, 2vw, 2rem) !important;
  font-weight: 900 !important;
  line-height: 1.05;
}

.future-strategy-select :deep([data-slot="value"]) {
  color: var(--operator-control-foreground);
  font-size: inherit;
  font-weight: 900;
  line-height: inherit;
}

.future-schedule-source-control :deep(.future-strategy-select [data-slot="value"]) {
  color: var(--operator-control-foreground) !important;
  font-size: inherit;
  font-weight: 900 !important;
  line-height: inherit;
}

.future-refresh-button {
  min-height: 2.4rem;
  border: 2px solid var(--operator-control-foreground) !important;
  border-radius: 3px !important;
  color: var(--operator-control-foreground-muted) !important;
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  font-size: 1rem;
  font-weight: 900;
  padding: 4px !important;
  white-space: nowrap;
}

.future-refresh-button :deep(.truncate) {
  color: var(--operator-control-foreground-muted);
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  font-size: 1rem;
  font-weight: 900;
}

@media (max-width: 720px) {
  .future-control-stack {
    width: 100%;
    flex-direction: column;
    align-items: stretch;
    min-width: 0;
  }

  .future-control-stack .future-schedule-source-control,
  .future-baseline-context,
  .future-demo-scenarios {
    min-width: 0;
    width: 100%;
  }

  .console-heading {
    display: grid;
    width: 100%;
    min-width: 0;
  }

  .console-heading > div {
    max-width: 100%;
    min-width: 0;
  }

  .future-refresh-button,
  .status-badge {
    width: 100%;
    justify-content: center;
  }
}
</style>
