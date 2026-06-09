<script setup lang="ts">
import type { DefenseOfflinePromotionRow } from '~/types/defense-page'
import {
  CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE,
  formatPercent,
  formatUah
} from '~/utils/defenseDataset'

defineProps<{
  readModelLabel: string
  rows: DefenseOfflinePromotionRow[]
}>()
</script>

<template>
  <section class="offline-promotion-panel">
    <div>
      <p class="eyebrow">
        Offline Strategy Promotion
      </p>
      <h2>Current thesis headline remains V2+</h2>
      <p class="section-explainer">
        The strongest current result is Ukrainian-only official global-panel NBEATSx Schedule/Value Learner V2+:
        {{ formatUah(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.meanRegretUah) }} mean regret,
        {{ formatPercent(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.improvementVsStrict) }} better than strict,
        and {{ CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.rollingPassCount }}/{{ CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.rollingWindowCount }}
        rolling windows. It is still read-model evidence only.
      </p>
    </div>
    <div class="offline-promotion-metrics">
      <article>
        <span>Strict baseline</span>
        <strong>{{ formatUah(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.strictMeanRegretUah) }}</strong>
      </article>
      <article>
        <span>Market execution</span>
        <strong>{{ CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.marketExecutionEnabled ? 'enabled' : 'false' }}</strong>
      </article>
      <article>
        <span>Backend gate</span>
        <strong>{{ readModelLabel }}</strong>
      </article>
    </div>
    <div
      v-if="rows.length > 0"
      class="offline-promotion-rows"
    >
      <div class="evidence-scope-note evidence-scope-note--wide">
        <UIcon name="i-lucide-info" />
        <p>
          The fixed V2+ headline above comes from the frozen 365-anchor evidence packet. Rows below are FastAPI
          read-model rows from the available gate endpoint, so NBEATSx/TFT UAH values may reflect older compact or
          source-specific evidence and are kept for traceability, not as the headline comparator.
        </p>
      </div>
      <article
        v-for="row in rows"
        :key="row.source_model_name"
      >
        <span>{{ row.source_model_name }}</span>
        <strong>{{ formatUah(row.latest_selected_mean_regret_uah) }}</strong>
        <small>{{ row.rolling_strict_pass_window_count }}/{{ row.rolling_window_count }} rolling / {{ row.production_promote ? 'read-model promoted' : row.promotion_blocker }}</small>
      </article>
    </div>
  </section>
</template>
