<script setup lang="ts">
import type { DefenseModelRow } from '~/types/defense-page'
import { formatPercent, formatUah } from '~/utils/defenseDataset'

defineProps<{
  claimBoundaries: string[]
  modelRows: DefenseModelRow[]
  sourceMode: string
}>()
</script>

<template>
  <section class="section-grid">
    <div class="wide-panel">
      <div class="section-heading">
        <div>
          <p class="eyebrow">
            Legacy FastAPI benchmark context
          </p>
          <h2>104-anchor compact rows, kept for read-model health</h2>
          <p class="section-explainer">
            This table is not the headline 365-anchor V2+ packet. It stays on the defense page as a backend/API
            consistency check for older benchmark rows; the promoted result is summarized in the V2+ cards and charts
            above.
          </p>
        </div>
        <span class="source-pill">{{ sourceMode }}</span>
      </div>

      <div
        v-if="modelRows.length > 0"
        class="table-wrap"
      >
        <table>
          <thead>
            <tr>
              <th>Model</th>
              <th>Role</th>
              <th>Anchors</th>
              <th>Mean regret</th>
              <th>Median regret</th>
              <th class="table-help-cell">
                <span
                  class="table-help"
                  tabindex="0"
                >
                  Win rate
                  <span
                    class="defense-tooltip"
                    role="tooltip"
                  >
                    <strong>Win rate</strong>
                    <span>Share of benchmark anchors where this row ranked first by regret among rows in its returned strategy response.</span>
                    <em>win_rate = count(rank_by_regret = 1) / anchor_count</em>
                  </span>
                </span>
              </th>
              <th>Throughput</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="row in modelRows"
              :key="row.modelName"
            >
              <td>{{ row.modelName }}</td>
              <td>{{ row.role }}</td>
              <td>{{ row.anchorCount }}</td>
              <td>{{ formatUah(row.meanRegretUah) }}</td>
              <td>{{ formatUah(row.medianRegretUah) }}</td>
              <td>{{ formatPercent(row.winRate) }}</td>
              <td>{{ row.meanThroughputMwh.toFixed(3) }} MWh</td>
            </tr>
          </tbody>
        </table>
      </div>
      <p
        v-else
        class="empty-state"
      >
        No benchmark rows returned by FastAPI for this tenant.
      </p>
    </div>

    <aside class="side-panel">
      <p class="eyebrow">
        Claim boundary
      </p>
      <h2>What examiner should not infer</h2>
      <ul class="boundary-list">
        <li
          v-for="boundary in claimBoundaries"
          :key="boundary"
        >
          <UIcon name="i-lucide-shield-check" />
          <span>{{ boundary }}</span>
        </li>
      </ul>
    </aside>
  </section>
</template>
