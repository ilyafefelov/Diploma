<script setup lang="ts">
import ClientVChart from '~/components/dashboard/ClientVChart.vue'
import {
  useOperatorDecisionEvidencePanelModel,
  type OperatorDecisionEvidencePanelModelInput
} from '~/composables/useOperatorDecisionEvidencePanelModel'

const props = defineProps<OperatorDecisionEvidencePanelModelInput>()

const {
  comparatorGuideItems,
  comparatorWinNarrative,
  controlTimelineSummary,
  forecastScenarioRows,
  readinessItems,
  readModelBadgeLabel,
  regretTimelineOption,
  sensitivityGuideItems,
  sensitivityOption,
  stateCards,
  strategyOption
} = useOperatorDecisionEvidencePanelModel(props)
</script>

<template>
  <section class="surface-panel operator-decision-panel">
    <div class="console-heading">
      <div>
        <p class="eyebrow">
          Decision evidence
        </p>
        <h2 class="section-title">
          Control, regret, and operating state
        </h2>
      </div>
      <UBadge
        class="status-badge"
        :label="readModelBadgeLabel"
        :color="activeErrorCount > 0 ? 'warning' : 'success'"
        variant="soft"
      />
    </div>

    <div class="decision-state-grid">
      <article
        v-for="card in stateCards"
        :key="card.label"
        class="decision-state-card"
        role="group"
        :aria-label="`${card.label}: ${card.value}. ${card.meta}`"
        tabindex="0"
      >
        <span>{{ card.label }}</span>
        <strong>{{ card.value }}</strong>
        <small>{{ card.meta }}</small>
        <span
          class="decision-state-tooltip"
          role="tooltip"
        >
          <strong>{{ card.tooltipTitle }}</strong>
          <span>{{ card.tooltipBody }}</span>
          <em>{{ card.tooltipFormula }}</em>
        </span>
      </article>
    </div>

    <div class="decision-readiness-strip">
      <article
        v-for="item in readinessItems"
        :key="item.label"
        class="decision-readiness-card"
        :class="`decision-readiness-card--${item.tone}`"
      >
        <span>{{ item.label }}</span>
        <strong>{{ item.status }}</strong>
        <small>{{ item.detail }}</small>
      </article>
    </div>

    <div class="decision-chart-grid">
      <article class="decision-chart-card">
        <div>
          <p class="decision-chart-card__eyebrow">
            Comparator graph
          </p>
          <h3>Mean regret and win rate</h3>
          <p>Blue bars show average regret in UAH. Green line shows how often each model was best on anchor-by-anchor ranking.</p>
        </div>
        <div class="decision-chart-guide">
          <span
            v-for="item in comparatorGuideItems"
            :key="item.label"
          >
            <strong>{{ item.label }}</strong>: {{ item.detail }}
          </span>
        </div>
        <ClientVChart
          :option="strategyOption"
          autoresize
          class="decision-chart"
        />
        <div
          v-if="comparatorWinNarrative"
          class="decision-win-reason"
        >
          <strong>{{ comparatorWinNarrative.headline }}</strong>
          <p>{{ comparatorWinNarrative.detail }}</p>
        </div>
      </article>

      <article class="decision-chart-card decision-chart-card-control">
        <div>
          <p class="decision-chart-card__eyebrow">
            Control graph
          </p>
          <h3>Strict control regret rate</h3>
          <p>Rolling anchor view of the default comparator against the oracle upper bound.</p>
        </div>
        <div class="decision-chart-summary">
          <span
            v-for="item in controlTimelineSummary"
            :key="item.label"
          >
            <strong>{{ item.value }}</strong>
            <small>{{ item.label }} · {{ item.detail }}</small>
          </span>
        </div>
        <ClientVChart
          :option="regretTimelineOption"
          autoresize
          class="decision-chart"
        />
      </article>

      <article class="decision-chart-card decision-chart-card-wide">
        <div>
          <p class="decision-chart-card__eyebrow">
            Regret attribution
          </p>
          <h3>Why the selected schedule loses value</h3>
          <p>This diagnostic chart compares mean regret across the strict baseline, selected V2+ schedule evidence, and forecast context buckets. It explains evidence quality, not live dispatch commands.</p>
        </div>
        <div class="decision-chart-guide">
          <span
            v-for="item in sensitivityGuideItems"
            :key="item.label"
          >
            <strong>{{ item.label }}</strong>: {{ item.detail }}
          </span>
        </div>
        <ClientVChart
          :option="sensitivityOption"
          autoresize
          class="decision-chart decision-chart-compact"
        />
      </article>
    </div>

    <div
      v-if="forecastScenarioRows.length > 0"
      class="forecast-scenario-strip"
    >
      <article
        v-for="candidate in forecastScenarioRows"
        :key="candidate.candidateId"
        class="forecast-scenario-card"
        :class="{ 'forecast-scenario-card--selected': candidate.selectedForPreview }"
      >
        <span>{{ candidate.rankLabel }}</span>
        <strong>{{ candidate.modelName }}</strong>
        <dl>
          <div>
            <dt>Value</dt>
            <dd>{{ candidate.decisionValueLabel }}</dd>
          </div>
          <div>
            <dt>Regret</dt>
            <dd>{{ candidate.regretLabel }}</dd>
          </div>
          <div>
            <dt>Throughput</dt>
            <dd>{{ candidate.throughputLabel }}</dd>
          </div>
          <div>
            <dt>Status</dt>
            <dd>{{ candidate.statusLabel }}</dd>
          </div>
        </dl>
      </article>
    </div>

    <div class="decision-explainer-grid">
      <article>
        <span>Gatekeeper meaning</span>
        <p>
          BUY/SELL/HOLD scores appear only after the selected DAM/IDM recommendation loads. Forecast candidates are
          advisory rank/abstain evidence; ProposedBid, settlement, and market-submission contracts are not emitted here.
        </p>
      </article>
      <article>
        <span>Weather slice meaning</span>
        <p>
          Prepare builds the Dagster run config. Materialize refreshes selected Bronze/Silver sources. Including DAM
          history lets the weather slice join price context for forecast features.
        </p>
      </article>
      <article>
        <span>Business use</span>
        <p>
          Operator should compare physical readiness, expected plan value, regret evidence, and grid risk before treating
          any schedule as a candidate hourly preview.
        </p>
      </article>
    </div>
  </section>
</template>

<style scoped src="../../../assets/css/operator-decision-evidence.css"></style>
