<script setup lang="ts">
import type { DefenseRegretLadderRow, DefenseTftPortfolioRow } from '~/types/defense-page'
import {
  CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE,
  CURRENT_TFT_PORTFOLIO_CLOSURE,
  CURRENT_TFT_SAFE_SELECTION_EXPLAINER,
  CURRENT_TFT_USE_DECISION,
  CURRENT_V2_PLUS_IMPROVEMENT_STORY,
  formatUah
} from '~/utils/defenseDataset'

defineProps<{
  regretRows: DefenseRegretLadderRow[]
  tftPortfolioRows: DefenseTftPortfolioRow[]
}>()
</script>

<template>
  <section class="evidence-chart-panel">
    <div class="section-heading">
      <div>
        <p class="eyebrow">
          Evidence charts
        </p>
        <h2>Best results and closed branches</h2>
        <p class="section-explainer">
          Lower regret is better. These charts separate the promoted V2+ result from research branches that were tested
          but did not replace it.
        </p>
      </div>
      <span class="source-pill">strict LP/oracle scoring</span>
    </div>
    <div class="chart-grid">
      <article class="chart-card chart-card-wide">
        <div class="chart-card-header">
          <div>
            <p class="eyebrow">
              Regret ladder
            </p>
            <h3>V2+ is the current low-regret headline</h3>
          </div>
          <strong>{{ formatUah(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.meanRegretUah) }}</strong>
        </div>
        <div class="regret-ladder">
          <div
            v-for="point in regretRows"
            :key="point.label"
            :class="`regret-row regret-row--${point.status}`"
          >
            <div class="regret-row-label">
              <span>{{ point.label }}</span>
              <small>{{ point.note }}</small>
            </div>
            <div class="regret-bar-track">
              <span
                class="regret-bar-fill"
                :style="{ width: `${point.barWidthPercent}%` }"
              />
            </div>
            <strong>{{ formatUah(point.meanRegretUah) }}</strong>
          </div>
        </div>
        <div class="calibration-explainer">
          <UIcon name="i-lucide-sliders-horizontal" />
          <div>
            <strong>Calibrated means “corrected before scoring”, not “peeked at the answer”.</strong>
            <p>
              V2+ first looks at previous anchors and learns a small horizon-by-horizon correction for forecast bias.
              Then it builds schedules and scores them with the same strict LP/oracle regret gate. Final-holdout
              realized prices are used only to score the result, not to choose the correction.
            </p>
          </div>
        </div>
      </article>

      <article class="chart-card">
        <div class="chart-card-header">
          <div>
            <p class="eyebrow">
              TFT portfolio closure
            </p>
            <h3>Complementary schedules exist, but not robustly</h3>
          </div>
          <strong>{{ CURRENT_TFT_PORTFOLIO_CLOSURE.rollingPassCount }}/{{ CURRENT_TFT_PORTFOLIO_CLOSURE.rollingWindowCount }}</strong>
        </div>
        <div class="portfolio-diagnostic-list">
          <div
            v-for="point in tftPortfolioRows"
            :key="point.label"
            :class="`portfolio-diagnostic portfolio-diagnostic--${point.status}`"
          >
            <div>
              <span>{{ point.label }}</span>
              <strong>{{ point.numerator }}/{{ point.denominator }}</strong>
              <small>{{ point.note }}</small>
            </div>
            <div class="portfolio-track">
              <span :style="{ width: `${point.barWidthPercent}%` }" />
            </div>
            <em>{{ point.percentLabel }}</em>
          </div>
        </div>
      </article>
    </div>

    <div class="v2-plus-improvement-panel">
      <div class="section-heading">
        <div>
          <p class="eyebrow">
            Why V2+ beats V2
          </p>
          <h3>Same strict judge, better schedule candidates</h3>
          <p class="section-explainer">
            V2+ did not weaken the benchmark and did not claim raw forecast superiority. It improved the decision
            layer by adding prior-safe schedule families around the failure modes found after V2, while keeping V2
            as fallback.
          </p>
        </div>
        <span class="source-pill">206.37 -> 174.77 UAH</span>
      </div>
      <div class="v2-plus-improvement-grid">
        <article
          v-for="point in CURRENT_V2_PLUS_IMPROVEMENT_STORY"
          :key="point.label"
          :class="`v2-plus-improvement-card v2-plus-improvement-card--${point.status}`"
        >
          <span>{{ point.label }}</span>
          <strong>{{ point.value }}</strong>
          <small>{{ point.englishBody }}</small>
          <em>{{ point.ukrainianBody }}</em>
        </article>
      </div>
    </div>

    <div class="tft-use-panel">
      <div class="section-heading">
        <div>
          <p class="eyebrow">
            Can TFT be used?
          </p>
          <h3>TFT is useful as candidate diversity, not as the selected policy yet</h3>
          <p class="section-explainer">
            The important nuance is timing. The 24 winning TFT schedules are known after realized prices are scored.
            A live or offline-promoted selector must know before the window starts, using only prior features.
          </p>
        </div>
        <span class="source-pill">no final-holdout leakage</span>
      </div>
      <div class="tft-use-grid">
        <article
          v-for="decision in CURRENT_TFT_USE_DECISION"
          :key="decision.label"
          :class="`tft-use-card tft-use-card--${decision.status}`"
        >
          <span>{{ decision.label }}</span>
          <strong>{{ decision.value }}</strong>
          <small>{{ decision.body }}</small>
        </article>
      </div>
      <div class="tft-safe-selection-panel">
        <div class="tft-safe-selection-heading">
          <p class="eyebrow">
            Why the 24 TFT wins are not selected yet
          </p>
          <h4>Good hindsight schedules are not enough for a safe selector</h4>
          <p>
            The selector must decide before the target hours begin. A schedule that is known to be good only after
            realized prices are scored is useful diagnostic evidence, not a safe promotion rule.
          </p>
        </div>
        <div class="tft-safe-selection-grid">
          <article
            v-for="item in CURRENT_TFT_SAFE_SELECTION_EXPLAINER"
            :key="item.label"
            :class="`tft-safe-selection-card tft-safe-selection-card--${item.status}`"
          >
            <span>{{ item.label }}</span>
            <div class="tft-safe-language-row">
              <div>
                <strong>{{ item.englishTitle }}</strong>
                <small>{{ item.englishBody }}</small>
              </div>
              <div>
                <strong>{{ item.ukrainianTitle }}</strong>
                <small>{{ item.ukrainianBody }}</small>
              </div>
            </div>
          </article>
        </div>
      </div>
    </div>
  </section>
</template>
