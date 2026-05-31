<script setup lang="ts">
import CollapsibleTextCard from '~/components/dashboard/CollapsibleTextCard.vue'

defineProps<{
  explanationMode: 'mvp' | 'future'
}>()
</script>

<template>
  <div class="signal-explainer-grid">
    <CollapsibleTextCard
      v-if="explanationMode === 'mvp'"
      title="How the selected schedule is calculated now"
      eyebrow="Selected strategy adapter"
    >
      <p class="signal-explainer-card__copy">
        The API asks for the selected strategy. For <strong>Offline V2+</strong>, the dashboard uses a frozen
        read-model preview adapter, then runs the same battery feasibility projection before returning the schedule.
      </p>
      <p class="signal-explainer-card__formula">
        Flow: <strong>selected strategy -> preview forecast/context -> feasible schedule -> operator review</strong>
      </p>
      <p class="signal-explainer-card__copy">
        <strong>Positive MW</strong> means discharge, and <strong>negative MW</strong> means charge. The lower dock
        filters idle hours so the next meaningful actions are visible first.
      </p>
    </CollapsibleTextCard>

    <CollapsibleTextCard
      v-else
      title="How dispatch should be decided later"
      eyebrow="Research dispatch logic"
    >
      <p class="signal-explainer-card__copy">
        In the target stack, the action bar should no longer be described as a normalized price-distance heuristic.
        Future DT/LAVA work should predict candidate schedules or schedule blocks first, then compete against V2+.
      </p>
      <p class="signal-explainer-card__formula">
        Research flow: <strong>forecast state + battery state + return target -> candidate schedule trajectory</strong>
      </p>
      <p class="signal-explainer-card__copy">
        At that point, action explanation should describe policy intent, safety constraints, and counterfactual value,
        not only price distance from a local average.
      </p>
    </CollapsibleTextCard>

    <CollapsibleTextCard
      :title="explanationMode === 'mvp' ? 'How value gap is calculated now' : 'What the future opportunity metric should mean'"
      :eyebrow="explanationMode === 'mvp' ? 'Selected value gap' : 'Future opportunity metric'"
      tone="rose"
    >
      <template v-if="explanationMode === 'mvp'">
        <p class="signal-explainer-card__copy">
          The value gap line is an operator-facing counterfactual preview: how much value is visible between the
          selected action and the best visible action at that hour. It is not market settlement revenue.
        </p>
        <p class="signal-explainer-card__formula">
          Interpretation: <strong>value_gap = best_visible_value - selected_action_value</strong>
        </p>
        <p class="signal-explainer-card__copy signal-explainer-card__copy-note">
          This value helps explain the preview schedule. It does not change the claim boundary:
          <strong>Offline Strategy Promotion</strong>, not live trading.
        </p>
      </template>
      <template v-else>
        <p class="signal-explainer-card__eyebrow">
          Future opportunity metric
        </p>
        <p class="signal-explainer-card__copy">
          In production, the pink line should become an explicit decision-quality metric such as regret against a
          counterfactual optimum, policy value gap, or expected opportunity cost under uncertainty.
        </p>
        <p class="signal-explainer-card__formula">
          Target interpretation: <strong>value_gap = value(best feasible action) - value(chosen action)</strong>
        </p>
        <p class="signal-explainer-card__copy signal-explainer-card__copy-note">
          That shift makes the explanation consistent with candidate-value learning and avoids carrying same-hour heuristic score into a
          stronger decision stack.
        </p>
      </template>
    </CollapsibleTextCard>
  </div>
</template>
