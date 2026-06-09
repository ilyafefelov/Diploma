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
      title="How the selected market context price is calculated"
      eyebrow="Selected calculation"
    >
      <p class="signal-explainer-card__copy">
        <strong>Expected price</strong> comes from the selected tenant API read model. The visible
        V2+ preview adapter can reuse this context, but the thesis result itself was validated offline on the
        365-anchor Ukrainian panel.
      </p>
      <p class="signal-explainer-card__formula">
        Formula: <strong>price_after_weather = market_price + weather_bias</strong>
      </p>
      <p class="signal-explainer-card__copy">
        <strong>Weather effect</strong> is predicted by a ridge-style calibration model trained on joined
        price-and-weather history for the selected location. The visible features are cloud cover, precipitation,
        humidity above 65%, absolute temperature gap from 18C, effective solar, and wind speed.
      </p>
    </CollapsibleTextCard>

    <CollapsibleTextCard
      v-else
      title="How the research forecast should be used"
      eyebrow="Research forecast calculation"
    >
      <p class="signal-explainer-card__copy">
        In the thesis architecture, raw <strong>NBEATSx</strong> and <strong>TFT</strong> forecasts are useful
        only after they become feasible schedules and pass the strict LP/oracle regret gate.
      </p>
      <p class="signal-explainer-card__formula">
        Evidence flow: <strong>market forecast model -> candidate schedules -> schedule/value gate</strong>
      </p>
      <p class="signal-explainer-card__copy">
        The weather explanation will shift from a single calibrated uplift number to model-driven attribution,
        for example feature importance, attention, uncertainty bands, and scenario-specific forecast deltas.
      </p>
    </CollapsibleTextCard>

    <CollapsibleTextCard
      class="signal-explainer-card-accent"
      :title="explanationMode === 'mvp' ? 'Selected market and weather sources' : 'Future forecast evidence'"
      :eyebrow="explanationMode === 'mvp' ? 'Selected data sources' : 'Research evidence data sources'"
      tone="accent"
    >
      <template v-if="explanationMode === 'mvp'">
        <p class="signal-explainer-card__copy">
          <strong>Price side:</strong> the API uses official/source-backed OREE DAM or IDM rows for published targets.
          Forecast context is only for unpublished horizons and should not re-predict an already published row.
        </p>
        <p class="signal-explainer-card__copy">
          <strong>Weather side:</strong> weather comes from <strong>Open-Meteo</strong> when available; otherwise the
          dashboard should show an explicit weather-readiness gap. The badge above shows which source was used for the
          visible points.
        </p>
        <p class="signal-explainer-card__copy signal-explainer-card__copy-note">
          This explanation is specific to the visible preview path. Thesis evidence is led by V2+
          schedule/value scoring; the dashboard preview is not live market execution.
        </p>
      </template>
      <template v-else>
        <p class="signal-explainer-card__eyebrow">
          Research evidence data sources
        </p>
        <p class="signal-explainer-card__copy">
          <strong>Forecast inputs:</strong> DAM or IDM market history, weather history and forecasts, calendar signals,
          regime context, and possibly cross-market coupling features.
        </p>
        <p class="signal-explainer-card__copy">
          <strong>Explanation surface:</strong> instead of one uplift bar, operators should expect forecast bands,
          feature attribution, and scenario comparisons tied directly to NBEATSx or TFT outputs.
        </p>
        <p class="signal-explainer-card__copy signal-explainer-card__copy-note">
          The visible chart can stay simple, but the explanation contract should move from heuristic uplift to
          model-backed evidence.
        </p>
      </template>
    </CollapsibleTextCard>
  </div>
</template>
