<script setup lang="ts">
import { computed, ref, watchEffect } from 'vue'
import type { PublicBessGovernanceBoundary } from '~/utils/publicBessArtifactTypes'

type PublicRow = Record<string, unknown>
type ActionTone = 'charge' | 'discharge' | 'hold'
type ActionPoint = {
  key: string
  hour: string
  action: string
  shortAction: string
  tone: ActionTone
  price: number | null
  power: number | null
  value: number | null
  soc: number | null
  intensity: number
  sourceLabel: string
  boundaryLabel: string
  isForecast: boolean
  status: string
}

const props = defineProps<{
  realizedSchedule: PublicRow[]
  forecastModels: PublicRow[]
  presetLabel?: string
  deliveryDate?: string
  contactHref: string
  claimBoundary: string
  proposedBidStatus: string
  governance?: PublicBessGovernanceBoundary
}>()

const selectedKey = ref('')

const realizedPriceRange = computed(() => priceRange(
  props.realizedSchedule.map(row => numberValue(row.price_uah_mwh))
))

const primaryForecastModel = computed<PublicRow | null>(() => (
  props.forecastModels.find(model => Array.isArray(model.points) && model.points.length > 0) || null
))

const forecastPointsRaw = computed<PublicRow[]>(() => {
  const points = primaryForecastModel.value?.points
  return Array.isArray(points) ? points.slice(0, 24) as PublicRow[] : []
})

const forecastPriceRange = computed(() => priceRange(
  forecastPointsRaw.value.map(row => numberValue(row.forecast_price_uah_mwh ?? row.price_uah_mwh))
))

const forecastThresholds = computed(() => {
  const prices = forecastPointsRaw.value
    .map(row => numberValue(row.forecast_price_uah_mwh ?? row.price_uah_mwh))
    .filter((value): value is number => Number.isFinite(value))
    .sort((a, b) => a - b)

  if (prices.length === 0) {
    return { low: null, high: null }
  }

  return {
    low: prices[Math.floor((prices.length - 1) * 0.33)] ?? null,
    high: prices[Math.ceil((prices.length - 1) * 0.67)] ?? null
  }
})

const realizedPoints = computed<ActionPoint[]>(() => (
  props.realizedSchedule.slice(0, 24).map((row, index) => {
    const power = numberValue(row.net_power_mw)
    const price = numberValue(row.price_uah_mwh)
    const soc = numberValue(row.soc_after_mwh)
    const value = numberValue(row.net_value_uah)
    const tone = actionToneFromPower(power)
    const action = actionLabelFromTone(tone)

    return {
      key: `realized-${index}`,
      hour: hourLabel(row.timestamp, index),
      action,
      shortAction: action,
      tone,
      price,
      power,
      value,
      soc,
      intensity: intensityFor(price, realizedPriceRange.value),
      sourceLabel: 'Perfect-hindsight analytical schedule',
      boundaryLabel: 'Historical index evidence',
      isForecast: false,
      status: 'official DAM rows'
    }
  })
))

const previewPoints = computed<ActionPoint[]>(() => (
  forecastPointsRaw.value.slice(0, 2).map((row, index) => {
    const price = numberValue(row.forecast_price_uah_mwh ?? row.price_uah_mwh)
    const tone = forecastToneFromPrice(price)
    const action = forecastWatchLabelFromTone(tone)

    return {
      key: `forecast-${index}`,
      hour: hourLabel(row.timestamp, index),
      action,
      shortAction: action,
      tone,
      price,
      power: null,
      value: null,
      soc: null,
      intensity: intensityFor(price, forecastPriceRange.value),
      sourceLabel: String(primaryForecastModel.value?.label || primaryForecastModel.value?.model_name || 'Forecast model'),
      boundaryLabel: 'Forecast price-signal preview',
      isForecast: true,
      status: String(row.point_in_time_status || primaryForecastModel.value?.point_in_time_status || 'pending score')
    }
  })
))

const hasPreview = computed(() => previewPoints.value.length > 0)
const allPoints = computed(() => [...realizedPoints.value, ...previewPoints.value])

const selectedPoint = computed(() => (
  allPoints.value.find(point => point.key === selectedKey.value)
  || previewPoints.value[0]
  || realizedPoints.value.find(point => point.tone !== 'hold')
  || realizedPoints.value[0]
  || null
))

const actionCounts = computed(() => realizedPoints.value.reduce(
  (counts, point) => {
    counts[point.tone] += 1
    return counts
  },
  { charge: 0, discharge: 0, hold: 0 } as Record<ActionTone, number>
))

const previewStatus = computed(() => {
  if (!primaryForecastModel.value) {
    return 'Forecast artifact pending'
  }

  return String(primaryForecastModel.value.backend_status || primaryForecastModel.value.point_in_time_status || 'published preview')
})

const claimBoundaryLabel = computed(() => receiptLabel(props.claimBoundary || 'public_bess_arbitrage_index_not_market_execution'))
const proposedBidStatusLabel = computed(() => receiptLabel(props.proposedBidStatus || 'not_emitted'))

const chipStyleFor = (point: ActionPoint) => ({
  '--bess-action-fill': `${Math.round(18 + point.intensity * 34)}px`
})

watchEffect(() => {
  if (selectedKey.value && allPoints.value.some(point => point.key === selectedKey.value)) {
    return
  }

  selectedKey.value = previewPoints.value[0]?.key || realizedPoints.value.find(point => point.tone !== 'hold')?.key || realizedPoints.value[0]?.key || ''
})

function setSelectedPoint(point: ActionPoint): void {
  selectedKey.value = point.key
}

function actionToneFromPower(power: number | null): ActionTone {
  if (power === null || !Number.isFinite(power)) {
    return 'hold'
  }
  if (power > 0.0001) {
    return 'discharge'
  }
  if (power < -0.0001) {
    return 'charge'
  }
  return 'hold'
}

function forecastToneFromPrice(price: number | null): ActionTone {
  const { low, high } = forecastThresholds.value
  if (price === null || low === null || high === null || !Number.isFinite(price) || !Number.isFinite(low) || !Number.isFinite(high)) {
    return 'hold'
  }
  if (price <= low) {
    return 'charge'
  }
  if (price >= high) {
    return 'discharge'
  }
  return 'hold'
}

function actionLabelFromTone(tone: ActionTone): string {
  if (tone === 'charge') {
    return 'Charge'
  }
  if (tone === 'discharge') {
    return 'Discharge'
  }
  return 'Hold'
}

function forecastWatchLabelFromTone(tone: ActionTone): string {
  if (tone === 'charge') {
    return 'Charge watch'
  }
  if (tone === 'discharge') {
    return 'Discharge watch'
  }
  return 'Hold watch'
}

function priceRange(values: Array<number | null>): { min: number, max: number } {
  const clean = values.filter((value): value is number => value !== null && Number.isFinite(value))
  if (clean.length === 0) {
    return { min: 0, max: 1 }
  }

  const min = Math.min(...clean)
  const max = Math.max(...clean)
  return min === max ? { min: min - 1, max: max + 1 } : { min, max }
}

function intensityFor(value: number | null, range: { min: number, max: number }): number {
  if (value === null || !Number.isFinite(value)) {
    return 0.18
  }

  return Math.max(0.12, Math.min(1, (value - range.min) / (range.max - range.min)))
}

function numberValue(value: unknown): number | null {
  const numeric = Number(value)
  return Number.isFinite(numeric) ? numeric : null
}

function formatNumber(value: number | null, digits = 0): string {
  if (value === null || !Number.isFinite(value)) {
    return 'pending'
  }

  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits
  }).format(value)
}

function formatUah(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return 'pending'
  }

  return `${formatNumber(value, 0)} UAH`
}

function formatMw(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return 'read-only'
  }

  return `${formatNumber(value, 3)} MW`
}

function formatMwh(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return 'pending'
  }

  return `${formatNumber(value, 3)} MWh`
}

function hourLabel(timestamp: unknown, fallbackIndex: number): string {
  const raw = String(timestamp || '')
  const match = raw.match(/T(\d{2}):?(\d{2})?/)
  if (match?.[1]) {
    return `${match[1]}:00`
  }

  return `${String(fallbackIndex).padStart(2, '0')}:00`
}

function receiptLabel(value: string): string {
  return value
    .replace(/^public_/, '')
    .replace(/_/g, ' ')
}
</script>

<template>
  <section
    class="bess-action-preview-panel"
    aria-label="Operator-style public action preview"
  >
    <div class="bess-action-preview__header">
      <div>
        <p class="bess-kicker">
          Operator-style preview
        </p>
        <h2>Analytical schedule from the realized index, plus a 2h forecast preview.</h2>
        <p>
          Public read-model only. The chips show yesterday's deterministic analytical schedule and a short
          forecast price-signal preview, not private operator commands.
        </p>
      </div>
      <div
        class="bess-action-preview__summary"
        aria-label="Analytical schedule action count summary"
      >
        <span><i class="is-discharge" />{{ actionCounts.discharge }} discharge</span>
        <span><i class="is-charge" />{{ actionCounts.charge }} charge</span>
        <span><i class="is-hold" />{{ actionCounts.hold }} hold</span>
      </div>
    </div>

    <div class="bess-action-preview__body">
      <div class="bess-action-preview__track-shell">
        <div class="bess-action-preview__track-head">
          <span>Last published 24h analytical schedule</span>
          <strong>{{ presetLabel || 'selected BESS preset' }}</strong>
        </div>

        <div class="bess-action-preview__track-layout">
          <div
            class="bess-action-preview__track"
            role="list"
            aria-label="24 analytical schedule actions"
          >
            <div
              v-for="point in realizedPoints"
              :key="point.key"
              class="bess-action-preview__item"
              role="listitem"
            >
              <button
                type="button"
                :class="[
                  'bess-action-chip',
                  `bess-action-chip--${point.tone}`,
                  { 'is-selected': selectedPoint?.key === point.key }
                ]"
                :style="chipStyleFor(point)"
                :aria-pressed="selectedPoint?.key === point.key"
                :aria-label="`${point.hour}: ${point.action}. ${formatMw(point.power)} at ${formatUah(point.price)} per MWh.`"
                @pointerenter="setSelectedPoint(point)"
                @focus="setSelectedPoint(point)"
                @click="setSelectedPoint(point)"
              >
                <span>{{ point.hour }}</span>
                <strong>{{ point.action }}</strong>
                <em>{{ formatUah(point.price) }}</em>
              </button>
            </div>
          </div>

          <div
            class="bess-action-preview__preview-lane"
            role="list"
            aria-label="Next two forecast preview hours"
          >
            <div
              class="bess-action-preview__gate"
              aria-hidden="true"
            >
              <span>Next 2h</span>
              <strong>Preview</strong>
            </div>

            <template v-if="hasPreview">
              <div
                v-for="point in previewPoints"
                :key="point.key"
                class="bess-action-preview__item bess-action-preview__item--preview"
                role="listitem"
              >
                <button
                  type="button"
                  :class="[
                    'bess-action-chip',
                    'bess-action-chip--preview',
                    `bess-action-chip--${point.tone}`,
                    { 'is-selected': selectedPoint?.key === point.key }
                  ]"
                  :style="chipStyleFor(point)"
                  :aria-pressed="selectedPoint?.key === point.key"
                  :aria-label="`${point.hour}: ${point.action}. Forecast price signal ${formatUah(point.price)} per MWh. No bids generated.`"
                  @pointerenter="setSelectedPoint(point)"
                  @focus="setSelectedPoint(point)"
                  @click="setSelectedPoint(point)"
                >
                  <span>{{ point.hour }}</span>
                  <strong>{{ point.action }}</strong>
                  <em>{{ formatUah(point.price) }}</em>
                </button>
              </div>
            </template>
            <div
              v-else
              class="bess-action-preview__pending"
              role="listitem"
            >
              <span>Preview pending</span>
              <strong>{{ previewStatus }}</strong>
            </div>
          </div>
        </div>
      </div>

      <aside
        v-if="selectedPoint"
        class="bess-action-preview__receipt"
        aria-label="Selected analytical schedule hour"
      >
        <div class="bess-action-preview__receipt-top">
          <span>{{ selectedPoint.isForecast ? 'Preview hour' : 'Selected hour' }}</span>
          <strong>{{ selectedPoint.hour }}</strong>
        </div>
        <dl>
          <div>
            <dt>Action</dt>
            <dd :class="`is-${selectedPoint.tone}`">
              {{ selectedPoint.action }}
            </dd>
          </div>
          <div>
            <dt>Price</dt>
            <dd>{{ formatUah(selectedPoint.price) }}/MWh</dd>
          </div>
          <div>
            <dt>{{ selectedPoint.isForecast ? 'Power' : 'Power' }}</dt>
            <dd>{{ formatMw(selectedPoint.power) }}</dd>
          </div>
          <div>
            <dt>{{ selectedPoint.isForecast ? 'Signal' : 'Net value' }}</dt>
            <dd>{{ selectedPoint.isForecast ? selectedPoint.status : formatUah(selectedPoint.value) }}</dd>
          </div>
          <div>
            <dt>SOC</dt>
            <dd>{{ selectedPoint.isForecast ? 'not emitted' : formatMwh(selectedPoint.soc) }}</dd>
          </div>
        </dl>
        <p>
          {{ selectedPoint.boundaryLabel }} · {{ selectedPoint.sourceLabel }}
        </p>
      </aside>

      <aside
        v-if="governance"
        class="bess-action-preview__governance"
        aria-label="Declared analytical model boundary"
      >
        <div>
          <p class="bess-kicker">
            Declared analytical model boundary
          </p>
          <h3>Analytical constraints applied</h3>
          <ul>
            <li
              v-for="constraint in governance.enforced_constraints"
              :key="constraint"
            >
              {{ receiptLabel(constraint) }}
            </li>
          </ul>
        </div>
        <div>
          <h3>Not modeled in the public index</h3>
          <ul>
            <li
              v-for="constraint in governance.not_modeled_constraints"
              :key="constraint"
            >
              {{ receiptLabel(constraint) }}
            </li>
          </ul>
        </div>
        <p>Declared metadata, not a source-publication receipt.</p>
      </aside>

      <aside class="bess-action-preview__lead">
        <UIcon name="i-lucide-sparkles" />
        <div>
          <p class="bess-kicker">
            Want this calibrated?
          </p>
          <h3>Turn the public read model into a private facility review.</h3>
          <p>
            Load profile, PV/BESS specs, tariff or market scope, and operating constraints can become
            a private savings and schedule-selection review.
          </p>
        </div>
        <a
          class="bess-action-preview__cta"
          :href="contactHref"
        >
          <span>Discuss setup</span>
          <UIcon name="i-lucide-arrow-right" />
        </a>
        <dl class="bess-action-preview__boundary">
          <div>
            <dt>Claim boundary</dt>
            <dd>{{ claimBoundaryLabel }}</dd>
          </div>
          <div>
            <dt>Bid status</dt>
            <dd>{{ proposedBidStatusLabel }}</dd>
          </div>
        </dl>
      </aside>
    </div>
  </section>
</template>

<style scoped>
.bess-action-preview-panel {
  display: grid;
  gap: 14px;
  border: 1px solid rgba(30, 121, 202, 0.28);
  border-radius: 8px;
  background:
    linear-gradient(90deg, rgba(16, 103, 192, 0.08) 1px, transparent 1px),
    linear-gradient(180deg, rgba(16, 103, 192, 0.06) 1px, transparent 1px),
    linear-gradient(180deg, rgba(255, 255, 255, 0.92), rgba(240, 250, 255, 0.78));
  background-size: 36px 36px, 36px 36px, auto;
  padding: 14px;
  box-shadow:
    0 18px 42px rgba(19, 75, 130, 0.08),
    inset 0 1px 0 rgba(255, 255, 255, 0.82);
}

.bess-action-preview__header {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 18px;
  align-items: end;
}

.bess-action-preview__header h2,
.bess-action-preview__lead h3 {
  margin: 0;
  color: var(--bess-ink, #082b55);
  font-family: var(--bess-font-display, "Advent Pro", sans-serif);
  font-weight: 800;
  letter-spacing: 0;
}

.bess-action-preview__header h2 {
  font-size: clamp(24px, 2.2vw, 34px);
  line-height: 1.02;
}

.bess-action-preview__header p,
.bess-action-preview__lead p,
.bess-action-preview__receipt p {
  margin: 6px 0 0;
  max-width: 820px;
  color: rgba(8, 43, 85, 0.72);
  font-family: var(--bess-font-detail, "Noto Sans", sans-serif);
  font-size: 14px;
  line-height: 1.45;
}

.bess-action-preview__summary {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 6px;
  min-width: min(100%, 330px);
}

.bess-action-preview__summary span {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  border: 1px solid rgba(66, 142, 209, 0.24);
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.72);
  padding: 6px 10px;
  color: rgba(8, 43, 85, 0.78);
  font-family: var(--bess-font-data, "Anonymous Pro", monospace);
  font-size: 12px;
  font-weight: 800;
}

.bess-action-preview__summary i {
  width: 9px;
  height: 9px;
  border-radius: 999px;
}

.bess-action-preview__summary .is-discharge,
.bess-action-chip--discharge::before,
.bess-action-preview__receipt dd.is-discharge {
  color: #057b57;
}

.bess-action-preview__summary .is-discharge {
  background: #2fbf8b;
}

.bess-action-preview__summary .is-charge {
  background: #f0bb46;
}

.bess-action-preview__summary .is-hold {
  background: #3ca8e8;
}

.bess-action-preview__body {
  display: grid;
  grid-template-columns: minmax(680px, 1fr) minmax(210px, 0.25fr) minmax(300px, 0.35fr);
  gap: 12px;
  align-items: start;
}

.bess-action-preview__track-shell,
.bess-action-preview__receipt,
.bess-action-preview__lead {
  min-width: 0;
  border: 1px solid rgba(30, 121, 202, 0.22);
  border-radius: 8px;
  background: rgba(255, 255, 255, 0.68);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.8);
}

.bess-action-preview__track-shell {
  display: grid;
  grid-template-rows: auto auto;
  align-content: start;
  gap: 9px;
  padding: 10px;
  overflow: hidden;
}

.bess-action-preview__track-head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  min-height: 24px;
  align-items: center;
  color: rgba(8, 43, 85, 0.68);
  font-family: var(--bess-font-data, "Anonymous Pro", monospace);
  font-size: 12px;
  line-height: 1.15;
  font-weight: 800;
  text-transform: uppercase;
}

.bess-action-preview__track-head strong {
  max-width: 42%;
  color: #0b63c7;
  font-size: 12px;
  line-height: 1.15;
  text-align: right;
  overflow-wrap: anywhere;
}

.bess-action-preview__track-layout {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(300px, 0.38fr);
  gap: 10px;
  align-items: stretch;
}

.bess-action-preview__track {
  display: flex;
  align-items: stretch;
  gap: 6px;
  min-width: 0;
  min-height: 112px;
  overflow-x: auto;
  overflow-y: hidden;
  padding: 6px 2px 8px;
  scroll-snap-type: inline proximity;
  scrollbar-color: rgba(13, 111, 202, 0.45) rgba(218, 238, 248, 0.82);
  scrollbar-width: thin;
}

.bess-action-preview__preview-lane {
  display: flex;
  align-items: stretch;
  gap: 6px;
  min-width: 0;
  border: 1px dashed rgba(11, 99, 199, 0.28);
  border-radius: 7px;
  background:
    linear-gradient(90deg, rgba(12, 126, 179, 0.1), transparent 42%),
    rgba(255, 255, 255, 0.46);
  padding: 6px;
}

.bess-action-chip {
  --bess-action-color: #3ca8e8;
  --bess-action-soft: rgba(60, 168, 232, 0.14);
  position: relative;
  display: grid;
  flex: 0 0 78px;
  height: 112px;
  min-height: 112px;
  gap: 5px;
  justify-items: start;
  overflow: hidden;
  border: 1px solid color-mix(in srgb, var(--bess-action-color) 42%, white);
  border-radius: 7px;
  background:
    linear-gradient(180deg, var(--bess-action-soft), rgba(255, 255, 255, 0.72)),
    linear-gradient(180deg, rgba(255, 255, 255, 0.8), rgba(232, 246, 255, 0.6));
  padding: 8px 7px;
  color: #082b55;
  font-family: var(--bess-font-data, "Anonymous Pro", monospace);
  text-align: left;
  scroll-snap-align: start;
  transition: transform 160ms ease, box-shadow 160ms ease, border-color 160ms ease, background-color 160ms ease;
}

.bess-action-preview__item {
  display: flex;
  flex: 0 0 78px;
  min-width: 0;
}

.bess-action-preview__item--preview {
  flex-basis: 104px;
}

.bess-action-preview__item > .bess-action-chip {
  width: 100%;
}

.bess-action-chip::before {
  content: "";
  position: absolute;
  inset: auto 7px 7px;
  height: var(--bess-action-fill, 24px);
  border-radius: 5px;
  background: linear-gradient(180deg, color-mix(in srgb, var(--bess-action-color) 70%, white), var(--bess-action-color));
  opacity: 0.22;
  pointer-events: none;
}

.bess-action-chip:hover,
.bess-action-chip:focus-visible,
.bess-action-chip.is-selected {
  z-index: 2;
  transform: translateY(-3px);
  border-color: color-mix(in srgb, var(--bess-action-color) 72%, #0b63c7);
  box-shadow:
    0 12px 22px rgba(15, 91, 156, 0.14),
    0 0 0 4px color-mix(in srgb, var(--bess-action-color) 16%, transparent);
}

.bess-action-chip:focus-visible,
.bess-action-preview__cta:focus-visible {
  outline: 2px solid #0b63c7;
  outline-offset: 3px;
}

.bess-action-chip.is-selected::after {
  content: "";
  position: absolute;
  top: 9px;
  right: 9px;
  width: 11px;
  height: 11px;
  border: 2px solid #fff;
  border-radius: 999px;
  background: var(--bess-action-color);
  box-shadow: 0 0 0 5px color-mix(in srgb, var(--bess-action-color) 18%, transparent);
}

.bess-action-chip--charge {
  --bess-action-color: #e7a92d;
  --bess-action-soft: rgba(240, 187, 70, 0.16);
}

.bess-action-chip--discharge {
  --bess-action-color: #19a976;
  --bess-action-soft: rgba(47, 191, 139, 0.15);
}

.bess-action-chip--hold {
  --bess-action-color: #348fe2;
  --bess-action-soft: rgba(60, 168, 232, 0.14);
}

.bess-action-chip--preview {
  flex-basis: 104px;
  border-style: dashed;
  background:
    linear-gradient(135deg, rgba(13, 111, 202, 0.13), transparent 44%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.82), rgba(233, 249, 255, 0.74));
}

.bess-action-chip span,
.bess-action-chip strong,
.bess-action-chip em {
  position: relative;
  z-index: 1;
}

.bess-action-chip span {
  color: color-mix(in srgb, var(--bess-action-color) 76%, #082b55);
  font-size: 14px;
  font-weight: 900;
}

.bess-action-chip strong {
  font-size: 12px;
  line-height: 1.05;
}

.bess-action-chip em {
  align-self: end;
  color: rgba(8, 43, 85, 0.62);
  font-size: 10px;
  font-style: normal;
  font-weight: 800;
  line-height: 1.1;
}

.bess-action-preview__gate {
  position: relative;
  display: grid;
  place-items: center;
  flex: 0 0 54px;
  min-height: 112px;
  border-right: 1px dashed rgba(11, 99, 199, 0.36);
  color: rgba(8, 43, 85, 0.68);
  font-family: var(--bess-font-data, "Anonymous Pro", monospace);
  text-align: center;
}

.bess-action-preview__gate::before {
  content: "";
  position: absolute;
  inset: 8px 50% 8px auto;
  border-left: 1px solid rgba(11, 99, 199, 0.28);
}

.bess-action-preview__gate span,
.bess-action-preview__gate strong {
  position: relative;
  z-index: 1;
  background: rgba(245, 252, 255, 0.86);
  padding-inline: 4px;
}

.bess-action-preview__gate span {
  font-size: 10px;
  text-transform: uppercase;
}

.bess-action-preview__gate strong {
  color: #0b63c7;
  font-size: 12px;
}

.bess-action-preview__pending {
  display: grid;
  flex: 0 0 190px;
  min-height: 112px;
  align-content: center;
  gap: 5px;
  border: 1px dashed rgba(30, 121, 202, 0.34);
  border-radius: 7px;
  background: rgba(255, 255, 255, 0.52);
  padding: 10px;
  color: rgba(8, 43, 85, 0.68);
  font-family: var(--bess-font-data, "Anonymous Pro", monospace);
}

.bess-action-preview__pending strong {
  color: #0b63c7;
  font-size: 12px;
}

.bess-action-preview__receipt {
  display: grid;
  align-content: start;
  gap: 10px;
  padding: 12px;
}

.bess-action-preview__receipt-top {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  border-bottom: 1px solid rgba(30, 121, 202, 0.16);
  padding-bottom: 8px;
  font-family: var(--bess-font-data, "Anonymous Pro", monospace);
}

.bess-action-preview__receipt-top span {
  color: rgba(8, 43, 85, 0.6);
  font-size: 11px;
  font-weight: 800;
  text-transform: uppercase;
}

.bess-action-preview__receipt-top strong {
  color: #082b55;
  font-size: 24px;
  line-height: 1;
}

.bess-action-preview__receipt dl,
.bess-action-preview__boundary {
  display: grid;
  gap: 7px;
  margin: 0;
}

.bess-action-preview__receipt dl div,
.bess-action-preview__boundary div {
  display: grid;
  grid-template-columns: minmax(72px, 0.42fr) minmax(0, 1fr);
  gap: 10px;
  align-items: baseline;
}

.bess-action-preview__receipt dt,
.bess-action-preview__boundary dt {
  color: rgba(8, 43, 85, 0.58);
  font-family: var(--bess-font-data, "Anonymous Pro", monospace);
  font-size: 11px;
  font-weight: 900;
  text-transform: uppercase;
}

.bess-action-preview__receipt dd,
.bess-action-preview__boundary dd {
  margin: 0;
  color: #082b55;
  font-family: var(--bess-font-data, "Anonymous Pro", monospace);
  font-size: 12px;
  font-weight: 900;
  text-align: right;
}

.bess-action-preview__receipt dd.is-charge {
  color: #b06d00;
}

.bess-action-preview__receipt dd.is-hold {
  color: #0b63c7;
}

.bess-action-preview__governance {
  grid-column: 1 / -1;
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px 18px;
  border: 1px solid rgba(30, 121, 202, 0.2);
  border-radius: 7px;
  padding: 13px;
  background: rgba(255, 255, 255, 0.64);
}

.bess-action-preview__governance h3 {
  margin: 2px 0 8px;
  color: #082b55;
  font-size: 16px;
}

.bess-action-preview__governance ul {
  display: grid;
  gap: 5px;
  margin: 0;
  padding-left: 18px;
  color: rgba(8, 43, 85, 0.76);
  font-family: var(--bess-font-data, "Anonymous Pro", monospace);
  font-size: 12px;
}

.bess-action-preview__governance > p {
  grid-column: 1 / -1;
  margin: 0;
  color: rgba(8, 43, 85, 0.62);
  font-size: 12px;
}

.bess-action-preview__lead {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  gap: 10px 12px;
  align-content: start;
  padding: 13px;
  background:
    linear-gradient(135deg, rgba(12, 126, 179, 0.16), rgba(255, 255, 255, 0.72) 46%),
    rgba(255, 255, 255, 0.72);
}

.bess-action-preview__lead > .icon {
  width: 28px;
  height: 28px;
  color: #0b63c7;
}

.bess-action-preview__lead h3 {
  margin-top: 2px;
  font-size: 24px;
  line-height: 1.02;
}

.bess-action-preview__lead p {
  font-size: 13px;
}

.bess-action-preview__cta {
  grid-column: 1 / -1;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  min-height: 38px;
  border-radius: 6px;
  background: #0b78d0;
  color: #fff;
  font-family: var(--bess-font-detail, "Noto Sans", sans-serif);
  font-size: 13px;
  font-weight: 800;
  text-decoration: none;
  box-shadow: 0 10px 24px rgba(11, 120, 208, 0.22);
}

.bess-action-preview__cta .icon {
  width: 16px;
  height: 16px;
}

.bess-action-preview__boundary {
  grid-column: 1 / -1;
  border-top: 1px solid rgba(30, 121, 202, 0.16);
  padding-top: 9px;
}

@media (max-width: 1280px) {
  .bess-action-preview__body {
    grid-template-columns: minmax(0, 1fr) minmax(230px, 0.34fr);
  }

  .bess-action-preview__lead {
    grid-column: 1 / -1;
  }

  .bess-action-preview__track-layout {
    grid-template-columns: minmax(0, 1fr);
  }

  .bess-action-preview__preview-lane {
    overflow-x: auto;
  }
}

@media (max-width: 720px) {
  .bess-action-preview__governance {
    grid-template-columns: minmax(0, 1fr);
  }

  .bess-action-preview__governance > p {
    grid-column: 1;
  }
}

@media (max-width: 860px) {
  .bess-action-preview-panel {
    padding: 12px;
  }

  .bess-action-preview__header,
  .bess-action-preview__body {
    grid-template-columns: 1fr;
  }

  .bess-action-preview__summary {
    justify-content: flex-start;
    min-width: 0;
  }

  .bess-action-chip {
    flex-basis: 84px;
    min-height: 82px;
  }

  .bess-action-preview__receipt dl div,
  .bess-action-preview__boundary div {
    grid-template-columns: minmax(0, 1fr);
    gap: 2px;
  }

  .bess-action-preview__receipt dd,
  .bess-action-preview__boundary dd {
    text-align: left;
  }
}
</style>
