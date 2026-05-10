<script setup lang="ts">
import { computed } from 'vue'

import type { BaselineLpPreview, SignalPreview, TenantSummary } from '~/types/control-plane'
import type { OperatorNavItem } from '~/types/operator-dashboard'

const props = defineProps<{
  tenants: TenantSummary[]
  selectedTenantId: string
  navItems: OperatorNavItem[]
  activeRegistrySummary: string
  signalPreview?: SignalPreview | null
  baselinePreview?: BaselineLpPreview | null
}>()

const emit = defineEmits<{
  'update:selectedTenantId': [value: string]
}>()

const compactBadgeUi = {
  base: 'min-w-0 max-w-full',
  label: 'truncate'
}

const tenantOptions = computed(() => {
  return props.tenants.map(tenant => ({
    label: tenant.name || tenant.tenant_id,
    value: tenant.tenant_id
  }))
})

const selectedTenant = computed(() => {
  return props.tenants.find(tenant => tenant.tenant_id === props.selectedTenantId) || null
})

const tenantCount = computed(() => props.tenants.length)
const criticalTenantCount = computed(() => {
  return props.tenants.filter(tenant => tenant.type === 'critical').length
})

const tenantMeta = computed(() => {
  if (!selectedTenant.value) {
    return 'Awaiting registry'
  }

  return `${selectedTenant.value.type || 'unspecified'} lot`
})

const tenantCoordinates = computed(() => {
  if (!selectedTenant.value) {
    return 'Location pending'
  }

  return `${selectedTenant.value.latitude.toFixed(2)} / ${selectedTenant.value.longitude.toFixed(2)}`
})

const ukraineMapBounds = {
  minLat: 44.386,
  maxLat: 52.375,
  minLon: 22.1404,
  maxLon: 40.2181
}

const ukraineMapViewport = {
  width: 1000,
  height: 720,
  padding: 34
}

type TenantMapMarker = TenantSummary & {
  left: number
  top: number
  isSelected: boolean
}

const clamp = (value: number, min: number, max: number): number => Math.min(max, Math.max(min, value))
const degToRad = (value: number): number => value * Math.PI / 180
const mercatorY = (lat: number): number => Math.log(Math.tan(Math.PI / 4 + degToRad(lat) / 2))
const formatWholeNumber = (value: number): string => Math.round(value).toLocaleString('en-US')

const tenantMarkers = computed<TenantMapMarker[]>(() => {
  const minX = degToRad(ukraineMapBounds.minLon)
  const maxX = degToRad(ukraineMapBounds.maxLon)
  const minY = mercatorY(ukraineMapBounds.minLat)
  const maxY = mercatorY(ukraineMapBounds.maxLat)
  const innerWidth = ukraineMapViewport.width - ukraineMapViewport.padding * 2
  const innerHeight = ukraineMapViewport.height - ukraineMapViewport.padding * 2
  const xSpan = maxX - minX
  const ySpan = maxY - minY
  const rawScale = Math.min(innerWidth / xSpan, innerHeight / ySpan)
  const mapWidth = xSpan * rawScale
  const mapHeight = ySpan * rawScale
  const xOffset = (ukraineMapViewport.width - mapWidth) / 2
  const yOffset = (ukraineMapViewport.height - mapHeight) / 2

  return props.tenants.map((tenant) => {
    const longitude = clamp(tenant.longitude, ukraineMapBounds.minLon, ukraineMapBounds.maxLon)
    const latitude = clamp(tenant.latitude, ukraineMapBounds.minLat, ukraineMapBounds.maxLat)
    const x = xOffset + (degToRad(longitude) - minX) * rawScale
    const y = yOffset + (maxY - mercatorY(latitude)) * rawScale

    return {
      ...tenant,
      left: x / ukraineMapViewport.width * 100,
      top: y / ukraineMapViewport.height * 100,
      isSelected: tenant.tenant_id === props.selectedTenantId
    }
  })
})

const markerLabel = (tenant: TenantSummary): string => {
  return tenant.name || tenant.tenant_id
}

const onSelectTenant = (tenantId: string): void => {
  emit('update:selectedTenantId', tenantId)
}

const weatherUpliftValue = computed(() => {
  const currentBias = props.signalPreview?.weather_bias?.[0]

  if (typeof currentBias === 'number') {
    return currentBias
  }

  const values = props.signalPreview?.weather_bias || []

  if (values.length === 0) {
    return null
  }

  return values.reduce((sum, value) => sum + value, 0) / values.length
})

const currentMarketPrice = computed(() => {
  const signalPrice = props.signalPreview?.market_price?.[0]

  if (typeof signalPrice === 'number') {
    return signalPrice
  }

  const baselinePrice = props.baselinePreview?.forecast?.[0]?.predicted_price_uah_mwh

  return typeof baselinePrice === 'number' ? baselinePrice : null
})

const currentWeatherEmoji = computed(() => {
  const uplift = weatherUpliftValue.value

  if (uplift === null) {
    return '🌤️'
  }

  if (uplift >= 120) {
    return '☀️'
  }

  if (uplift >= 40) {
    return '🌤️'
  }

  if (uplift <= -40) {
    return '🌧️'
  }

  return '⛅'
})

const currentWeatherLabel = computed(() => {
  const uplift = weatherUpliftValue.value

  if (uplift === null) {
    return 'Weather pending'
  }

  if (uplift >= 120) {
    return 'Sunny uplift'
  }

  if (uplift >= 40) {
    return 'Mild uplift'
  }

  if (uplift <= -40) {
    return 'Rain drag'
  }

  return 'Stable sky'
})

const weatherUpliftLabel = computed(() => {
  const uplift = weatherUpliftValue.value

  if (uplift === null) {
    return 'Waiting'
  }

  return `${uplift > 0 ? '+' : ''}${formatWholeNumber(uplift)} UAH/MWh`
})

const currentMarketPriceLabel = computed(() => {
  const price = currentMarketPrice.value

  if (price === null) {
    return 'Waiting'
  }

  return `${formatWholeNumber(price)} UAH/MWh`
})

const weatherSourceLabel = computed(() => {
  const source = props.signalPreview?.weather_sources?.[0]

  return source ? source.replaceAll('_', ' ') : 'Source pending'
})
</script>

<template>
  <aside class="operator-sidebar">
    <section class="tenant-card">
      <div
        class="tenant-card__skyline"
        aria-hidden="true"
      >
        <span />
        <span />
        <span />
        <span />
        <span />
      </div>
      <div class="tenant-card__icon">
        <UIcon name="i-lucide-building-2" />
      </div>
      <div class="tenant-card__copy">
        <p class="tenant-card__label">
          Tenant / site
        </p>
        <USelect
          id="tenant-select"
          class="field-select field-select-compact"
          :model-value="selectedTenantId"
          :items="tenantOptions"
          value-key="value"
          label-key="label"
          color="info"
          variant="none"
          size="sm"
          @update:model-value="value => emit('update:selectedTenantId', String(value || ''))"
        />
        <div class="tenant-card__meta">
          <span>{{ tenantMeta }}</span>
          <UTooltip
            text="Coordinate pair in decimal degrees from the selected tenant record."
            :delay-duration="0"
          >
            <span
              class="tenant-card__meta-item"
              tabindex="0"
            >
              {{ tenantCoordinates }}
            </span>
          </UTooltip>
        </div>
      </div>
      <div
        class="tenant-card__ukraine-map"
        role="group"
        aria-label="Active tenant and sites on Ukraine map"
      >
        <div class="tenant-card__location-weather">
          <div
            class="tenant-card__weather-icon"
            aria-hidden="true"
          >
            {{ currentWeatherEmoji }}
          </div>
          <div class="tenant-card__weather-copy">
            <div class="tenant-card__weather-row">
              <span class="tenant-card__selection-info__name">
                {{ selectedTenant ? markerLabel(selectedTenant) : 'No lot selected' }}
              </span>
              <UBadge
                :label="currentWeatherLabel"
                icon="i-lucide-cloud-sun"
                color="success"
                variant="soft"
                size="xs"
                :ui="compactBadgeUi"
              />
            </div>
            <div class="tenant-card__weather-stats">
              <UBadge
                :label="currentMarketPriceLabel"
                icon="i-lucide-zap"
                color="warning"
                variant="subtle"
                size="xs"
                :ui="compactBadgeUi"
              />
              <UBadge
                :label="weatherUpliftLabel"
                icon="i-lucide-cloud-sun"
                color="success"
                variant="subtle"
                size="xs"
                :ui="compactBadgeUi"
              />
              <UBadge
                :label="tenantCoordinates"
                icon="i-lucide-crosshair"
                color="info"
                variant="subtle"
                size="xs"
                :ui="compactBadgeUi"
              />
            </div>
            <p class="tenant-card__weather-source">
              {{ weatherSourceLabel }}
            </p>
          </div>
        </div>
        <div class="tenant-card__ukraine-map-surface">
          <img
            class="tenant-card__ukraine-outline"
            src="/design/ukraine-outline.svg"
            alt="Outline of Ukraine"
          >
          <div
            class="tenant-card__tenant-markers"
          >
            <button
              v-for="marker in tenantMarkers"
              :key="marker.tenant_id"
              class="tenant-card__tenant-marker"
              :class="{ 'tenant-card__tenant-marker--active': marker.isSelected }"
              :style="{ left: `${marker.left}%`, top: `${marker.top}%` }"
              type="button"
              :aria-label="`Select tenant ${markerLabel(marker)}`"
              @click="onSelectTenant(marker.tenant_id)"
              @keydown.enter.prevent="onSelectTenant(marker.tenant_id)"
              @keydown.space.prevent="onSelectTenant(marker.tenant_id)"
            >
              <span
                aria-hidden="true"
                class="tenant-card__tenant-marker-icon"
              />
            </button>
          </div>
        </div>
        <ul
          class="tenant-card__ukraine-map-meta"
          role="list"
        >
          <li>
            <UBadge
              label="Active"
              icon="i-lucide-gem"
              color="success"
              variant="solid"
              size="xs"
              :ui="compactBadgeUi"
            />
            <span>green diamond</span>
            <UBadge
              label="Other"
              icon="i-lucide-circle-dot"
              color="info"
              variant="subtle"
              size="xs"
              :ui="compactBadgeUi"
            />
            <span>blue points</span>
          </li>
          <li>
            <UIcon name="i-lucide-mouse-pointer-click" />
            <span>Click any point to open that client</span>
          </li>
        </ul>
      </div>
    </section>

    <nav
      class="hud-nav"
      aria-label="Operator dashboard sections"
    >
      <UButton
        v-for="item in navItems"
        :key="item.label"
        class="hud-nav__item"
        :class="{ 'hud-nav__item-active': item.active }"
        :icon="item.icon"
        :label="item.label"
        color="info"
        variant="ghost"
        block
      />
    </nav>

    <section class="sidebar-status-card">
      <div>
        <p class="sidebar-status-card__label">
          Connection
        </p>
        <p class="sidebar-status-card__value">
          {{ activeRegistrySummary }}
        </p>
        <span
          class="sidebar-status-card__tooltip"
          role="tooltip"
        >
          <span>Formula: connection_health = live_tenants / total_tenants</span>
          <span>Current values are derived from tenant registry heartbeat and active flow error counters.</span>
        </span>
      </div>
      <div class="sidebar-status-card__mini-grid">
        <article
          class="tenant-count-card"
          tabindex="0"
        >
          <span>Total</span>
          <strong>{{ tenantCount }}</strong>
          <small>mapped lots</small>
          <span
            class="tenant-count-card__tooltip"
            role="tooltip"
          >
            <span class="tenant-count-card__title">Mapped lots</span>
            <span>{{ tenantCount }} tenants are currently loaded in the current registry snapshot.</span>
          </span>
        </article>
        <article
          class="tenant-count-card"
          tabindex="0"
        >
          <span>Critical</span>
          <strong>{{ criticalTenantCount }}</strong>
          <small>alert level</small>
          <span
            class="tenant-count-card__tooltip"
            role="tooltip"
          >
            <span class="tenant-count-card__title">Critical sites</span>
            <span>Critical tenants are shown separately for operator prioritization and guardrail checks.</span>
          </span>
        </article>
      </div>
      <span class="signal-bars">
        <i />
        <i />
        <i />
      </span>
    </section>

    <UButton
      class="report-link"
      to="/week1/interactive_report1"
      icon="i-lucide-book-open-check"
      label="Week 1 report"
      color="info"
      variant="ghost"
      block
    />
  </aside>
</template>

<style scoped>
.tenant-card {
  border-color: rgba(166, 245, 255, 0.32);
  background:
    linear-gradient(180deg, rgba(8, 26, 45, 0.82), rgba(3, 13, 31, 0.88));
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.2),
    inset 0 -1px 0 rgba(14, 255, 154, 0.12),
    0 16px 34px rgba(0, 16, 36, 0.34);
  backdrop-filter: blur(18px) saturate(1.25);
}

.tenant-card__ukraine-map {
  grid-column: 1 / -1;
  width: calc(100% + 1.44rem);
  margin-top: 0.54rem;
  margin-inline: -0.72rem;
  padding: 0;
  background: transparent;
  border: 0;
  overflow: visible;
  position: relative;
}

.tenant-card__ukraine-map-surface {
  position: relative;
  width: 100%;
  min-height: 8.35rem;
  aspect-ratio: 1 / 0.72;
  padding-inline: 4px;
  overflow: hidden;
  border-top: 1px solid rgba(141, 244, 255, 0.34);
  border-bottom: 1px solid rgba(61, 255, 152, 0.22);
  border-inline: 0;
  border-radius: 0;
  isolation: isolate;
  background:
    radial-gradient(circle at 56% 54%, rgba(57, 255, 152, 0.08), transparent 34%),
    linear-gradient(180deg, rgba(4, 22, 43, 0.78), rgba(1, 8, 23, 0.94));
  box-shadow:
    inset 0 10px 26px rgba(2, 9, 26, 0.62),
    inset 0 -14px 24px rgba(36, 255, 149, 0.06),
    inset 0 0 0 1px rgba(141, 244, 255, 0.08);
  backdrop-filter: blur(10px);
}

.tenant-card__ukraine-map-surface::before {
  content: '';
  position: absolute;
  inset: 0;
  background:
    linear-gradient(rgba(141, 244, 255, 0.05) 1px, transparent 1px),
    linear-gradient(90deg, rgba(141, 244, 255, 0.04) 1px, transparent 1px);
  background-size: 2.2rem 2.2rem;
  mask-image: radial-gradient(circle at 50% 50%, black 32%, transparent 78%);
  pointer-events: none;
}

.tenant-card__ukraine-outline {
  width: 100%;
  height: 100%;
  object-fit: contain;
  display: block;
  opacity: 0.96;
  filter:
    drop-shadow(0 0 8px rgba(141, 244, 255, 0.58))
    drop-shadow(0 0 18px rgba(35, 205, 255, 0.22));
}

.tenant-card__tenant-markers {
  position: absolute;
  inset: 0 4px;
}

.tenant-card__tenant-marker {
  position: absolute;
  left: 0;
  top: 0;
  transform: translate(-50%, -50%);
  width: 0.62rem;
  height: 0.62rem;
  min-width: 0;
  min-height: 0;
  padding: 0;
  border: 2px solid rgba(222, 249, 255, 0.95);
  border-radius: 999px;
  background:
    radial-gradient(circle at 26% 24%, rgba(204, 237, 255, 0.95), rgba(19, 126, 226, 0.94) 68%);
  box-shadow:
    0 0 0 2px rgba(10, 51, 97, 0.44),
    0 6px 12px rgba(3, 10, 24, 0.45);
  cursor: pointer;
  outline: none;
  transform-origin: center;
  transition: transform 0.28s ease, box-shadow 0.28s ease;
  z-index: 3;
}

.tenant-card__tenant-marker:hover,
.tenant-card__tenant-marker:focus-visible {
  transform: translate(-50%, -50%) scale(1.28);
  box-shadow:
    0 0 0 3px rgba(78, 255, 114, 0.72),
    0 10px 20px rgba(12, 24, 58, 0.55);
}

.tenant-card__tenant-marker-icon {
  display: block;
  width: 100%;
  height: 100%;
  border-radius: inherit;
}

.tenant-card__tenant-marker--active {
  width: 1.55rem;
  height: 1.55rem;
  border: 0;
  background: transparent;
  animation:
    sims-active-drift 3.4s ease-in-out infinite;
  filter:
    drop-shadow(0 14px 16px rgba(20, 58, 17, 0.44));
  z-index: 4;
}

.tenant-card__tenant-marker--active:hover,
.tenant-card__tenant-marker--active:focus-visible {
  animation:
    sims-active-hover-bounce 0.72s ease-in-out infinite;
  filter:
    drop-shadow(0 16px 20px rgba(66, 255, 73, 0.46));
}

.tenant-card__tenant-marker--active .tenant-card__tenant-marker-icon {
  position: relative;
  border-radius: 0.16rem;
  clip-path: polygon(50% 0, 100% 50%, 50% 100%, 0 50%);
  background:
    linear-gradient(135deg, #b6ff38 0%, #35f56f 48%, #119346 100%);
  box-shadow:
    inset -6px -6px 14px rgba(8, 48, 13, 0.35),
    inset 3px 4px 12px rgba(255, 255, 255, 0.52);
  animation:
    sims-active-spin 4.6s linear infinite,
    sims-active-glow 2.4s ease-in-out infinite alternate;
}

.tenant-card__tenant-marker--active .tenant-card__tenant-marker-icon::after {
  content: '';
  position: absolute;
  left: 50%;
  top: 78%;
  width: 56%;
  height: 14%;
  transform: translateX(-50%) rotate(45deg);
  border-radius: 999px;
  background: rgba(2, 37, 10, 0.48);
  filter: blur(2px);
  z-index: -1;
}

.tenant-card__ukraine-map-meta {
  list-style: none;
  padding: 0.42rem 0.72rem 0;
  margin: 0;
  color: rgba(218, 249, 255, 0.78);
  font-size: 0.58rem;
  line-height: 1.35;
  border: 0;
  background: transparent;
  display: flex;
  gap: 0.42rem;
  overflow: hidden;
  flex-wrap: wrap;
}

.tenant-card__ukraine-map-meta li {
  display: flex;
  align-items: center;
  gap: 0.32rem;
  min-width: 0;
  flex-wrap: wrap;
}

.tenant-card__ukraine-map-meta .icon {
  flex: 0 0 auto;
  color: #b8ff32;
}

.tenant-card__location-weather {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  gap: 0.52rem;
  margin: 0 0.72rem 0.48rem;
  padding: 0.55rem 0.58rem;
  border: 1px solid rgba(164, 246, 255, 0.24);
  border-radius: 0.72rem;
  background: rgba(1, 15, 35, 0.46);
  box-shadow:
    inset 0 1px 0 rgba(255, 255, 255, 0.14),
    0 12px 26px rgba(0, 9, 25, 0.26);
  backdrop-filter: blur(14px) saturate(1.15);
}

.tenant-card__weather-icon {
  display: grid;
  width: 2rem;
  height: 2rem;
  place-items: center;
  border: 1px solid rgba(255, 255, 255, 0.18);
  border-radius: 0.64rem;
  background:
    linear-gradient(180deg, rgba(22, 76, 112, 0.82), rgba(6, 26, 50, 0.86));
  font-size: 1.12rem;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.2);
}

.tenant-card__weather-copy {
  display: grid;
  min-width: 0;
  gap: 0.3rem;
}

.tenant-card__weather-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  min-width: 0;
  gap: 0.35rem;
}

.tenant-card__weather-stats {
  display: flex;
  gap: 0.26rem;
  flex-wrap: wrap;
  min-width: 0;
}

.tenant-card__weather-source {
  margin: 0;
  color: rgba(220, 246, 255, 0.62);
  font-size: 0.58rem;
  line-height: 1.2;
  overflow: hidden;
  text-overflow: ellipsis;
  text-transform: capitalize;
  white-space: nowrap;
}

.tenant-card__selection-info__name {
  font-weight: 700;
  font-size: 0.72rem;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.tenant-card__selection-info__coords {
  color: rgba(227, 255, 234, 0.77);
  font-family:
    ui-monospace,
    SFMono-Regular,
    Menlo,
    Monaco,
    Consolas,
    "Liberation Mono",
    "Courier New",
    monospace;
  font-size: 0.64rem;
}

:deep(.tenant-card__location-weather .badge),
:deep(.tenant-card__ukraine-map-meta .badge) {
  max-width: 100%;
}

code {
  font-family:
    ui-monospace,
    SFMono-Regular,
    Menlo,
    Monaco,
    Consolas,
    "Liberation Mono",
    "Courier New",
    monospace;
  font-size: 0.58rem;
  color: #c9ffdb;
}

@keyframes sims-active-drift {
  0%,
  100% {
    transform: translate(-50%, -50%) rotate(-6deg);
  }
  25% {
    transform: translate(-50%, -50%) rotate(-2deg) translateX(-1px);
  }
  50% {
    transform: translate(-50%, -50%) rotate(6deg) translateX(1px);
  }
  75% {
    transform: translate(-50%, -50%) rotate(2deg) translateX(-1px);
  }
}

@keyframes sims-active-spin {
  0% {
    transform: rotate(0deg);
  }
  30% {
    transform: rotate(-8deg);
  }
  50% {
    transform: rotate(8deg);
  }
  80% {
    transform: rotate(0deg);
  }
  100% {
    transform: rotate(12deg);
  }
}

@keyframes sims-active-glow {
  from {
    box-shadow:
      inset -6px -6px 14px rgba(8, 48, 13, 0.35),
      inset 3px 4px 12px rgba(255, 255, 255, 0.52),
      0 0 0 3px rgba(89, 255, 122, 0.22);
  }

  to {
    box-shadow:
      inset -4px -4px 18px rgba(14, 56, 22, 0.28),
      inset 2px 2px 20px rgba(255, 255, 255, 0.7),
      0 0 0 6px rgba(89, 255, 122, 0.38);
  }
}

@keyframes sims-active-hover-bounce {
  0%,
  100% {
    transform: translate(-50%, -50%) rotate(-8deg) scale(1.08);
  }

  35% {
    transform: translate(-50%, -56%) rotate(10deg) scale(1.24);
  }

  70% {
    transform: translate(-50%, -47%) rotate(-4deg) scale(1.14);
  }
}
</style>
