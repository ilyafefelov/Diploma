<script setup lang="ts">
import { computed } from 'vue'

import type { TenantSummary } from '~/types/control-plane'
import type { OperatorNavItem } from '~/types/operator-dashboard'

const props = defineProps<{
  tenants: TenantSummary[]
  selectedTenantId: string
  navItems: OperatorNavItem[]
  activeRegistrySummary: string
}>()

const emit = defineEmits<{
  'update:selectedTenantId': [value: string]
}>()

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
  minLat: 44.0,
  maxLat: 52.6,
  minLon: 22.0,
  maxLon: 40.2
}

type TenantMapMarker = TenantSummary & {
  left: number
  top: number
  isSelected: boolean
}

const clamp01 = (value: number): number => Math.min(1, Math.max(0, value))

const tenantMarkers = computed<TenantMapMarker[]>(() => {
  const latSpan = ukraineMapBounds.maxLat - ukraineMapBounds.minLat
  const lonSpan = ukraineMapBounds.maxLon - ukraineMapBounds.minLon

  return props.tenants.map(tenant => {
    const left = clamp01((tenant.longitude - ukraineMapBounds.minLon) / lonSpan) * 100
    const top = clamp01((ukraineMapBounds.maxLat - tenant.latitude) / latSpan) * 100

    return {
      ...tenant,
      left,
      top,
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
          <span class="tenant-card__meta-item">
            {{ tenantCoordinates }}
            <span
              class="tenant-card__meta-tooltip"
              role="tooltip"
            >
              <span>Coordinate pair in decimal degrees from the selected tenant record.</span>
              <span>Formula: latitude and longitude are lat/lon of the mapped installation.</span>
            </span>
          </span>
        </div>
        <div
          class="tenant-card__ukraine-map"
          role="img"
          aria-label="Active tenant and sites on Ukraine map"
        >
          <p class="tenant-card__selection-info">
            <span class="tenant-card__selection-info__title">
              {{ selectedTenant ? 'Active lot' : 'Select a lot' }}
            </span>
            <span class="tenant-card__selection-info__name">
              {{ selectedTenant ? markerLabel(selectedTenant) : 'No lot selected' }}
            </span>
            <span
              class="tenant-card__selection-info__coords"
              title="Coordinates of the selected tenant"
            >
              {{ tenantCoordinates }}
            </span>
          </p>
          <div class="tenant-card__ukraine-map-surface">
            <img
              class="tenant-card__ukraine-outline"
              src="/design/ukraine-outline.svg"
              alt="Outline of Ukraine"
            />
            <div
              class="tenant-card__tenant-markers"
              aria-hidden="true"
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
          <ul class="tenant-card__ukraine-map-meta" role="list">
            <li>Legend: active lot = green diamond, others = blue points</li>
            <li>
              Projection:
              <code>x = (lon - 22.0) / (40.2 - 22.0), y = (52.6 - lat) / (52.6 - 44.0)</code>
            </li>
            <li>Markers are interactive: click any point to open that client</li>
          </ul>
        </div>
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
        <article class="tenant-count-card" tabindex="0">
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
        <article class="tenant-count-card" tabindex="0">
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
.tenant-card__ukraine-map {
  margin-top: 0.45rem;
  padding: 0;
  background: transparent;
  border: 0;
  overflow: visible;
  position: relative;
}

.tenant-card__ukraine-map-surface {
  position: relative;
  width: 100%;
  min-height: clamp(12rem, 32vw, 17rem);
  aspect-ratio: 16 / 10;
  overflow: hidden;
  border: 1px solid rgba(163, 255, 132, 0.44);
  border-radius: 1rem;
  isolation: isolate;
  background:
    radial-gradient(circle at 18% 8%, rgba(178, 255, 132, 0.24), transparent 32%),
    linear-gradient(145deg, rgba(6, 36, 83, 0.85), rgba(4, 26, 56, 0.8));
  box-shadow:
    inset 0 2px 0 rgba(255, 255, 255, 0.18),
    inset 0 14px 34px rgba(5, 12, 34, 0.56),
    0 20px 30px rgba(1, 8, 22, 0.48);
  backdrop-filter: blur(10px);
}

.tenant-card__ukraine-map-surface::before {
  content: '';
  position: absolute;
  inset: 0.28rem;
  border: 1px solid rgba(255, 255, 255, 0.12);
  border-radius: 0.78rem;
  box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.05);
  pointer-events: none;
}

.tenant-card__ukraine-outline {
  width: 100%;
  height: 100%;
  object-fit: contain;
  display: block;
  opacity: 0.9;
}

.tenant-card__tenant-markers {
  position: absolute;
  inset: 0;
}

.tenant-card__tenant-marker {
  position: absolute;
  left: 0;
  top: 0;
  transform: translate(-50%, -50%);
  width: 0.72rem;
  height: 0.72rem;
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
  width: 1.4rem;
  height: 1.4rem;
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
  transform: none;
  box-shadow: none;
}

.tenant-card__tenant-marker--active .tenant-card__tenant-marker-icon {
  position: relative;
  border-radius: 0.16rem;
  clip-path: polygon(50% 0, 100% 50%, 50% 100%, 0 50%);
  background:
    linear-gradient(135deg, #8fff76, #2ed46f 58%, #2a9b47);
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
  padding: 0.52rem 0.62rem;
  margin: 0.45rem 0 0;
  color: rgba(226, 255, 236, 0.91);
  font-size: 0.64rem;
  line-height: 1.35;
  border-radius: 0.66rem;
  border: 1px solid rgba(163, 255, 132, 0.32);
  background:
    linear-gradient(180deg, rgba(8, 45, 90, 0.55), rgba(10, 31, 57, 0.6));
  display: grid;
  gap: 0.22rem;
}

.tenant-card__selection-info {
  position: absolute;
  left: 0.55rem;
  top: 0.55rem;
  z-index: 5;
  display: grid;
  gap: 0.18rem;
  margin: 0;
  padding: 0.45rem 0.56rem;
  border: 1px solid rgba(185, 255, 153, 0.35);
  border-radius: 0.62rem;
  backdrop-filter: blur(10px);
  background:
    linear-gradient(180deg, rgba(11, 41, 82, 0.82), rgba(7, 28, 53, 0.82));
  font-size: 0.68rem;
  line-height: 1.25;
  color: rgba(227, 255, 234, 0.95);
  width: max-content;
}

.tenant-card__selection-info__title {
  color: #a1ff87;
  font-size: 0.58rem;
  letter-spacing: 0.02em;
  text-transform: uppercase;
}

.tenant-card__selection-info__name {
  font-weight: 700;
  font-size: 0.72rem;
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
</style>
