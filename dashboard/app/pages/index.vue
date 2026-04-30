<script setup lang="ts">
import { computed, onMounted } from 'vue'

import TenantRegistryScatter from '~/components/dashboard/TenantRegistryScatter.vue'
import { useControlPlaneRegistry } from '~/composables/useControlPlaneRegistry'

const {
  tenants,
  selectedTenant,
  selectedTenantId,
  isLoading,
  error,
  lastLoadedAt,
  loadTenants,
  clearError
} = useControlPlaneRegistry()

const selectedTenantName = computed(() => {
  if (!selectedTenant.value) {
    return 'No tenant selected'
  }

  return selectedTenant.value.name || selectedTenant.value.tenant_id
})

const selectedTenantType = computed(() => {
  const tenantType = selectedTenant.value?.type
  if (!tenantType) {
    return 'Type unavailable'
  }

  return tenantType.replace(/[_-]+/g, ' ')
})

const lastSyncLabel = computed(() => {
  if (!lastLoadedAt.value) {
    return 'Not loaded yet'
  }

  return new Date(lastLoadedAt.value).toLocaleTimeString('en-GB', {
    hour: '2-digit',
    minute: '2-digit'
  })
})

const tenantTypeMix = computed(() => {
  if (tenants.value.length === 0) {
    return 'No tenant mix available'
  }

  const counts = tenants.value.reduce<Record<string, number>>((accumulator, tenant) => {
    const tenantType = tenant.type || 'unspecified'
    accumulator[tenantType] = (accumulator[tenantType] || 0) + 1
    return accumulator
  }, {})

  return Object.entries(counts)
    .map(([tenantType, count]) => `${tenantType}: ${count}`)
    .join(' / ')
})

const registryEnvelope = computed(() => {
  if (tenants.value.length === 0) {
    return 'Registry envelope unavailable'
  }

  const latitudes = tenants.value.map((tenant) => tenant.latitude)
  const longitudes = tenants.value.map((tenant) => tenant.longitude)
  const latitudeSpan = Math.max(...latitudes) - Math.min(...latitudes)
  const longitudeSpan = Math.max(...longitudes) - Math.min(...longitudes)

  return `${latitudeSpan.toFixed(2)} lat / ${longitudeSpan.toFixed(2)} lon span`
})

const criticalTenantCount = computed(() => {
  return tenants.value.filter((tenant) => tenant.type === 'critical').length
})

const selectedTenantBadge = computed(() => {
  if (!selectedTenant.value) {
    return 'No active lot'
  }

  return `${selectedTenant.value.type || 'unspecified'} lot`
})

const selectedTenantCoordinates = computed(() => {
  if (!selectedTenant.value) {
    return 'Awaiting selection'
  }

  return `${selectedTenant.value.latitude.toFixed(2)} / ${selectedTenant.value.longitude.toFixed(2)}`
})

const activeRegistrySummary = computed(() => {
  if (tenants.value.length === 0) {
    return 'Registry offline'
  }

  return `${tenants.value.length} live tenants / ${criticalTenantCount.value} critical`
})

const refreshRegistry = async (): Promise<void> => {
  await loadTenants()
}

onMounted(async () => {
  if (tenants.value.length === 0) {
    await loadTenants()
  }
})
</script>

<template>
  <div class="workspace-shell">
    <div class="workspace-grid">
      <section class="hero-shell">
        <div class="hero-copy-block surface-panel surface-panel-strong">
          <div class="hero-copy-block__topline">
            <div class="plumbob-chip">
              <span class="plumbob-chip__shape"></span>
              Sims mode / operator HUD
            </div>

            <div class="status-pill">
              <span class="status-pill__dot"></span>
              {{ isLoading ? 'Refreshing registry' : 'Control plane online' }}
            </div>
          </div>

          <div class="hero-copy-block__body">
            <p class="eyebrow">Smart arbitrage / dashboard w1</p>
            <h1 class="workspace-title">Run the tenant registry like a bright simulation HUD.</h1>
            <p class="workspace-copy">
              Clean white glass, Sims-blue actions, plumbob-green feedback, and a playful chart stage for the first
              operator-facing baseline workspace.
            </p>
          </div>

          <div class="hero-copy-block__actions">
            <button class="control-button control-button-primary" type="button" :disabled="isLoading" @click="refreshRegistry">
              {{ isLoading ? 'Refreshing' : 'Refresh registry' }}
            </button>

            <div class="hero-kicker">
              <span class="hero-kicker__label">Registry mood</span>
              <span class="hero-kicker__value">{{ activeRegistrySummary }}</span>
            </div>
          </div>
        </div>

        <aside class="hero-stage surface-panel surface-panel-blue">
          <div class="hero-stage__halo"></div>
          <div class="hero-stage__plumbob"></div>

          <div class="hero-stage__content">
            <p class="eyebrow eyebrow-light">Active lot</p>
            <h2 class="hero-stage__title">{{ selectedTenantName }}</h2>
            <p class="hero-stage__subtitle">{{ selectedTenantBadge }}</p>

            <div class="hero-stage__facts">
              <div>
                <span>Coordinates</span>
                <strong>{{ selectedTenantCoordinates }}</strong>
              </div>
              <div>
                <span>Timezone</span>
                <strong>{{ selectedTenant?.timezone || 'Unavailable' }}</strong>
              </div>
            </div>
          </div>
        </aside>
      </section>

      <section class="workspace-metrics">
        <article class="metric-strip surface-panel metric-strip-blue">
          <p class="metric-strip__label">Tenants</p>
          <p class="metric-strip__value">{{ tenants.length }}</p>
          <p class="metric-strip__meta">Last sync {{ lastSyncLabel }}</p>
        </article>

        <article class="metric-strip surface-panel metric-strip-green">
          <p class="metric-strip__label">Critical sites</p>
          <p class="metric-strip__value">{{ criticalTenantCount }}</p>
          <p class="metric-strip__meta">Priority-safe lots under watch</p>
        </article>

        <article class="metric-strip surface-panel metric-strip-orange">
          <p class="metric-strip__label">Location envelope</p>
          <p class="metric-strip__value metric-strip__value--small">{{ registryEnvelope }}</p>
          <p class="metric-strip__meta">Source: FastAPI tenant registry</p>
        </article>
      </section>

      <section v-if="error" class="workspace-alert">
        <div>
          <p class="workspace-alert__title">Registry load failed</p>
          <p class="workspace-alert__copy">{{ error }}</p>
        </div>

        <button class="control-button control-button-secondary" type="button" @click="clearError()">
          Dismiss
        </button>
      </section>

      <section class="workspace-main-grid">
        <aside class="surface-panel inspector-panel">
          <div class="panel-heading">
            <div>
              <p class="eyebrow">Tenant registry</p>
              <h2 class="section-title">Lot selector</h2>
            </div>

            <div class="mini-plumbob"></div>
          </div>

          <label class="field-label" for="tenant-select">Selected tenant</label>
          <select id="tenant-select" v-model="selectedTenantId" class="field-select">
            <option v-for="tenant in tenants" :key="tenant.tenant_id" :value="tenant.tenant_id">
              {{ tenant.name || tenant.tenant_id }}
            </option>
          </select>

          <dl class="detail-list">
            <div>
              <dt>Selected site</dt>
              <dd>{{ selectedTenantName }}</dd>
            </div>
            <div>
              <dt>Tenant type</dt>
              <dd>{{ selectedTenantType }}</dd>
            </div>
            <div>
              <dt>Timezone</dt>
              <dd>{{ selectedTenant?.timezone || 'Unavailable' }}</dd>
            </div>
            <div>
              <dt>Latitude</dt>
              <dd>{{ selectedTenant?.latitude?.toFixed(2) || '-' }}</dd>
            </div>
            <div>
              <dt>Longitude</dt>
              <dd>{{ selectedTenant?.longitude?.toFixed(2) || '-' }}</dd>
            </div>
          </dl>

          <div class="note-block note-block-soft">
            <p class="note-block__title">Registry mix</p>
            <p class="note-block__copy">{{ tenantTypeMix }}</p>
          </div>
        </aside>

        <section class="surface-panel chart-panel chart-panel-main">
          <div class="chart-panel__header">
            <div>
              <p class="eyebrow">Analytical surface</p>
              <h2 class="section-title">Tenant registry constellation</h2>
            </div>

            <div class="chart-panel__legend">
              <span class="legend-pill legend-pill-blue">Registry node</span>
              <span class="legend-pill legend-pill-green">Focused lot</span>
            </div>
          </div>

          <ClientOnly>
            <TenantRegistryScatter
              v-if="tenants.length > 0"
              :tenants="tenants"
              :selected-tenant-id="selectedTenantId"
            />

            <template #fallback>
              <div class="chart-fallback">Preparing simulation map...</div>
            </template>
          </ClientOnly>

          <div v-if="!isLoading && tenants.length === 0" class="chart-fallback">
            No tenant data available yet.
          </div>
        </section>

        <aside class="surface-panel narrative-panel">
          <div class="panel-heading">
            <div>
              <p class="eyebrow">HUD notes</p>
              <h2 class="section-title">Sim cues</h2>
            </div>

            <div class="mini-plumbob mini-plumbob-green"></div>
          </div>

          <div class="narrative-stack">
            <div class="note-block note-block-blue">
              <p class="note-block__title">Primary boundary</p>
              <p class="note-block__copy">
                Browser requests stay same-origin and are forwarded by Nuxt to the control-plane API. That keeps the
                dashboard bright and simple while the data plumbing stays behind the glass.
              </p>
            </div>

            <div class="note-block note-block-green">
              <p class="note-block__title">What comes next</p>
              <ol class="note-list">
                <li>Materialization controls for weather and bronze assets.</li>
                <li>Market price history and baseline forecast overlays.</li>
                <li>Dispatch and regret signatures with game-like motion.</li>
              </ol>
            </div>

            <div class="note-block note-block-orange">
              <p class="note-block__title">Current data path</p>
              <p class="note-block__copy">Nuxt route to FastAPI `/tenants` to operator HUD.</p>
            </div>
          </div>
        </aside>
      </section>
    </div>
  </div>
</template>

<style scoped>
.workspace-shell {
  min-height: 100vh;
  padding: 1rem;
}

.workspace-grid {
  margin: 0 auto;
  display: grid;
  max-width: 96rem;
  gap: 1.1rem;
}

.hero-shell {
  display: grid;
  gap: 1rem;
}

.hero-copy-block {
  display: grid;
  gap: 1.35rem;
  overflow: hidden;
}

.hero-copy-block__topline,
.hero-copy-block__actions,
.panel-heading,
.chart-panel__header {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 0.85rem;
}

.hero-copy-block__body {
  display: grid;
  gap: 0.6rem;
}

.plumbob-chip,
.status-pill {
  display: inline-flex;
  align-items: center;
  gap: 0.7rem;
  border-radius: 999px;
  padding: 0.55rem 0.95rem;
  font-size: 0.76rem;
  font-weight: 800;
  letter-spacing: 0.22em;
  text-transform: uppercase;
}

.plumbob-chip {
  background: rgba(0, 121, 193, 0.1);
  color: var(--sims-blue-deep);
}

.status-pill {
  background: rgba(126, 211, 33, 0.14);
  color: var(--ink-strong);
}

.plumbob-chip__shape,
.mini-plumbob,
.hero-stage__plumbob {
  clip-path: polygon(50% 0%, 100% 50%, 50% 100%, 0% 50%);
}

.plumbob-chip__shape {
  width: 0.9rem;
  height: 1.25rem;
  background: linear-gradient(180deg, #a7f35b 0%, var(--plumbob-green) 100%);
  box-shadow: 0 0 18px rgba(126, 211, 33, 0.35);
}

.status-pill__dot {
  width: 0.85rem;
  height: 0.85rem;
  border-radius: 999px;
  background: var(--plumbob-green);
  box-shadow: 0 0 0 0 rgba(126, 211, 33, 0.42);
  animation: sims-pulse 2s infinite;
}

.workspace-title {
  max-width: 40rem;
  font-size: clamp(2.5rem, 5.8vw, 5.5rem);
  line-height: 0.88;
  letter-spacing: -0.05em;
  color: var(--sims-blue-deep);
}

.workspace-copy {
  max-width: 36rem;
  font-size: 1.08rem;
  line-height: 1.65;
  color: var(--ink-soft);
}

.hero-kicker {
  display: grid;
  gap: 0.2rem;
}

.hero-kicker__label,
.metric-strip__label,
.field-label,
.detail-list dt,
.note-block__title,
.eyebrow,
.chart-panel__meta,
.legend-pill {
  font-size: 0.74rem;
  font-weight: 800;
  letter-spacing: 0.2em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.hero-kicker__value {
  font-size: 1rem;
  font-weight: 700;
  color: var(--ink-strong);
}

.hero-stage {
  position: relative;
  overflow: hidden;
  min-height: 20rem;
  background:
    radial-gradient(circle at top, rgba(255, 255, 255, 0.4), transparent 38%),
    linear-gradient(160deg, var(--sims-blue) 0%, #35a6ff 55%, #7ed4ff 100%);
  color: white;
}

.hero-stage__halo {
  position: absolute;
  top: -4rem;
  right: -1rem;
  width: 14rem;
  height: 14rem;
  border-radius: 999px;
  background: radial-gradient(circle, rgba(255, 255, 255, 0.38) 0%, rgba(255, 255, 255, 0) 70%);
}

.hero-stage__plumbob {
  position: absolute;
  top: 1.75rem;
  right: 2rem;
  width: 4.75rem;
  height: 6.8rem;
  background: linear-gradient(180deg, #d7ff9c 0%, var(--plumbob-green) 68%, #54b30d 100%);
  box-shadow: 0 18px 35px rgba(0, 0, 0, 0.18);
  animation: plumbob-float 3.2s ease-in-out infinite;
}

.hero-stage__content {
  position: relative;
  display: grid;
  gap: 0.85rem;
  z-index: 1;
}

.hero-stage__title {
  max-width: 16rem;
  font-size: clamp(1.9rem, 3vw, 3rem);
  line-height: 0.94;
}

.hero-stage__subtitle {
  font-size: 1rem;
  font-weight: 700;
  text-transform: capitalize;
  color: rgba(255, 255, 255, 0.82);
}

.hero-stage__facts {
  display: grid;
  gap: 0.85rem;
  margin-top: 1rem;
}

.hero-stage__facts div {
  display: grid;
  gap: 0.2rem;
}

.hero-stage__facts span {
  font-size: 0.75rem;
  font-weight: 800;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: rgba(255, 255, 255, 0.7);
}

.hero-stage__facts strong {
  font-size: 1rem;
  font-weight: 700;
}

.workspace-metrics {
  display: grid;
  gap: 1rem;
}

.metric-strip {
  display: grid;
  gap: 0.45rem;
}

.metric-strip-blue {
  border-color: rgba(0, 121, 193, 0.16);
}

.metric-strip-green {
  border-color: rgba(126, 211, 33, 0.18);
}

.metric-strip-orange {
  border-color: rgba(245, 166, 35, 0.18);
}

.metric-strip__value {
  font-size: 2.4rem;
  line-height: 1;
  letter-spacing: -0.05em;
  color: var(--sims-blue-deep);
}

.metric-strip__value--small {
  font-size: 1.55rem;
}

.metric-strip__meta {
  font-size: 0.95rem;
  color: var(--ink-soft);
}

.workspace-alert {
  display: flex;
  flex-direction: column;
  gap: 1rem;
  justify-content: space-between;
  border: 1px solid rgba(153, 27, 27, 0.18);
  border-radius: 1.5rem;
  background: rgba(254, 242, 242, 0.82);
  padding: 1rem 1.25rem;
  color: #991b1b;
}

.workspace-alert__title {
  font-size: 0.75rem;
  font-weight: 700;
  letter-spacing: 0.18em;
  text-transform: uppercase;
}

.workspace-alert__copy {
  margin-top: 0.45rem;
  line-height: 1.6;
}

.workspace-main-grid {
  display: grid;
  gap: 1rem;
}

.section-title {
  margin-top: 0.4rem;
  font-size: 1.8rem;
  line-height: 1.1;
  color: var(--sims-blue-deep);
}

.inspector-panel,
.chart-panel,
.narrative-panel {
  display: grid;
  gap: 1.25rem;
}

.eyebrow-light {
  color: rgba(255, 255, 255, 0.72);
}

.field-select {
  width: 100%;
  border: 3px solid rgba(255, 255, 255, 0.95);
  border-radius: 999px;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(241, 249, 255, 0.94));
  padding: 1rem 1.1rem;
  font-size: 0.98rem;
  color: var(--ink-strong);
  box-shadow: 0 10px 30px rgba(0, 121, 193, 0.08);
  transition: border-color 0.2s ease, transform 0.2s ease, box-shadow 0.2s ease;
}

.field-select:focus {
  outline: none;
  border-color: rgba(0, 121, 193, 0.38);
  box-shadow: 0 0 0 5px rgba(0, 121, 193, 0.14);
  transform: translateY(-1px);
}

.detail-list {
  display: grid;
  gap: 0.9rem;
}

.detail-list dd {
  margin-top: 0.2rem;
  font-size: 1rem;
  color: var(--ink-strong);
  font-weight: 700;
}

.note-block {
  border: 1px solid var(--line-soft);
  border-radius: 1.35rem;
  background: rgba(255, 255, 255, 0.68);
  padding: 1rem;
}

.note-block__copy,
.note-list {
  margin-top: 0.7rem;
  line-height: 1.7;
  color: var(--ink-soft);
}

.note-block-soft {
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.92), rgba(241, 249, 255, 0.76));
}

.note-block-blue {
  background: linear-gradient(180deg, rgba(0, 121, 193, 0.09), rgba(255, 255, 255, 0.8));
}

.note-block-green {
  background: linear-gradient(180deg, rgba(126, 211, 33, 0.11), rgba(255, 255, 255, 0.82));
}

.note-block-orange {
  background: linear-gradient(180deg, rgba(245, 166, 35, 0.13), rgba(255, 255, 255, 0.8));
}

.chart-panel-main {
  overflow: hidden;
}

.chart-panel__legend {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
}

.legend-pill {
  border-radius: 999px;
  padding: 0.45rem 0.8rem;
}

.legend-pill-blue {
  background: rgba(0, 121, 193, 0.08);
  color: var(--sims-blue-deep);
}

.legend-pill-green {
  background: rgba(126, 211, 33, 0.12);
  color: #4d8714;
}

.chart-fallback {
  display: flex;
  min-height: 25rem;
  align-items: center;
  justify-content: center;
  border: 2px dashed rgba(0, 121, 193, 0.18);
  border-radius: 1.5rem;
  color: var(--ink-soft);
}

.narrative-stack {
  display: grid;
  gap: 1rem;
}

.note-list {
  padding-left: 1rem;
}

.mini-plumbob {
  width: 1rem;
  height: 1.4rem;
  background: linear-gradient(180deg, #65c6ff 0%, var(--sims-blue) 100%);
  box-shadow: 0 8px 18px rgba(0, 121, 193, 0.22);
}

.mini-plumbob-green {
  background: linear-gradient(180deg, #b6ff6d 0%, var(--plumbob-green) 100%);
}

.control-button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border: 1px solid transparent;
  border-radius: 999px;
  padding: 0.92rem 1.4rem;
  font-size: 0.74rem;
  font-weight: 800;
  letter-spacing: 0.22em;
  text-transform: uppercase;
  color: white;
  transition: transform 0.22s cubic-bezier(0.175, 0.885, 0.32, 1.275), box-shadow 0.22s ease, background-color 0.22s ease, opacity 0.2s ease;
}

.control-button-primary {
  border: 3px solid rgba(255, 255, 255, 0.95);
  background: var(--sims-blue);
  box-shadow: 0 12px 26px rgba(0, 121, 193, 0.24);
}

.control-button:hover:not(:disabled) {
  transform: scale(1.05);
}

.control-button:disabled {
  cursor: wait;
  opacity: 0.68;
}

.control-button-primary:hover:not(:disabled) {
  background: var(--sims-blue-hover);
  box-shadow: 0 16px 30px rgba(0, 121, 193, 0.28);
}

.control-button-secondary {
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.92), rgba(252, 240, 240, 0.92));
  border: 2px solid rgba(208, 2, 27, 0.2);
  color: var(--urgent-red);
  box-shadow: 0 10px 22px rgba(208, 2, 27, 0.1);
}

@media (min-width: 900px) {
  .hero-shell {
    grid-template-columns: minmax(0, 1.6fr) minmax(18rem, 0.78fr);
  }

  .workspace-metrics {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .workspace-main-grid {
    grid-template-columns: minmax(17rem, 20rem) minmax(0, 1fr) minmax(18rem, 22rem);
    align-items: start;
  }

  .workspace-alert {
    flex-direction: row;
    align-items: center;
  }
}

@keyframes sims-pulse {
  0% {
    box-shadow: 0 0 0 0 rgba(126, 211, 33, 0.38);
  }

  70% {
    box-shadow: 0 0 0 12px rgba(126, 211, 33, 0);
  }

  100% {
    box-shadow: 0 0 0 0 rgba(126, 211, 33, 0);
  }
}

@keyframes plumbob-float {
  0%,
  100% {
    transform: translateY(0) rotate(0deg);
  }

  50% {
    transform: translateY(-10px) rotate(2deg);
  }
}
</style>
