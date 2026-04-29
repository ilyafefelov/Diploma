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
      <header class="workspace-banner surface-panel surface-panel-strong">
        <div>
          <p class="eyebrow">Smart arbitrage / dashboard w1</p>
          <h1 class="workspace-title">Operator workspace for the tenant-aware DAM baseline.</h1>
          <p class="workspace-copy">
            Fresh runtime, tracked in the root repository, with a dedicated same-origin control-plane boundary and the
            first ECharts analytical surface.
          </p>
        </div>

        <div class="banner-actions">
          <div class="status-pulse">
            <span class="status-pulse__dot"></span>
            {{ isLoading ? 'Refreshing registry' : 'Control plane online' }}
          </div>

          <button class="control-button" type="button" :disabled="isLoading" @click="refreshRegistry">
            {{ isLoading ? 'Refreshing' : 'Refresh registry' }}
          </button>
        </div>
      </header>

      <section class="workspace-metrics">
        <article class="metric-strip surface-panel">
          <p class="metric-strip__label">Tenants</p>
          <p class="metric-strip__value">{{ tenants.length }}</p>
          <p class="metric-strip__meta">Last sync {{ lastSyncLabel }}</p>
        </article>

        <article class="metric-strip surface-panel">
          <p class="metric-strip__label">Critical sites</p>
          <p class="metric-strip__value">{{ criticalTenantCount }}</p>
          <p class="metric-strip__meta">Policy-sensitive load under watch</p>
        </article>

        <article class="metric-strip surface-panel">
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
          <div>
            <p class="eyebrow">Tenant registry</p>
            <h2 class="section-title">Control surface</h2>
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

          <div class="notebook-block">
            <p class="eyebrow">Registry mix</p>
            <p class="notebook-block__body">{{ tenantTypeMix }}</p>
          </div>
        </aside>

        <section class="surface-panel chart-panel">
          <div class="chart-panel__header">
            <div>
              <p class="eyebrow">Analytical surface</p>
              <h2 class="section-title">Tenant registry constellation</h2>
            </div>

            <p class="chart-panel__meta">Longitude / latitude map for the current registry</p>
          </div>

          <ClientOnly>
            <TenantRegistryScatter
              v-if="tenants.length > 0"
              :tenants="tenants"
              :selected-tenant-id="selectedTenantId"
            />

            <template #fallback>
              <div class="chart-fallback">Preparing chart runtime...</div>
            </template>
          </ClientOnly>

          <div v-if="!isLoading && tenants.length === 0" class="chart-fallback">
            No tenant data available yet.
          </div>
        </section>

        <aside class="surface-panel narrative-panel">
          <div>
            <p class="eyebrow">Why this slice</p>
            <h2 class="section-title">Operational framing</h2>
          </div>

          <div class="narrative-stack">
            <div class="note-block">
              <p class="note-block__title">Primary boundary</p>
              <p class="note-block__copy">
                Browser requests stay same-origin and are forwarded by Nuxt to the control-plane API. Future dashboard
                slices can add richer read models behind the same server boundary.
              </p>
            </div>

            <div class="note-block">
              <p class="note-block__title">What comes next</p>
              <ol class="note-list">
                <li>Materialization controls for weather and bronze assets.</li>
                <li>Market price history and baseline forecast overlays.</li>
                <li>Dispatch and regret signatures on the same visual system.</li>
              </ol>
            </div>

            <div class="note-block note-block-accent">
              <p class="note-block__title">Current data path</p>
              <p class="note-block__copy">Nuxt server route -> FastAPI /tenants -> tenant-aware operator shell.</p>
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
  padding: 1.25rem;
}

.workspace-grid {
  margin: 0 auto;
  display: grid;
  max-width: 92rem;
  gap: 1rem;
}

.workspace-banner {
  display: grid;
  gap: 1rem;
  align-items: start;
}

.workspace-title {
  max-width: 52rem;
  margin-top: 0.75rem;
  font-size: clamp(2rem, 4vw, 4.25rem);
  line-height: 0.95;
  letter-spacing: -0.05em;
  color: var(--ink-strong);
}

.workspace-copy {
  max-width: 42rem;
  margin-top: 1rem;
  font-size: 1rem;
  line-height: 1.75;
  color: var(--ink-soft);
}

.banner-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem;
  align-items: center;
  justify-content: space-between;
}

.status-pulse {
  display: inline-flex;
  align-items: center;
  gap: 0.65rem;
  font-size: 0.74rem;
  font-weight: 700;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.status-pulse__dot {
  width: 0.7rem;
  height: 0.7rem;
  border-radius: 999px;
  background: var(--accent-cyan);
  box-shadow: 0 0 0 0 rgba(28, 104, 114, 0.4);
  animation: pulse 1.8s infinite;
}

.workspace-metrics {
  display: grid;
  gap: 1rem;
}

.metric-strip {
  display: grid;
  gap: 0.5rem;
}

.metric-strip__label {
  font-size: 0.74rem;
  font-weight: 700;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.metric-strip__value {
  font-size: 2.4rem;
  line-height: 1;
  letter-spacing: -0.05em;
  color: var(--ink-strong);
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
  margin-top: 0.65rem;
  font-size: 1.65rem;
  line-height: 1.1;
  color: var(--ink-strong);
}

.inspector-panel,
.chart-panel,
.narrative-panel {
  display: grid;
  gap: 1.25rem;
}

.field-label,
.detail-list dt,
.note-block__title,
.chart-panel__meta,
.eyebrow {
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.field-select {
  width: 100%;
  border: 1px solid var(--line-soft);
  border-radius: 1.1rem;
  background: rgba(255, 253, 250, 0.86);
  padding: 0.95rem 1rem;
  font-size: 0.98rem;
  color: var(--ink-strong);
  transition: border-color 0.2s ease, transform 0.2s ease, box-shadow 0.2s ease;
}

.field-select:focus {
  outline: none;
  border-color: rgba(28, 104, 114, 0.35);
  box-shadow: 0 0 0 4px rgba(28, 104, 114, 0.12);
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
}

.notebook-block,
.note-block {
  border: 1px solid var(--line-soft);
  border-radius: 1.35rem;
  background: rgba(255, 253, 250, 0.68);
  padding: 1rem;
}

.notebook-block__body,
.note-block__copy,
.note-list {
  margin-top: 0.7rem;
  line-height: 1.7;
  color: var(--ink-soft);
}

.chart-panel__header {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  justify-content: space-between;
}

.chart-fallback {
  display: flex;
  min-height: 25rem;
  align-items: center;
  justify-content: center;
  border: 1px dashed var(--line-soft);
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

.note-block-accent {
  background: linear-gradient(180deg, rgba(28, 104, 114, 0.12), rgba(214, 126, 44, 0.08));
}

.control-button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border: 1px solid transparent;
  border-radius: 999px;
  background: var(--accent-cyan);
  padding: 0.85rem 1.2rem;
  font-size: 0.74rem;
  font-weight: 700;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: white;
  transition: transform 0.2s ease, background-color 0.2s ease, opacity 0.2s ease;
}

.control-button:hover:not(:disabled) {
  background: var(--accent-cyan-strong);
  transform: translateY(-1px);
}

.control-button:disabled {
  cursor: wait;
  opacity: 0.68;
}

.control-button-secondary {
  background: rgba(127, 29, 29, 0.08);
  color: #991b1b;
}

@media (min-width: 900px) {
  .workspace-banner {
    grid-template-columns: minmax(0, 1.55fr) minmax(18rem, 0.85fr);
  }

  .banner-actions {
    align-self: stretch;
    flex-direction: column;
    align-items: flex-end;
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

  .chart-panel__header {
    flex-direction: row;
    align-items: end;
  }
}

@keyframes pulse {
  0% {
    box-shadow: 0 0 0 0 rgba(28, 104, 114, 0.36);
  }

  70% {
    box-shadow: 0 0 0 12px rgba(28, 104, 114, 0);
  }

  100% {
    box-shadow: 0 0 0 0 rgba(28, 104, 114, 0);
  }
}
</style>
