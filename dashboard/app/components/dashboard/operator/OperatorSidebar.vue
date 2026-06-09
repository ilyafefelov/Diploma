<script setup lang="ts">
import { computed } from 'vue'

import type { BaselineLpPreview, SignalPreview, TenantSummary } from '~/types/control-plane'
import type { OperatorNavItem } from '~/types/operator-dashboard'
import OperatorTenantMapCard from './OperatorTenantMapCard.vue'

const props = defineProps<{
  tenants: TenantSummary[]
  selectedTenantId: string
  navItems: OperatorNavItem[]
  activeRegistrySummary: string
  batteryAssetLabel: string
  signalPreview?: SignalPreview | null
  baselinePreview?: BaselineLpPreview | null
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
</script>

<template>
  <aside
    class="operator-sidebar"
    aria-label="Tenant selection and operator sections"
  >
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
        <UFormField
          label="Tenant / site"
          name="tenant-select"
          class="tenant-card__field"
          :ui="{ label: 'tenant-card__label', container: 'mt-1' }"
        >
          <USelect
            id="tenant-select"
            aria-label="Select operator tenant"
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
        </UFormField>
        <div class="tenant-card__meta">
          <span>{{ tenantMeta }}</span>
          <UTooltip
            text="Battery energy capacity and max charge/discharge power from the existing preview read model."
            :delay-duration="0"
          >
            <span
              class="tenant-card__meta-item"
              role="group"
              :aria-label="`Battery asset context: ${batteryAssetLabel}`"
              tabindex="0"
            >
              {{ batteryAssetLabel }}
            </span>
          </UTooltip>
          <UTooltip
            text="Coordinate pair in decimal degrees from the selected tenant record."
            :delay-duration="0"
          >
            <span
              class="tenant-card__meta-item"
              role="group"
              :aria-label="`Tenant coordinates: ${tenantCoordinates}`"
              tabindex="0"
            >
              {{ tenantCoordinates }}
            </span>
          </UTooltip>
        </div>
      </div>
      <OperatorTenantMapCard
        :tenants="tenants"
        :selected-tenant-id="selectedTenantId"
        :signal-preview="signalPreview"
        :baseline-preview="baselinePreview"
        @update:selected-tenant-id="emit('update:selectedTenantId', $event)"
      />
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
        :to="`#${item.targetId}`"
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
          <span>Values are derived from tenant registry heartbeat and active flow error counters.</span>
        </span>
      </div>
      <div class="sidebar-status-card__mini-grid">
        <article
          class="tenant-count-card"
          role="group"
          :aria-label="`Total mapped lots: ${tenantCount}`"
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
            <span>{{ tenantCount }} tenants are loaded in the selected registry snapshot.</span>
          </span>
        </article>
        <article
          class="tenant-count-card"
          role="group"
          :aria-label="`Critical tenants: ${criticalTenantCount}`"
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
      to="/defense"
      icon="i-lucide-presentation"
      label="Defense evidence"
      color="info"
      variant="ghost"
      block
    />
  </aside>
</template>

<style scoped>
.tenant-card {
  border-color: var(--operator-tenant-map-card-border);
  background:
    radial-gradient(circle at top right, var(--operator-tenant-map-card-highlight), transparent 34%),
    linear-gradient(180deg, var(--operator-tenant-map-surface-top), var(--operator-tenant-map-surface-bottom));
  box-shadow:
    inset 0 1px 0 var(--operator-tenant-map-card-highlight),
    inset 0 -1px 0 var(--operator-tenant-map-surface-glow),
    0 16px 34px var(--operator-tenant-map-card-shadow);
  backdrop-filter: blur(18px) saturate(1.25);
}
</style>
