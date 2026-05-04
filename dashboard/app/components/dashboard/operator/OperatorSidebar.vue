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
          <span>{{ tenantCoordinates }}</span>
        </div>
        <div
          class="tenant-card__ukraine-map"
          aria-hidden="true"
        >
          <svg
            viewBox="0 0 128 82"
            role="img"
          >
            <path
              class="tenant-card__ukraine-body"
              d="M8 39l8-10 16 1 7-7 15 4 10-9 11 7 15-3 10 8 16 1 4 9-9 6 4 9-13 7-10-3-10 9-14-2-11 7-13-5-14 3-8-10-15-4z"
            />
            <path
              class="tenant-card__ukraine-river"
              d="M70 18c-3 12 8 18 5 30-2 8-12 11-10 23"
            />
            <circle
              class="tenant-card__ukraine-pin"
              cx="84"
              cy="52"
              r="4"
            />
          </svg>
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
