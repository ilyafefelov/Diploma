<script setup lang="ts">
import type { TenantSummary } from '~/types/control-plane'

defineProps<{
  activeTenantId: string
  isLoading: boolean
  tenants: TenantSummary[]
}>()

const emit = defineEmits<{
  'refresh': []
  'update:activeTenantId': [value: string]
}>()
</script>

<template>
  <header class="defense-topbar">
    <NuxtLink
      class="brand-link"
      to="/operator"
    >
      <UIcon name="i-lucide-arrow-left" />
      Operator
    </NuxtLink>
    <div class="topbar-controls">
      <label class="tenant-picker">
        <span>Tenant</span>
        <select
          :value="activeTenantId"
          @change="emit('update:activeTenantId', ($event.target as HTMLSelectElement).value)"
        >
          <option
            v-for="tenant in tenants"
            :key="tenant.tenant_id"
            :value="tenant.tenant_id"
          >
            {{ tenant.name || tenant.tenant_id }}
          </option>
        </select>
      </label>
      <button
        class="icon-button"
        type="button"
        :disabled="isLoading"
        @click="emit('refresh')"
      >
        <UIcon name="i-lucide-refresh-cw" />
        Refresh
      </button>
    </div>
  </header>
</template>
