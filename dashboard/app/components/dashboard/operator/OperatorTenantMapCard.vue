<script setup lang="ts">
import {
  type OperatorTenantMapCardProps,
  useOperatorTenantMapCardModel
} from '~/composables/useOperatorTenantMapCardModel'

const props = defineProps<OperatorTenantMapCardProps>()

const emit = defineEmits<{
  'update:selectedTenantId': [value: string]
}>()

const {
  compactBadgeUi,
  currentMarketPriceLabel,
  currentWeatherEmoji,
  currentWeatherLabel,
  markerLabel,
  selectedTenant,
  tenantCoordinates,
  tenantMarkers,
  weatherSourceLabel,
  weatherUpliftLabel
} = useOperatorTenantMapCardModel(props)

const onSelectTenant = (tenantId: string): void => {
  emit('update:selectedTenantId', tenantId)
}
</script>

<template>
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
        :src="'/design/ukraine-outline.svg'"
        alt="Outline of Ukraine"
      >
      <div class="tenant-card__tenant-markers">
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
</template>

<style scoped src="../../../assets/css/operator-tenant-map.css"></style>
