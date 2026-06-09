import { computed } from 'vue'

import type { BaselineLpPreview, SignalPreview, TenantSummary } from '~/types/control-plane'

export type OperatorTenantMapCardProps = {
  tenants: TenantSummary[]
  selectedTenantId: string
  signalPreview?: SignalPreview | null
  baselinePreview?: BaselineLpPreview | null
}

type TenantMapMarker = TenantSummary & {
  left: number
  top: number
  isSelected: boolean
}

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

const compactBadgeUi = {
  base: 'min-w-0 max-w-full',
  label: 'truncate'
}

const clamp = (value: number, min: number, max: number): number => Math.min(max, Math.max(min, value))
const degToRad = (value: number): number => value * Math.PI / 180
const mercatorY = (lat: number): number => Math.log(Math.tan(Math.PI / 4 + degToRad(lat) / 2))
const formatWholeNumber = (value: number): string => Math.round(value).toLocaleString('en-US')

export const markerLabel = (tenant: TenantSummary): string => tenant.name || tenant.tenant_id

export const useOperatorTenantMapCardModel = (props: OperatorTenantMapCardProps) => {
  const selectedTenant = computed(() => {
    return props.tenants.find(tenant => tenant.tenant_id === props.selectedTenantId) || null
  })

  const tenantCoordinates = computed(() => {
    if (!selectedTenant.value) {
      return 'Location pending'
    }

    return `${selectedTenant.value.latitude.toFixed(2)} / ${selectedTenant.value.longitude.toFixed(2)}`
  })

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

  return {
    compactBadgeUi,
    currentMarketPrice,
    currentMarketPriceLabel,
    currentWeatherEmoji,
    currentWeatherLabel,
    markerLabel,
    selectedTenant,
    tenantCoordinates,
    tenantMarkers,
    weatherSourceLabel,
    weatherUpliftLabel,
    weatherUpliftValue
  }
}
