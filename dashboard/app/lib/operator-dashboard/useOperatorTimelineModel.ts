import { computed, type Ref } from 'vue'

import type {
  BaselineLpPreview,
  BidRecommendationPreviewPoint,
  OperatorRecommendationResponse,
  SignalPreview
} from '~/types/control-plane'
import type {
  OperatorGatekeeperAction,
  OperatorGatekeeperActionLabel,
  OperatorMarketVenue,
  OperatorTimelineSegment
} from '~/types/operator-dashboard'
import {
  DAM_REVIEW_ACTION_THRESHOLD_MW,
  formatDamDeliveryLabel,
  formatSignedMw,
  powerToTimelineLabel,
  timelineTooltipBody
} from '../../utils/operatorTimeline'

interface OperatorTimelineModelInput {
  signalPreview: Readonly<Ref<SignalPreview | null>>
  baselinePreview: Readonly<Ref<BaselineLpPreview | null>>
  operatorRecommendation?: Readonly<Ref<OperatorRecommendationResponse | null>>
  selectedMarketVenue?: Readonly<Ref<OperatorMarketVenue>>
}

const TIMELINE_SEGMENT_LIMIT = 5

export const useOperatorTimelineModel = (input: OperatorTimelineModelInput) => {
  const activeRecommendationSchedule = computed(() => {
    if (input.operatorRecommendation?.value) {
      return input.operatorRecommendation.value.recommendation_schedule ?? []
    }

    return input.baselinePreview.value?.recommendation_schedule ?? []
  })

  const activeBidRecommendationPreview = computed(() => {
    if (input.operatorRecommendation?.value) {
      return input.operatorRecommendation.value.bid_recommendation_preview ?? []
    }

    return input.baselinePreview.value?.bid_recommendation_preview ?? []
  })

  const activeIntervalMinutes = computed(() => input.operatorRecommendation?.value?.interval_minutes
    ?? input.baselinePreview.value?.interval_minutes
    ?? 60)
  const activeMarketVenue = computed(() => input.operatorRecommendation?.value?.market_venue
    ?? input.selectedMarketVenue?.value
    ?? input.baselinePreview.value?.market_venue
    ?? 'DAM')
  const hasSelectedOperatorRecommendationSchedule = computed(() => {
    return Boolean(input.operatorRecommendation?.value?.recommendation_schedule?.length)
  })

  const batteryCapacityMwh = computed(() => input.baselinePreview.value?.battery_metrics.capacity_mwh ?? null)

  const deliveryWindowLabel = computed(() => {
    const windowStart = input.operatorRecommendation?.value?.target_delivery_window_start
      ?? input.baselinePreview.value?.target_delivery_window_start
      ?? null
    const windowEnd = input.operatorRecommendation?.value?.target_delivery_window_end
      ?? input.baselinePreview.value?.target_delivery_window_end
      ?? null

    if (!windowStart || !windowEnd) {
      return 'Delivery window: pending'
    }

    return `Delivery window: ${formatDamDeliveryLabel(windowStart)} -> ${formatDamDeliveryLabel(windowEnd)}`
  })

  const activeEconomics = computed(() => input.operatorRecommendation?.value?.economics
    ?? input.baselinePreview.value?.economics
    ?? null)

  const selectedTimelineSchedulePoints = computed(() => selectTimelineSchedulePoints(activeRecommendationSchedule.value))
  const bidPreviewByInterval = computed(() => new Map(
    activeBidRecommendationPreview.value.map(point => [point.interval_start, point])
  ))

  const latestRecommendedPowerMw = computed(() => {
    const selectedPoint = selectedTimelineSchedulePoints.value[0]
    if (selectedPoint) {
      return selectedPoint.recommended_net_power_mw
    }

    return 0
  })

  const preferredGatekeeperAction = computed<OperatorGatekeeperActionLabel>(() => {
    const previewAction = powerToTimelineLabel(latestRecommendedPowerMw.value)

    if (previewAction === 'Discharge') {
      return 'SELL'
    }

    if (previewAction === 'Charge') {
      return 'BUY'
    }

    return 'HOLD'
  })

  const gatekeeperActions = computed<OperatorGatekeeperAction[]>(() => {
    if (!hasSelectedOperatorRecommendationSchedule.value) {
      return []
    }

    return [
      {
        label: 'BUY',
        score: preferredGatekeeperAction.value === 'BUY' ? 87 : 32,
        icon: 'i-lucide-download',
        active: preferredGatekeeperAction.value === 'BUY',
        tooltipTitle: 'Charge preview score',
        tooltipBody: 'Higher BUY means the selected market delivery hour is a charging preview, reserving energy for a later price window.',
        tooltipFormula: 'score = 50 + charge_bias * 35 - guardrail_penalty; charge_bias comes from negative recommended_net_power_mw'
      },
      {
        label: 'SELL',
        score: preferredGatekeeperAction.value === 'SELL' ? 87 : 38,
        icon: 'i-lucide-upload',
        active: preferredGatekeeperAction.value === 'SELL',
        tooltipTitle: 'Discharge preview score',
        tooltipBody: 'Higher SELL means the selected market delivery hour is a discharge preview; future bid validation still checks SOC and power limits.',
        tooltipFormula: 'score = 50 + discharge_bias * 35 - guardrail_penalty; discharge_bias comes from positive recommended_net_power_mw'
      },
      {
        label: 'HOLD',
        score: preferredGatekeeperAction.value === 'HOLD' ? 82 : 41,
        icon: 'i-lucide-pause',
        active: preferredGatekeeperAction.value === 'HOLD',
        tooltipTitle: 'Hold preview score',
        tooltipBody: 'Higher HOLD means the selected market delivery-hour spread is weak or the safer review choice is to wait for a cleaner interval.',
        tooltipFormula: 'score = 50 + idle_bias * 32 + uncertainty_penalty; idle_bias rises when recommended_net_power_mw is near zero'
      }
    ]
  })

  const timelineSegments = computed<OperatorTimelineSegment[]>(() => {
    const schedule = selectedTimelineSchedulePoints.value

    if (schedule.length === 0) {
      return [
        {
          time: 'Delivery window',
          label: 'Preview pending',
          value: 'No schedule loaded',
          marketSideLabel: 'PENDING',
          indicativePriceLabel: 'price pending',
          marketBoundaryLabel: 'No market payload',
          tone: 'blue',
          tooltipTitle: 'Delivery schedule pending',
          tooltipBody: 'No DAM/IDM hourly schedule has loaded yet, so this dock is not showing a bid, ProposedBid, or market instruction.'
        }
      ]
    }

    return schedule.map((point) => {
      const label = powerToTimelineLabel(point.recommended_net_power_mw)
      const bidPreview = bidPreviewByInterval.value.get(point.interval_start)
      const marketSideLabel = bidPreview?.side ?? marketSideFromTimelineLabel(label)
      const indicativePriceLabel = bidPreview
        ? `${Math.round(bidPreview.indicative_limit_price_uah_mwh).toLocaleString('en-GB')} UAH/MWh`
        : 'price pending'
      const marketBoundaryLabel = bidPreview?.market_order_payload_emitted === false
        ? 'No market payload'
        : 'Preview only'
      const quantityLabel = formatPowerEnergyCapacityLabel(
        point.recommended_net_power_mw,
        activeIntervalMinutes.value,
        batteryCapacityMwh.value
      )

      return {
        time: formatDamDeliveryLabel(point.interval_start),
        label,
        value: quantityLabel,
        marketSideLabel,
        indicativePriceLabel,
        marketBoundaryLabel,
        tone: label === 'Discharge'
          ? 'green'
          : label === 'Charge'
            ? 'orange'
            : 'blue',
        tooltipTitle: `${label} for ${formatDamDeliveryLabel(point.interval_start)}`,
        tooltipBody: formatTimelineTooltipBody(
          label,
          point.recommended_net_power_mw,
          quantityLabel,
          bidPreview,
          marketSideLabel,
          indicativePriceLabel,
          marketBoundaryLabel,
          activeMarketVenue.value
        )
      }
    })
  })

  const batteryStatusLabel = computed(() => {
    const action = powerToTimelineLabel(latestRecommendedPowerMw.value)

    if (action === 'Discharge') {
      return `${activeMarketVenue.value} discharge preview`
    }

    if (action === 'Charge') {
      return `${activeMarketVenue.value} charge preview`
    }

    return `${activeMarketVenue.value} hold preview`
  })

  const latestRecommendedPowerLabel = computed(() => formatSignedMw(latestRecommendedPowerMw.value))

  return {
    activeEconomics,
    batteryStatusLabel,
    deliveryWindowLabel,
    gatekeeperActions,
    latestRecommendedPowerLabel,
    latestRecommendedPowerMw,
    timelineSegments
  }
}

const formatPowerEnergyCapacityLabel = (
  powerMw: number,
  intervalMinutes: number,
  capacityMwh: number | null
): string => {
  const intervalHours = intervalMinutes / 60
  const energyMwh = Math.abs(powerMw) * intervalHours
  const baseLabel = `${powerMw > 0 ? '+' : ''}${powerMw.toFixed(2)} MW / ${energyMwh.toFixed(2)} MWh`

  if (!capacityMwh || capacityMwh <= 0) {
    return baseLabel
  }

  return `${baseLabel} (${Math.round(energyMwh / capacityMwh * 100)}% cap)`
}

const marketSideFromTimelineLabel = (
  label: OperatorTimelineSegment['label']
): OperatorTimelineSegment['marketSideLabel'] => {
  if (label === 'Discharge') {
    return 'SELL'
  }

  if (label === 'Charge') {
    return 'BUY'
  }

  if (label === 'Hold') {
    return 'HOLD'
  }

  return 'PENDING'
}

const formatTimelineTooltipBody = (
  label: OperatorTimelineSegment['label'],
  powerMw: number,
  quantityLabel: string,
  bidPreview: BidRecommendationPreviewPoint | undefined,
  marketSideLabel: OperatorTimelineSegment['marketSideLabel'],
  indicativePriceLabel: string,
  marketBoundaryLabel: string,
  marketVenue: string
): string => {
  const scheduleBody = `${timelineTooltipBody(label, powerMw)} Hourly quantity: ${quantityLabel}.`
  if (!bidPreview || marketSideLabel === 'PENDING') {
    return scheduleBody
  }

  return `${scheduleBody} It is a non-submittable ${marketVenue} ${marketSideLabel} preview at ${indicativePriceLabel}; ${marketBoundaryLabel.toLowerCase()} and no ProposedBid.`
}

const selectTimelineSchedulePoints = <T extends { recommended_net_power_mw: number }>(schedule: T[]): T[] => {
  const actionPoints = schedule.filter(point => Math.abs(point.recommended_net_power_mw) >= DAM_REVIEW_ACTION_THRESHOLD_MW)
  if (actionPoints.length > 0) {
    return actionPoints.slice(0, TIMELINE_SEGMENT_LIMIT)
  }

  return schedule.slice(0, TIMELINE_SEGMENT_LIMIT)
}
