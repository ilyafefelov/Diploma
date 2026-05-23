import type { OperatorTimelineSegment } from '~/types/operator-dashboard'

export const DAM_REVIEW_ACTION_THRESHOLD_MW = 0.05
const MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

export const formatSignedMw = (value: number): string => `${value > 0 ? '+' : ''}${value.toFixed(1)} MW`

export const powerToTimelineLabel = (powerMw: number): OperatorTimelineSegment['label'] => {
  if (powerMw > DAM_REVIEW_ACTION_THRESHOLD_MW) {
    return 'Discharge'
  }

  if (powerMw < -DAM_REVIEW_ACTION_THRESHOLD_MW) {
    return 'Charge'
  }

  return 'Hold'
}

export const formatDamDeliveryLabel = (intervalStart: string): string => {
  const match = intervalStart.match(/^(?<year>\d{4})-(?<month>\d{2})-(?<day>\d{2})T(?<hour>\d{2}):(?<minute>\d{2})/)
  if (!match?.groups) {
    return `DAM ${intervalStart}`
  }

  const { month, day, hour, minute } = match.groups
  if (!month || !day || !hour || !minute) {
    return `DAM ${intervalStart}`
  }

  const monthIndex = Number.parseInt(month, 10) - 1
  const monthLabel = MONTH_LABELS[monthIndex]
  if (!monthLabel) {
    return `DAM ${intervalStart}`
  }

  return `DAM ${Number.parseInt(day, 10)} ${monthLabel}, ${hour}:${minute}`
}

export const timelineTooltipBody = (label: OperatorTimelineSegment['label'], powerMw: number): string => {
  if (label === 'Charge') {
    return `Recommended net power is ${formatSignedMw(powerMw)}, so the selected DAM delivery hour charges the battery inside a review-only schedule; not a live dispatch command.`
  }

  if (label === 'Discharge') {
    return `Recommended net power is ${formatSignedMw(powerMw)}, so the selected DAM delivery hour discharges stored energy inside a review-only schedule; not a submitted market bid.`
  }

  return `Recommended net power is ${formatSignedMw(powerMw)}, so the DAM delivery-hour preview keeps the battery idle and avoids unnecessary cycling.`
}
