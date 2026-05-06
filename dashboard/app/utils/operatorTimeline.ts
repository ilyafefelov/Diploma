import type { OperatorTimelineSegment } from '~/types/operator-dashboard'

const TIMELINE_ACTION_EPSILON_MW = 0.05

export const formatSignedMw = (value: number): string => `${value > 0 ? '+' : ''}${value.toFixed(1)} MW`

export const powerToTimelineLabel = (powerMw: number): OperatorTimelineSegment['label'] => {
  if (powerMw > TIMELINE_ACTION_EPSILON_MW) {
    return 'Discharge'
  }

  if (powerMw < -TIMELINE_ACTION_EPSILON_MW) {
    return 'Charge'
  }

  return 'Hold'
}

export const timelineTooltipBody = (label: OperatorTimelineSegment['label'], powerMw: number): string => {
  if (label === 'Charge') {
    return `Recommended net power is ${formatSignedMw(powerMw)}, so the baseline preview is filling the battery for a later market window.`
  }

  if (label === 'Discharge') {
    return `Recommended net power is ${formatSignedMw(powerMw)}, so the baseline preview is selling stored energy into this interval.`
  }

  return `Recommended net power is ${formatSignedMw(powerMw)}, so the preview keeps the battery idle and avoids unnecessary cycling.`
}
