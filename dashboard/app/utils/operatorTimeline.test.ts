import { describe, expect, it } from 'vitest'

import { formatDamDeliveryLabel, powerToTimelineLabel, timelineTooltipBody } from './operatorTimeline'

describe('operator timeline labels', () => {
  it('labels small nonzero dispatch proposals instead of hiding them as hold', () => {
    expect(powerToTimelineLabel(0.2)).toBe('Discharge')
    expect(powerToTimelineLabel(-0.2)).toBe('Charge')
    expect(powerToTimelineLabel(0.03)).toBe('Hold')
  })

  it('formats visible rows as generic delivery-hour labels', () => {
    expect(formatDamDeliveryLabel('2026-05-24T00:00:00Z')).toBe('Delivery 24 May, 00:00')
  })

  it('keeps tooltip copy inside the DAM/IDM delivery review boundary', () => {
    expect(timelineTooltipBody('Charge', -0.2)).toContain('market delivery hour')
    expect(timelineTooltipBody('Charge', -0.2)).toContain('not a live dispatch command')
    expect(timelineTooltipBody('Discharge', 0.2)).toContain('not a submitted market bid')
    expect(timelineTooltipBody('Hold', 0)).toContain('delivery-hour')
  })
})
