import { describe, expect, it } from 'vitest'

import { powerToTimelineLabel } from './operatorTimeline'

describe('operator timeline labels', () => {
  it('labels small nonzero dispatch proposals instead of hiding them as hold', () => {
    expect(powerToTimelineLabel(0.2)).toBe('Discharge')
    expect(powerToTimelineLabel(-0.2)).toBe('Charge')
    expect(powerToTimelineLabel(0.03)).toBe('Hold')
  })
})
