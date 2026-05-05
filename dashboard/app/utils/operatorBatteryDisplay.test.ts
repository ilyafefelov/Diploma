import { describe, expect, it } from 'vitest'

import { buildOperatorBatteryDisplay } from './operatorBatteryDisplay'
import type { BaselineLpPreview, DashboardBatteryStateResponse } from '../types/control-plane'

describe('operator battery display', () => {
  it('prefers latest telemetry SOC and SOH for live operator display', () => {
    const batteryState: DashboardBatteryStateResponse = {
      tenant_id: 'client_003_dnipro_factory',
      latest_telemetry: {
        tenant_id: 'client_003_dnipro_factory',
        observed_at: '2026-05-05T12:00:00Z',
        current_soc: 0.64,
        soh: 0.96,
        power_mw: 0.02,
        temperature_c: 24,
        source: 'simulated_mqtt',
        source_kind: 'observed'
      },
      hourly_snapshot: {
        tenant_id: 'client_003_dnipro_factory',
        snapshot_hour: '2026-05-05T11:00:00Z',
        observation_count: 12,
        soc_open: 0.58,
        soc_close: 0.59,
        soc_mean: 0.6,
        soh_close: 0.97,
        power_mw_mean: 0.01,
        throughput_mwh: 0.2,
        efc_delta: 0.02,
        telemetry_freshness: 'fresh',
        first_observed_at: '2026-05-05T11:00:00Z',
        last_observed_at: '2026-05-05T11:55:00Z'
      },
      fallback_reason: null
    }

    expect(buildOperatorBatteryDisplay({ batteryState, baselinePreview: null })).toMatchObject({
      socPercent: 64,
      sohPercent: 96,
      socSourceLabel: 'latest telemetry',
      sohSourceLabel: 'latest telemetry',
      socFormula: 'SOC = latest_telemetry.current_soc * 100'
    })
  })

  it('falls back to hourly telemetry snapshot before baseline preview', () => {
    const batteryState: DashboardBatteryStateResponse = {
      tenant_id: 'client_003_dnipro_factory',
      latest_telemetry: null,
      hourly_snapshot: {
        tenant_id: 'client_003_dnipro_factory',
        snapshot_hour: '2026-05-05T11:00:00Z',
        observation_count: 12,
        soc_open: 0.58,
        soc_close: 0.59,
        soc_mean: 0.6,
        soh_close: 0.97,
        power_mw_mean: 0.01,
        throughput_mwh: 0.2,
        efc_delta: 0.02,
        telemetry_freshness: 'stale',
        first_observed_at: '2026-05-05T11:00:00Z',
        last_observed_at: '2026-05-05T11:55:00Z'
      },
      fallback_reason: 'hourly_snapshot_stale'
    }

    expect(buildOperatorBatteryDisplay({ batteryState, baselinePreview: null })).toMatchObject({
      socPercent: 59,
      sohPercent: 97,
      socSourceLabel: 'hourly snapshot: stale',
      sohSourceLabel: 'hourly snapshot',
      socFormula: 'SOC = hourly_snapshot.soc_close * 100'
    })
  })

  it('uses baseline preview when telemetry is unavailable', () => {
    const baselinePreview = {
      starting_soc_fraction: 0.52,
      economics: { total_throughput_mwh: 1.5 }
    } as BaselineLpPreview

    expect(buildOperatorBatteryDisplay({ batteryState: null, baselinePreview })).toMatchObject({
      socPercent: 52,
      sohPercent: 96,
      socSourceLabel: 'baseline LP starting SOC',
      sohSourceLabel: 'Level 1 throughput proxy'
    })
  })
})
