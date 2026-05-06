import type { BaselineLpPreview, DashboardBatteryStateResponse } from '../types/control-plane'

export interface OperatorBatteryDisplay {
  socPercent: number
  sohPercent: number
  socSourceLabel: string
  sohSourceLabel: string
  socFormula: string
  sohFormula: string
  telemetryIngestLabel: string
  telemetryIngestTooltip: string
}

export const buildOperatorBatteryDisplay = (input: {
  batteryState: DashboardBatteryStateResponse | null
  baselinePreview: BaselineLpPreview | null
}): OperatorBatteryDisplay => {
  const telemetryIngestLabel = formatTelemetryIngestLabel(input.batteryState)
  const telemetryIngestTooltip = formatTelemetryIngestTooltip(input.batteryState)
  const latestTelemetry = input.batteryState?.latest_telemetry
  if (latestTelemetry) {
    return {
      socPercent: toPercent(latestTelemetry.current_soc),
      sohPercent: toPercent(latestTelemetry.soh),
      socSourceLabel: 'latest telemetry',
      sohSourceLabel: 'latest telemetry',
      socFormula: 'SOC = latest_telemetry.current_soc * 100',
      sohFormula: 'SOH = latest_telemetry.soh * 100',
      telemetryIngestLabel,
      telemetryIngestTooltip
    }
  }

  const hourlySnapshot = input.batteryState?.hourly_snapshot
  if (hourlySnapshot) {
    return {
      socPercent: toPercent(hourlySnapshot.soc_close),
      sohPercent: toPercent(hourlySnapshot.soh_close),
      socSourceLabel: `hourly snapshot: ${hourlySnapshot.telemetry_freshness}`,
      sohSourceLabel: 'hourly snapshot',
      socFormula: 'SOC = hourly_snapshot.soc_close * 100',
      sohFormula: 'SOH = hourly_snapshot.soh_close * 100',
      telemetryIngestLabel,
      telemetryIngestTooltip
    }
  }

  if (input.baselinePreview) {
    const throughput = input.baselinePreview.economics.total_throughput_mwh

    return {
      socPercent: toPercent(input.baselinePreview.starting_soc_fraction),
      sohPercent: Math.round(Math.max(91, 96.2 - throughput * 0.12)),
      socSourceLabel: 'baseline LP starting SOC',
      sohSourceLabel: 'Level 1 throughput proxy',
      socFormula: 'SOC = baseline_lp.starting_soc_fraction * 100',
      sohFormula: 'SOH_proxy = max(91, 96.2 - total_throughput_mwh * 0.12)',
      telemetryIngestLabel,
      telemetryIngestTooltip
    }
  }

  return {
    socPercent: 62,
    sohPercent: 96,
    socSourceLabel: 'display default',
    sohSourceLabel: 'display default',
    socFormula: 'SOC = default display value when telemetry and baseline are unavailable',
    sohFormula: 'SOH = default display value when telemetry and baseline are unavailable',
    telemetryIngestLabel,
    telemetryIngestTooltip
  }
}

const toPercent = (value: number): number => Math.round(value * 100)

const formatTelemetryIngestLabel = (batteryState: DashboardBatteryStateResponse | null): string => {
  const source = batteryState?.telemetry_ingest_source
  if (!source) {
    return 'MQTT ingest path pending'
  }

  return `${source.protocol.toUpperCase()} ${source.broker_host}:${source.broker_port}`
}

const formatTelemetryIngestTooltip = (batteryState: DashboardBatteryStateResponse | null): string => {
  const source = batteryState?.telemetry_ingest_source
  if (!source) {
    return 'Battery telemetry ingest path has not been returned by the control plane yet.'
  }

  return `Configured ingest path only, not a connectivity probe. Topic: ${source.topic}. Source kind: ${source.source_kind}.`
}
