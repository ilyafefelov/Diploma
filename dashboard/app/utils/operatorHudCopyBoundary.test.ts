import { describe, expect, it } from 'vitest'

import {
  readBaselinePreviewComponents,
  readDashboardFixture
} from './test-fixtures/operatorHudTestFixtures'

describe('operator HUD copy boundary', () => {
  it('keeps baseline comparator collapsed and scoped to selected market delivery metadata', () => {
    const baselinePreview = readBaselinePreviewComponents()

    expect(baselinePreview).not.toContain('animation: slab-sheen')
    expect(baselinePreview).not.toContain('@keyframes slab-sheen')
    expect(baselinePreview).toContain('isExpanded = ref(false)')
    expect(baselinePreview).toContain('Expand baseline')
    expect(baselinePreview).toContain('Collapse baseline')
    expect(baselinePreview).toContain('v-if="!isExpanded"')
    expect(baselinePreview).toContain('Compact view keeps baseline value')
    expect(baselinePreview).toContain('target_delivery_window_start')
    expect(baselinePreview).toContain('target_delivery_window_end')
    expect(baselinePreview).toContain('market_execution_enabled')
    expect(baselinePreview).toContain('proposed_bid_status')
    expect(baselinePreview).toContain('market_venue')
    expect(baselinePreview).toContain('delivery')
    expect(baselinePreview).toContain('No market execution')
  })

  it('keeps first viewport scoped to DAM/IDM hourly preview copy', () => {
    const topBar = readDashboardFixture('../components/dashboard/operator/OperatorTopBar.vue')
    const marketConsole = readDashboardFixture('../components/dashboard/operator/OperatorMarketConsole.vue')
    const marketSignalHero = readDashboardFixture('../components/dashboard/operator/OperatorMarketSignalHero.vue')
    const operatorPage = readDashboardFixture('../pages/operator.vue')
    const scheduleDock = readDashboardFixture('../components/dashboard/operator/OperatorScheduleDock.vue')
    const signalCharts = readDashboardFixture('../components/dashboard/HudSignalCharts.vue')
    const pageNarrativeModel = readDashboardFixture('../composables/useOperatorPageNarrativeModel.ts')
    const batteryPanel = readDashboardFixture('../components/dashboard/operator/OperatorBatteryPanel.vue')
    const gatekeeperPanel = readDashboardFixture('../components/dashboard/operator/OperatorGatekeeperPanel.vue')
    const moodPanel = readDashboardFixture('../components/dashboard/operator/OperatorMoodPanel.vue')

    expect(topBar).toContain('Operator Preview')
    expect(topBar).not.toContain('BESS Control')
    expect(topBar).toContain('Preview gaps')
    expect(marketConsole).toContain('DAM/IDM hourly recommendation preview')
    expect(marketConsole).toContain('marketVenueOptions')
    expect(marketConsole).toContain('targetDateShortcuts')
    expect(marketConsole).toContain('chartHorizonOptions')
    expect(marketConsole).toContain('Official/source row first')
    expect(marketConsole).toContain('marketPreviewError')
    expect(marketConsole).toContain('hasMarketPreviewError')
    expect(marketConsole).toContain('hasMarketPreviewError.value ? null : props.operatorRecommendation')
    expect(operatorPage).toContain(':operator-recommendation="operatorRecommendation"')
    expect(marketConsole).not.toContain('DAM / IDM arbitrage surface')
    expect(marketSignalHero).toContain('selectedMarketVenue')
    expect(marketSignalHero).toContain('marketVenueLabel')
    expect(marketSignalHero).toContain('Source-backed preview blocker')
    expect(marketSignalHero).toContain('price context')
    expect(marketSignalHero).not.toContain('DAM delivery price')
    expect(marketSignalHero).not.toContain('label="IDM"')
    expect(marketSignalHero).not.toContain('label="Both"')
    expect(marketSignalHero).not.toContain('DAM context price')
    expect(scheduleDock).toContain('hourly delivery review')
    expect(scheduleDock).toContain('Review mode')
    expect(scheduleDock).not.toContain('Schedule timeline')
    expect(scheduleDock).not.toContain('Dispatch mode')
    expect(signalCharts).toContain('Selected schedule and value preview')
    expect(signalCharts).toContain('Review context for selected preview')
    expect(signalCharts).toContain('No substitute prices are rendered')
    expect(signalCharts).not.toContain('Use now: context for selected preview')
    expect(batteryPanel).toContain('First preview action')
    expect(batteryPanel).toContain('DAM/IDM delivery-hour preview')
    expect(batteryPanel).not.toContain('Intent to dispatch')
    expect(gatekeeperPanel).toContain('Preview scorer')
    expect(gatekeeperPanel).toContain('DAM/IDM delivery-hour preference')
    expect(gatekeeperPanel).not.toContain('Pydantic gatekeeper')
    expect(moodPanel).toContain('Preview posture')
    expect(moodPanel).not.toContain('Operator mood')
    expect(moodPanel).not.toContain('Great')
    expect(pageNarrativeModel).toContain('DAM/IDM hourly preview / no ProposedBid / no market submission')
  })

  it('keeps market and future-stack labels inside the DAM/IDM delivery review boundary', () => {
    const marketSignalHero = readDashboardFixture('../components/dashboard/operator/OperatorMarketSignalHero.vue')
    const futurePanelModel = readDashboardFixture('../composables/useOperatorFutureStackPanelModel.ts')
    const forecastChartCard = readDashboardFixture('../components/dashboard/operator/future-charts/OperatorForecastStackChartCard.vue')
    const policyChartCard = readDashboardFixture('../components/dashboard/operator/future-charts/OperatorPolicyValueChartCard.vue')

    expect(marketSignalHero).toContain('latestPricePeriodLabel')
    expect(marketSignalHero).toContain('forecastWindowPeriodLabel')
    expect(marketSignalHero).not.toContain('Latest visible hour')
    expect(futurePanelModel).toContain('DAM/IDM hourly schedule review')
    expect(futurePanelModel).toContain('selectedChartHorizon')
    expect(futurePanelModel).toContain('DAM/IDM delivery-hour')
    expect(futurePanelModel).toContain('no live IDM bid or market submission')
    expect(futurePanelModel).toContain('Selected DAM/IDM net power')
    expect(futurePanelModel).toContain('official or scenario price context')
    expect(futurePanelModel).toContain('not a value metric')
    expect(futurePanelModel).not.toContain('Selected strategy schedule')
    expect(forecastChartCard).toContain('aria-label="Forecast chart legend"')
    expect(forecastChartCard).toContain('Price context')
    expect(forecastChartCard).toContain('Battery MW')
    expect(forecastChartCard).toContain('SOC')
    expect(forecastChartCard).toContain('Site load')
    expect(forecastChartCard).toContain('NBEATSx/TFT rows are not independent model evidence')
    expect(policyChartCard).toContain('aria-label="Policy value chart legend"')
    expect(policyChartCard).toContain('Battery MW')
    expect(policyChartCard).toContain('Shortfall')
    expect(policyChartCard).toContain('Price context')
    expect(policyChartCard).toContain('max(0, strict LP/reference value - selected preview value), UAH')
    expect(policyChartCard).toContain('not a value metric')
  })

  it('does not leave current-facing stale DAM-only or synthetic market copy in the operator HUD', () => {
    const currentFacingFiles = [
      '../components/dashboard/operator/OperatorMarketConsole.vue',
      '../components/dashboard/operator/OperatorMarketSignalHero.vue',
      '../components/dashboard/operator/OperatorScheduleDock.vue',
      '../components/dashboard/HudSignalCharts.vue',
      '../components/dashboard/signal/HudSignalMarketExplainers.vue',
      '../lib/charts/dashboardSignalMarketPulseChart.ts',
      '../lib/charts/dashboardBaselineChartOptions.ts',
      '../lib/operator-dashboard/useOperatorTimelineModel.ts',
      '../lib/operator-future/operatorFutureForecastPanelModel.ts',
      '../composables/useOperatorFutureStackPanelModel.ts'
    ].map(readDashboardFixture).join('\n')

    expect(currentFacingFiles).not.toContain('IDM disabled')
    expect(currentFacingFiles).not.toContain('DAM-only')
    expect(currentFacingFiles).not.toContain('no IDM recommendation mode')
    expect(currentFacingFiles).not.toContain('synthetic DAM history')
    expect(currentFacingFiles).not.toContain('non-submittable DAM ')
    expect(currentFacingFiles).not.toContain('Current MVP path: HourlyDamBaselineSolver over tenant-aware DAM history')
  })
})
