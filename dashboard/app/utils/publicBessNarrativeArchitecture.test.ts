import { describe, expect, it } from 'vitest'

import { readDashboardFixture } from './test-fixtures/operatorHudTestFixtures'

describe('public BESS narrative architecture', () => {
  it('renders the public route from committed JSON artifacts only', () => {
    const page = readDashboardFixture('../pages/ukraine-bess-arbitrage-index.vue')
    const field = readDashboardFixture('../components/public/BessDispatchField.vue')

    expect(page).toContain("'/data/bess-arbitrage-index/latest.json'")
    expect(page).toContain("'/data/bess-arbitrage-index/history.json'")
    expect(page).toContain("'/data/bess-arbitrage-index/forecast/latest.json'")
    expect(page).toContain("'/data/bess-arbitrage-index/forecast_scoreboard.json'")
    expect(page).toContain("'/data/bess-arbitrage-index/publication_status.json'")
    expect(page).toContain('<BessDispatchField')
    expect(page).toContain('SVG evidence charts below remain the analytical source of truth')
    expect(page).toContain('GitHub Pages redeploys')
    expect(page).not.toContain('GitHub to Vercel publication lane')

    expect(field).toContain("await import('three')")
    expect(field).toContain('prefers-reduced-motion: reduce')
    expect(field).toContain('visibilitychange')
    expect(field).toContain('Math.min(window.devicePixelRatio || 1, 1.5)')
  })

  it('keeps the public claim boundary explicit and non-executing', () => {
    const page = readDashboardFixture('../pages/ukraine-bess-arbitrage-index.vue')

    expect(page).toContain('No market execution')
    expect(page).toContain('not_market_execution')
    expect(page).toContain('not_emitted')
    expect(page).toContain('DT / HF DT challenger')
    expect(page).toContain('never default market execution')
    expect(page).not.toContain('Market bids generated')
    expect(page).not.toContain('Forecast guaranteed')
    expect(page).not.toContain('DT controller deployed')
  })

  it('covers published, blocked, and empty forecast artifact states', () => {
    const page = readDashboardFixture('../pages/ukraine-bess-arbitrage-index.vue')
    const latest = JSON.parse(readDashboardFixture('../../public/data/bess-arbitrage-index/latest.json'))
    const forecast = JSON.parse(readDashboardFixture('../../public/data/bess-arbitrage-index/forecast/latest.json'))
    const scoreboard = JSON.parse(readDashboardFixture('../../public/data/bess-arbitrage-index/forecast_scoreboard.json'))
    const publicationStatus = JSON.parse(readDashboardFixture('../../public/data/bess-arbitrage-index/publication_status.json'))

    expect(latest.market_execution_enabled).toBe(false)
    expect(latest.proposed_bid_status).toBe('not_emitted')
    expect(latest.source.row_count).toBeGreaterThan(0)
    expect(latest.presets.length).toBeGreaterThan(0)

    expect(forecast.market_execution_enabled).toBe(false)
    expect(forecast.models.some((model: Record<string, unknown>) => model.backend_status === 'blocked')).toBe(true)
    expect(page).toContain('bess-status--blocked')
    expect(page).toContain('model.backend_status ===')

    expect(scoreboard.row_count).toBe(0)
    expect(scoreboard.rows).toEqual([])
    expect(publicationStatus.realized.is_current_for_kyiv_schedule).toBe(true)
    expect(publicationStatus.forecast.is_current_for_kyiv_schedule).toBe(true)
    expect(publicationStatus.autonomy.compute_layer).toBe('github_actions_scheduled_static_json')
    expect(page).toContain('No scored forecast pairs yet.')
  })
})
