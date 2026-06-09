import { mkdir, writeFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import os from 'node:os'
import path from 'node:path'

const requireFromDashboard = createRequire(new URL('../dashboard/package.json', import.meta.url))
const { chromium } = requireFromDashboard('playwright')

const operatorUrl = process.env.SMART_ARBITRAGE_OPERATOR_URL ?? 'http://localhost:64163/operator'
const smokeToday = process.env.SMART_ARBITRAGE_BROWSER_SMOKE_TODAY ?? '2026-06-02'
const screenshotDir = process.env.SMART_ARBITRAGE_BROWSER_SMOKE_DIR
  ?? path.join(os.tmpdir(), 'smart-arbitrage-hf-value-aligned-shadow-smoke')
const headless = process.env.SMART_ARBITRAGE_BROWSER_SMOKE_HEADED !== '1'
const tenantId = process.env.SMART_ARBITRAGE_BROWSER_SMOKE_TENANT ?? 'client_003_dnipro_factory'

const dateIso = (baseDate, offsetDays) => {
  const parsed = new Date(`${baseDate}T00:00:00.000Z`)
  parsed.setUTCDate(parsed.getUTCDate() + offsetDays)
  return parsed.toISOString().slice(0, 10)
}

const cases = [
  { id: 'dam_latest_official', venue: 'DAM', targetDate: null },
  { id: 'dam_today', venue: 'DAM', targetDate: smokeToday },
  { id: 'dam_tomorrow', venue: 'DAM', targetDate: dateIso(smokeToday, 1) },
  { id: 'dam_day_plus_2', venue: 'DAM', targetDate: dateIso(smokeToday, 2) },
  { id: 'idm_latest_official', venue: 'IDM', targetDate: null },
  { id: 'idm_today', venue: 'IDM', targetDate: smokeToday },
  { id: 'idm_tomorrow', venue: 'IDM', targetDate: dateIso(smokeToday, 1) },
  { id: 'idm_day_plus_2', venue: 'IDM', targetDate: dateIso(smokeToday, 2) }
]

const queryFor = ({ venue, targetDate }) => {
  const params = new URLSearchParams({
    tenant_id: tenantId,
    preview_source: 'hf_live_safe_switch_value_aligned_shadow',
    market_venue: venue
  })
  if (targetDate) {
    params.set('target_delivery_date', targetDate)
  }
  return params
}

const assert = (condition, message) => {
  if (!condition) {
    throw new Error(message)
  }
}

const waitForText = async (page, text, timeout = 30000) => {
  await page.getByText(text, { exact: false }).first().waitFor({ timeout })
}

const selectHfValueAlignedSource = async (page) => {
  const directButton = page.getByText('HF live safe-switch value-aligned shadow', { exact: false }).first()
  if (await directButton.count()) {
    await directButton.click({ timeout: 10000 }).catch(() => {})
  }

  const bodyText = await page.locator('body').innerText({ timeout: 10000 })
  if (bodyText.includes('HF live safe-switch value-aligned shadow')) {
    return
  }

  const forecastActionPreset = page.getByRole('button', { name: /Forecast DAM action/i }).first()
  if (await forecastActionPreset.count()) {
    await forecastActionPreset.click({ timeout: 10000 })
    return
  }

  const selects = await page.locator('select').all()
  for (const select of selects) {
    const options = await select.locator('option').allTextContents()
    const matchIndex = options.findIndex(option => option.includes('HF live safe-switch value-aligned shadow'))
    if (matchIndex >= 0) {
      const values = await select.locator('option').evaluateAll(nodes => nodes.map(node => node.value))
      await select.selectOption(values[matchIndex])
      return
    }
  }

  throw new Error('HF value-aligned shadow source selector was not found')
}

const setVenue = async (page, venue) => {
  await page.getByRole('button', { name: venue }).first().click({ timeout: 10000 })
}

const setTargetDate = async (page, targetDate) => {
  if (!targetDate) {
    await page.getByRole('button', { name: /Latest official/i }).first().click({ timeout: 10000 })
    return
  }
  const input = page.locator('input[type="date"]').first()
  await input.fill(targetDate, { timeout: 10000 })
  await input.dispatchEvent('change')
}

const scheduleActionCount = schedule => schedule
  .filter(row => String(row.action ?? '').toUpperCase() !== 'HOLD')
  .length

const main = async () => {
  await mkdir(screenshotDir, { recursive: true })

  const browser = await chromium.launch({ headless })
  const page = await browser.newPage({ viewport: { width: 1440, height: 1800 } })

  const consoleIssues = []
  const pageErrors = []
  const operatorRecommendationRequests = []
  const shadowResponses = []

  page.on('console', message => {
    if (['error', 'warning'].includes(message.type())) {
      consoleIssues.push(`${message.type()}: ${message.text()}`)
    }
  })
  page.on('pageerror', error => pageErrors.push(error.message))
  page.on('request', request => {
    if (request.url().includes('/dashboard/operator-recommendation')) {
      operatorRecommendationRequests.push(request.url())
    }
  })
  page.on('response', async response => {
    if (!response.url().includes('/dashboard/shadow-recommendation-preview')) {
      return
    }
    try {
      shadowResponses.push({
        url: response.url(),
        status: response.status(),
        body: await response.json()
      })
    }
    catch {
      shadowResponses.push({
        url: response.url(),
        status: response.status(),
        body: null
      })
    }
  })

  await page.goto(operatorUrl, { waitUntil: 'domcontentloaded', timeout: 60000 })
  await waitForText(page, 'Operator Preview')
  await waitForText(page, 'Dnipro Manufacturing Plant', 60000)

  const requestCountBeforeHfSelection = operatorRecommendationRequests.length
  await selectHfValueAlignedSource(page)
  await waitForText(page, 'HF live safe-switch value-aligned shadow')

  const caseResults = []
  for (const smokeCase of cases) {
    await setVenue(page, smokeCase.venue)
    await setTargetDate(page, smokeCase.targetDate)

    const params = queryFor(smokeCase)
    const apiUrl = new URL('/api/control-plane/dashboard/shadow-recommendation-preview', operatorUrl)
    apiUrl.search = params.toString()
    const apiResponse = await page.request.get(apiUrl.toString())
    const payload = await apiResponse.json()

    assert(apiResponse.ok(), `${smokeCase.id} API failed with ${apiResponse.status()}`)
    assert(payload.preview_source_id === 'hf_live_safe_switch_value_aligned_shadow', `${smokeCase.id} returned wrong preview source`)
    assert(payload.market_execution_enabled === false, `${smokeCase.id} enabled market execution`)
    assert(payload.promotion_gate_passed === false, `${smokeCase.id} passed production promotion gate`)
    assert(payload.proposed_bid === undefined, `${smokeCase.id} emitted proposed_bid`)
    assert(payload.market_order_payload === undefined, `${smokeCase.id} emitted market_order_payload`)

    const metrics = payload.comparison_metrics ?? {}
    assert(metrics.market_order_payload_emitted === 0, `${smokeCase.id} reported market payload emission`)
    assert(metrics.source_backed_price_context_available === 1, `${smokeCase.id} did not load source-backed context`)
    assert(metrics.forecast_rows_loaded === 24, `${smokeCase.id} did not load 24 forecast/price rows`)
    assert((payload.recommendation_schedule ?? []).length === 24, `${smokeCase.id} did not return 24 schedule rows`)

    await page.waitForTimeout(750)
    const bodyText = await page.locator('body').innerText({ timeout: 10000 })
    const screenshotPath = path.join(screenshotDir, `${smokeCase.id}.png`)
    await page.screenshot({ path: screenshotPath, fullPage: true })

    const uiHasHfLabel = bodyText.includes('HF live safe-switch value-aligned shadow')
    const uiHasControlIssue = bodyText.includes('CONTROL SURFACE ISSUE')
    const uiHasStaleV2Chip = bodyText.includes('Selected V2+ evidence')
    const uiHasStaleProofDate = bodyText.includes('2026-05-02')
    const uiHasBlankPolicyGraph = bodyText.includes('No trade preview is shown')
      || bodyText.includes('Preview pending')

    assert(uiHasHfLabel, `${smokeCase.id} did not show the selected HF label`)
    assert(!uiHasControlIssue, `${smokeCase.id} showed a control-surface issue`)
    assert(!uiHasStaleV2Chip, `${smokeCase.id} still showed stale Selected V2+ chip`)
    assert(!uiHasStaleProofDate, `${smokeCase.id} showed stale 2026-05-02 proof fallback`)
    assert(!uiHasBlankPolicyGraph, `${smokeCase.id} showed a blank/pending policy graph`)

    caseResults.push({
      id: smokeCase.id,
      venue: smokeCase.venue,
      target_date: smokeCase.targetDate ?? 'latest_official',
      api_rows: payload.recommendation_schedule.length,
      api_non_hold_rows: scheduleActionCount(payload.recommendation_schedule),
      source_context: metrics.official_published === 1
        ? 'official_published'
        : metrics.same_day_forecast_refresh === 1
          ? 'same_day_forecast_refresh'
          : metrics.request_fallback_materialized === 1
            ? 'request_fallback_materialized'
            : 'pre_publication_forecast',
      guard_abstained_to_safe_fallback: metrics.guard_abstained_to_safe_fallback ?? null,
      screenshot_path: screenshotPath
    })
  }

  await browser.close()

  const newOperatorRecommendationRequests = operatorRecommendationRequests.slice(requestCountBeforeHfSelection)
  assert(newOperatorRecommendationRequests.length === 0, 'HF shadow selection called /dashboard/operator-recommendation')
  assert(pageErrors.length === 0, `page errors were raised: ${pageErrors.join('; ')}`)
  assert(consoleIssues.length === 0, `console issues were raised: ${consoleIssues.join('; ')}`)

  const summary = {
    operator_url: operatorUrl,
    smoke_today: smokeToday,
    screenshot_dir: screenshotDir,
    cases: caseResults,
    shadow_response_count: shadowResponses.length,
    operator_recommendation_requests_after_hf_selection: newOperatorRecommendationRequests,
    console_issues: consoleIssues,
    page_errors: pageErrors,
    market_execution_enabled: false,
    market_order_payload_emitted: false
  }
  const summaryPath = path.join(screenshotDir, 'summary.json')
  await writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`)
  console.log(`HF value-aligned shadow browser smoke passed: ${summaryPath}`)
}

main().catch(error => {
  console.error(error)
  process.exit(1)
})
