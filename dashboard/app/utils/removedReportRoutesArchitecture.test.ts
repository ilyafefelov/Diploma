// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const appShellPath = fileURLToPath(new URL('../app.vue', import.meta.url))
const week1PagePath = fileURLToPath(new URL('../pages/week1/interactive_report1.vue', import.meta.url))
const week1ComposablePath = fileURLToPath(new URL('../composables/useWeek1InteractiveReport.ts', import.meta.url))
const week1CssPath = fileURLToPath(new URL('../assets/css/week1-interactive-report.css', import.meta.url))
const week1SocialImagePath = fileURLToPath(new URL('../../public/social/week1-interactive-report.svg', import.meta.url))
const week1DocsPath = fileURLToPath(new URL('../../../docs/thesis/weekly-reports/week1', import.meta.url))

const docsEntrypointPaths = [
  fileURLToPath(new URL('../../../docs/README.md', import.meta.url)),
  fileURLToPath(new URL('../../../docs/technical/OPERATOR_DEMO_READY.md', import.meta.url)),
  fileURLToPath(new URL('../../../docs/technical/API_ENDPOINTS.md', import.meta.url))
]

describe('removed report routes architecture', () => {
  it('keeps the retired Week 1 interactive report out of the dashboard bundle', () => {
    const appShell = readFileSync(appShellPath, 'utf8')

    expect(existsSync(week1PagePath), 'retired Week 1 route should not exist').toBe(false)
    expect(existsSync(week1ComposablePath), 'retired Week 1 model composable should not exist').toBe(false)
    expect(existsSync(week1CssPath), 'retired Week 1 stylesheet should not exist').toBe(false)
    expect(existsSync(week1SocialImagePath), 'retired Week 1 social image should not exist').toBe(false)
    expect(appShell).not.toContain('week1-interactive-report')
    expect(appShell).not.toContain('/week1/')
  })

  it('keeps the retired Week 1 report package out of documentation entrypoints', () => {
    expect(existsSync(week1DocsPath), 'retired Week 1 docs package should not exist').toBe(false)

    for (const docsPath of docsEntrypointPaths) {
      const docs = readFileSync(docsPath, 'utf8')

      expect(docs).not.toMatch(/week1|Week 1|interactive_report1|weekly-reports\/week1/)
    }
  })
})
