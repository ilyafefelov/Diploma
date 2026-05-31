// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

import { approxTokens } from './test-fixtures/operatorHudTestFixtures'

const utilsDirectoryUrl = new URL('./', import.meta.url)
const focusedHudTestUrls = [
  './operatorHudCss.test.ts',
  './operatorHudScrollArchitecture.test.ts',
  './operatorHudAccessibilityArchitecture.test.ts',
  './operatorHudCopyBoundary.test.ts',
  './operatorHudChartArchitecture.test.ts'
]

const readTestFile = (testUrl: string): string => readFileSync(
  fileURLToPath(new URL(testUrl, utilsDirectoryUrl)),
  'utf8'
)

describe('operator HUD test suite architecture', () => {
  it('keeps HUD architecture tests split into focused bounded modules', () => {
    const rootHudCssTest = readTestFile('./operatorHudCss.test.ts')

    expect(rootHudCssTest).not.toContain('names non-native focusable operator info surfaces')
    expect(rootHudCssTest).not.toContain('keeps the operator first viewport scoped')
    expect(rootHudCssTest).not.toContain('keeps operator chart components SSR-safe')

    for (const testUrl of focusedHudTestUrls) {
      const path = fileURLToPath(new URL(testUrl, utilsDirectoryUrl))

      expect(existsSync(path), `${testUrl} should exist`).toBe(true)
      expect(approxTokens(readTestFile(testUrl)), `${testUrl} should stay focused`).toBeLessThan(1_900)
    }
  })
})
