// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const utilsDirectoryUrl = new URL('./', import.meta.url)
const focusedShadowTestUrls = [
  './operatorShadowPreview.test.ts',
  './operatorShadowPreviewAdapter.test.ts',
  './operatorShadowPreviewTables.test.ts',
  './operatorShadowPreviewIntegration.test.ts'
]

const approxTokens = (text: string): number => Math.ceil(text.length / 4)
const readTestFile = (testUrl: string): string => readFileSync(
  fileURLToPath(new URL(testUrl, utilsDirectoryUrl)),
  'utf8'
)

describe('operator shadow preview test suite architecture', () => {
  it('keeps shadow preview tests split by source, adapter, table, and integration seams', () => {
    const rootTest = readTestFile('./operatorShadowPreview.test.ts')

    expect(rootTest).not.toContain('keeps the strategy comparison chart anchored')
    expect(rootTest).not.toContain('builds a comparison surface including direct DT')
    expect(rootTest).not.toContain('maps DT shadow rows into the same recommendation shape')

    for (const testUrl of focusedShadowTestUrls) {
      const testPath = fileURLToPath(new URL(testUrl, utilsDirectoryUrl))

      expect(existsSync(testPath), `${testUrl} should exist`).toBe(true)
      expect(approxTokens(readTestFile(testUrl)), `${testUrl} should stay focused`).toBeLessThan(1_900)
    }
  })
})
