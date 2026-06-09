// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const presentationPath = fileURLToPath(new URL('./operatorFutureStackPresentation.ts', import.meta.url))
const previewSourcesPath = fileURLToPath(
  new URL('../lib/operator-future/operatorFuturePreviewSources.ts', import.meta.url)
)

const approxTokens = (text: string): number => Math.ceil(text.length / 4)

describe('operator future-stack presentation architecture', () => {
  it('keeps preview-source catalog behind a focused module', () => {
    const presentation = readFileSync(presentationPath, 'utf8')

    expect(presentation).toContain('../lib/operator-future/operatorFuturePreviewSources')
    expect(presentation).not.toContain('DEFAULT_PREVIEW_SOURCE_OPTIONS')
    expect(approxTokens(presentation), 'presentation helper should stay focused').toBeLessThan(3000)

    expect(existsSync(previewSourcesPath), 'operatorFuturePreviewSources.ts should exist').toBe(true)

    const previewSources = readFileSync(previewSourcesPath, 'utf8')
    expect(previewSources).toContain('DEFAULT_PREVIEW_SOURCE_OPTIONS')
    expect(previewSources).toContain('formatPreviewSourceOptionLabel')
    expect(previewSources).toContain('buildPreviewSourceSelectItems')
    expect(approxTokens(previewSources), 'preview-source module should stay bounded').toBeLessThan(1800)
  })
})
