// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { readdirSync, readFileSync, statSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const appRoot = fileURLToPath(new URL('../', import.meta.url))
const sourceExtensions = new Set(['.css', '.ts', '.vue'])
// The public index is a standalone GitHub Pages evidence surface. Keep the
// operator/defense dashboard budget strict while allowing that static page.
const publicIndexExceptions = new Set([
  '/assets/css/bess-index.css',
  '/components/public/BessActionPreviewRail.vue',
  '/components/public/BessDispatchField.vue',
  '/utils/publicBessNarrativeArchitecture.test.ts',
  '/pages/ukraine-bess-arbitrage-index.vue'
])

const approxTokens = (text: string): number => Math.ceil(text.length / 4)

const collectSourceFiles = (directory: string): string[] => {
  const entries = readdirSync(directory)

  return entries.flatMap((entry: string) => {
    const path = `${directory}/${entry}`
    const stats = statSync(path)

    if (stats.isDirectory()) {
      return collectSourceFiles(path)
    }

    return sourceExtensions.has(entry.slice(entry.lastIndexOf('.')))
      ? [path]
      : []
  })
}

describe('dashboard file-size architecture', () => {
  it('keeps dashboard source, style, and test files below the reviewable size budget', () => {
    const oversizedFiles = collectSourceFiles(appRoot)
      .map(path => ({
        path: path.replace(appRoot, ''),
        tokens: approxTokens(readFileSync(path, 'utf8'))
      }))
      .filter(file => file.tokens >= 5000)
      .filter(file => !publicIndexExceptions.has(file.path))

    expect(oversizedFiles).toEqual([])
  })
})
