// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { existsSync, readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const barrelPath = fileURLToPath(new URL('./control-plane.ts', import.meta.url))
const moduleDirectoryUrl = new URL('./control-plane/', import.meta.url)
const expectedModules = [
  'controlPlaneCore.ts',
  'controlPlaneEvidence.ts',
  'controlPlaneOperator.ts'
]

const approxTokens = (text: string): number => Math.ceil(text.length / 4)

describe('control-plane type contract architecture', () => {
  it('keeps the public contract file as a bounded type export surface', () => {
    const barrel = readFileSync(barrelPath, 'utf8')

    expect(approxTokens(barrel), 'control-plane.ts should stay below 5000 approx tokens').toBeLessThan(5000)
    expect(barrel).toContain('./control-plane/controlPlaneCore')
    expect(barrel).toContain('./control-plane/controlPlaneEvidence')
    expect(barrel).toContain('./control-plane/controlPlaneOperator')
    expect(barrel).not.toContain('export interface OperatorRecommendationResponse')
  })

  it('keeps control-plane contract groups bounded', () => {
    for (const moduleName of expectedModules) {
      const modulePath = fileURLToPath(new URL(moduleName, moduleDirectoryUrl))

      expect(existsSync(modulePath), `${moduleName} should exist`).toBe(true)

      const source = readFileSync(modulePath, 'utf8')
      expect(approxTokens(source), `${moduleName} should stay below 5000 approx tokens`).toBeLessThan(5000)
    }
  })
})
