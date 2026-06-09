// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'

const utilsDirectoryUrl = new URL('../', import.meta.url)
const cssDirectoryUrl = new URL('../../assets/css/', import.meta.url)
const operatorHudCssPath = fileURLToPath(new URL('../../assets/css/operator-hud.css', import.meta.url))
const designTokensCssPath = fileURLToPath(new URL('../../assets/css/design-tokens.css', import.meta.url))
const cssImportPattern = /@import\s+"(?<path>\.\/[^"]+\.css)";/g

export const baselinePreviewComponentUrls = [
  '../components/dashboard/HudBaselinePreview.vue',
  '../components/dashboard/baseline/HudBaselinePreviewHeader.vue',
  '../components/dashboard/baseline/HudBaselineMetricStrips.vue',
  '../components/dashboard/baseline/HudBaselineChartGrid.vue',
  '../components/dashboard/baseline/HudBaselineExplainerGrid.vue',
  '../components/dashboard/baseline/HudBaselinePlanningBoundary.vue'
]

export const expectedOperatorHudPartials = [
  './operator-hud.shell.css',
  './operator-hud.layout.css',
  './operator-hud.market.css',
  './operator-hud.rail.css',
  './operator-hud.schedule.css',
  './operator-hud.responsive.css'
]

export const readDesignTokenBundle = (): string => {
  const manifestCss = readFileSync(designTokensCssPath, 'utf8')
  const imports = [...manifestCss.matchAll(/@import\s+"(?<path>\.\/design-tokens\.[^"]+\.css)";/g)]

  return [
    manifestCss,
    ...imports.map(match => readFileSync(
      fileURLToPath(new URL(match.groups?.path ?? '', cssDirectoryUrl)),
      'utf8'
    ))
  ].join('\n')
}

const readCssPartialWithImports = (partialUrl: string, seen = new Set<string>()): string => {
  if (seen.has(partialUrl)) {
    return ''
  }

  seen.add(partialUrl)

  const partialCss = readFileSync(
    fileURLToPath(new URL(partialUrl, cssDirectoryUrl)),
    'utf8'
  )
  const imports = [...partialCss.matchAll(cssImportPattern)]
  if (imports.length === 0) {
    return partialCss
  }

  return [
    partialCss,
    ...imports.map(match => readCssPartialWithImports(match.groups?.path ?? '', seen))
  ].join('\n')
}

export const readDashboardFixture = (fixtureUrl: string): string => {
  if (fixtureUrl.endsWith('design-tokens.css')) {
    return readDesignTokenBundle()
  }

  if (fixtureUrl.endsWith('operator-hud.rail.css')) {
    return readCssPartialWithImports('./operator-hud.rail.css')
  }

  return readFileSync(
    fileURLToPath(new URL(fixtureUrl, utilsDirectoryUrl)),
    'utf8'
  )
}

export const readOperatorHudRootCss = (): string => readFileSync(operatorHudCssPath, 'utf8')

export const readOperatorHudPartial = (partialUrl: string): string => readCssPartialWithImports(partialUrl)

export const readOperatorHudCss = (): string => {
  const rootCss = readOperatorHudRootCss()
  const imports = [...rootCss.matchAll(/@import\s+"(?<path>\.\/operator-hud\.[^"]+\.css)";/g)]
  if (imports.length === 0) {
    return rootCss
  }

  return imports
    .map(match => readOperatorHudPartial(match.groups?.path ?? ''))
    .join('\n')
}

export const readBaselinePreviewComponents = (): string => baselinePreviewComponentUrls
  .map(readDashboardFixture)
  .join('\n')

export const approxTokens = (text: string): number => Math.ceil(text.length / 4)

export const getSelectorBlock = (css: string, selector: string): string => {
  const escapedSelector = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const matches = [...css.matchAll(new RegExp(`${escapedSelector}\\s*\\{(?<body>[^}]*)\\}`, 'gm'))]
  return matches.map(match => match.groups?.body ?? '').join('\n')
}
