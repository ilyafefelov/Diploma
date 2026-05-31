// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { readFileSync } from 'node:fs'
// @ts-expect-error Vitest runs this in Node; the dashboard intentionally has no @types/node dependency.
import { fileURLToPath } from 'node:url'

import { describe, expect, it } from 'vitest'

import {
  canConsumeVerticalWheel,
  clampRootWheelTarget,
  isWheelScrollableOverflowY,
  shouldForwardWheelToRoot,
  wheelDeltaToPixels
} from './rootWheelScrollFallback'

const operatorPagePath = fileURLToPath(new URL('../pages/operator.vue', import.meta.url))
const operatorSidebarPath = fileURLToPath(new URL('../components/dashboard/operator/OperatorSidebar.vue', import.meta.url))
const operatorRootScrollRecoveryPath = fileURLToPath(new URL('../composables/useOperatorRootScrollRecovery.ts', import.meta.url))
const mainCssPath = fileURLToPath(new URL('../assets/css/main.css', import.meta.url))

const getSelectorBlock = (css: string, selector: string): string => {
  const escapedSelector = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const matches = [...css.matchAll(new RegExp(`${escapedSelector}\\s*\\{(?<body>[^}]*)\\}`, 'gm'))]
  return matches.map(match => match.groups?.body ?? '').join('\n')
}

describe('root wheel scroll fallback', () => {
  it('normalizes wheel units into viewport pixels', () => {
    expect(wheelDeltaToPixels({ deltaY: 24, deltaMode: 0 }, 900)).toBe(24)
    expect(wheelDeltaToPixels({ deltaY: 3, deltaMode: 1 }, 900)).toBe(48)
    expect(wheelDeltaToPixels({ deltaY: 1, deltaMode: 2 }, 900)).toBe(900)
  })

  it('lets nested vertical scroll containers consume wheel movement until they reach an edge', () => {
    expect(canConsumeVerticalWheel({
      scrollTop: 24,
      scrollHeight: 400,
      clientHeight: 200
    }, -80)).toBe(true)

    expect(canConsumeVerticalWheel({
      scrollTop: 80,
      scrollHeight: 400,
      clientHeight: 200
    }, 80)).toBe(true)

    expect(canConsumeVerticalWheel({
      scrollTop: 0,
      scrollHeight: 400,
      clientHeight: 200
    }, -80)).toBe(false)

    expect(canConsumeVerticalWheel({
      scrollTop: 200,
      scrollHeight: 400,
      clientHeight: 200
    }, 80)).toBe(false)
  })

  it('forwards wheel input when no nested vertical scroller can use it', () => {
    expect(shouldForwardWheelToRoot({
      deltaY: 80,
      nestedScrollables: []
    })).toBe(true)

    expect(shouldForwardWheelToRoot({
      deltaY: 0.35,
      nestedScrollables: []
    })).toBe(true)

    expect(shouldForwardWheelToRoot({
      deltaY: 0,
      nestedScrollables: []
    })).toBe(false)

    expect(shouldForwardWheelToRoot({
      deltaY: 80,
      nestedScrollables: [{
        scrollTop: 64,
        scrollHeight: 400,
        clientHeight: 200
      }]
    })).toBe(false)
  })

  it('clamps forwarded root wheel movement to document boundaries', () => {
    expect(clampRootWheelTarget({
      currentScrollY: 1800,
      deltaY: 500,
      maxScrollY: 4000
    })).toBe(2300)

    expect(clampRootWheelTarget({
      currentScrollY: 1800,
      deltaY: -500,
      maxScrollY: 4000
    })).toBe(1300)

    expect(clampRootWheelTarget({
      currentScrollY: 120,
      deltaY: -500,
      maxScrollY: 4000
    })).toBe(0)

    expect(clampRootWheelTarget({
      currentScrollY: 3900,
      deltaY: 500,
      maxScrollY: 4000
    })).toBe(4000)
  })

  it('only treats user-scrollable overflow containers as nested wheel consumers', () => {
    expect(isWheelScrollableOverflowY('auto')).toBe(true)
    expect(isWheelScrollableOverflowY('scroll')).toBe(true)
    expect(isWheelScrollableOverflowY('overlay')).toBe(true)
    expect(isWheelScrollableOverflowY('hidden')).toBe(false)
    expect(isWheelScrollableOverflowY('clip')).toBe(false)
    expect(isWheelScrollableOverflowY('visible')).toBe(false)
  })

  it('keeps wheel recovery at the operator route seam, not inside one DOM shell or select adapter', () => {
    const operatorPage = readFileSync(operatorPagePath, 'utf8')
    const sidebar = readFileSync(operatorSidebarPath, 'utf8')

    expect(operatorPage).toContain('useOperatorRootScrollRecovery()')
    expect(operatorPage).not.toContain('window.addEventListener(\'wheel\'')
    expect(operatorPage).not.toContain('window.removeEventListener(\'wheel\'')
    expect(operatorPage).not.toContain('@wheel.capture')
    expect(sidebar).not.toContain('@wheel.capture.prevent')
    expect(sidebar).not.toContain('window.scrollBy')
  })

  it('restores document scrolling through an isolated route recovery module', () => {
    const recovery = readFileSync(operatorRootScrollRecoveryPath, 'utf8')
    const mainCss = readFileSync(mainCssPath, 'utf8')

    expect(recovery).toContain('operator-scroll-root')
    expect(recovery).toContain('useHead({')
    expect(recovery).toContain('htmlAttrs')
    expect(recovery).toContain('bodyAttrs')
    expect(recovery).toContain('document.documentElement.classList.add')
    expect(recovery).toContain('document.documentElement.classList.remove')
    expect(recovery).toContain('window.addEventListener(\'wheel\', handleOperatorRootWheel')
    expect(recovery).toContain('window.removeEventListener(\'wheel\', handleOperatorRootWheel')
    expect(recovery).toContain('shouldForwardWheelToRoot')
    expect(recovery).toContain('collectNestedVerticalScrollables')
    expect(recovery).toContain('clampRootWheelTarget')
    expect(recovery).toContain('passive: true')
    expect(recovery).toContain('pendingRootWheelDeltaY')
    expect(recovery).toContain('pendingRootWheelBaselineY')
    expect(recovery).toContain('window.requestAnimationFrame')
    expect(recovery).toContain('window.cancelAnimationFrame')
    expect(recovery).toContain('window.scrollTo({')
    expect(recovery).not.toContain('event.preventDefault()')
    expect(recovery).not.toContain('passive: false')
    expect(getSelectorBlock(mainCss, 'html.operator-scroll-root')).toContain('overflow-y: auto !important')
    expect(getSelectorBlock(mainCss, 'html.operator-scroll-root body')).toContain('overflow: visible !important')
    expect(getSelectorBlock(mainCss, 'body.operator-scroll-root')).toContain('overflow: visible !important')
    expect(mainCss).toContain('body.operator-scroll-root #__nuxt')
    expect(mainCss).toContain('body.operator-scroll-root .min-h-screen')
    expect(mainCss).toContain('body.operator-scroll-root .operator-shell')
    expect(mainCss).toContain('html.operator-scroll-root .min-h-screen')
    expect(mainCss).toContain('html.operator-scroll-root .operator-shell')
    expect(mainCss).toContain('height: auto !important')
    expect(mainCss).toContain('touch-action: pan-y')
    expect(mainCss).toContain('-webkit-overflow-scrolling: touch')
  })
})
