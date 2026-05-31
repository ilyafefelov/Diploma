import { onBeforeUnmount, onMounted } from 'vue'

import {
  clampRootWheelTarget,
  collectNestedVerticalScrollables,
  shouldForwardWheelToRoot,
  wheelDeltaToPixels
} from '~/utils/rootWheelScrollFallback'

const operatorScrollRootClass = 'operator-scroll-root'

const operatorWheelListenerOptions: AddEventListenerOptions = {
  capture: true,
  passive: true
}

export const useOperatorRootScrollRecovery = (): void => {
  useHead({
    htmlAttrs: {
      class: operatorScrollRootClass
    },
    bodyAttrs: {
      class: operatorScrollRootClass
    }
  })

  let pendingRootWheelDeltaY = 0
  let pendingRootWheelFrameId: number | null = null
  let pendingRootWheelBaselineY: number | null = null

  const getMaxScrollY = (): number => {
    return Math.max(
      0,
      Math.max(document.documentElement.scrollHeight, document.body.scrollHeight) - window.innerHeight
    )
  }

  const flushPendingRootWheel = (): void => {
    pendingRootWheelFrameId = null

    if (
      pendingRootWheelBaselineY !== null
      && Math.abs(window.scrollY - pendingRootWheelBaselineY) > 1
    ) {
      pendingRootWheelDeltaY = 0
      pendingRootWheelBaselineY = null
      return
    }

    pendingRootWheelBaselineY = null

    const currentScrollY = window.scrollY
    const maxScrollY = getMaxScrollY()
    const targetScrollY = clampRootWheelTarget({
      currentScrollY,
      deltaY: pendingRootWheelDeltaY,
      maxScrollY
    })

    if (Math.abs(targetScrollY - currentScrollY) <= 1) {
      if (targetScrollY === 0 || targetScrollY === maxScrollY) {
        pendingRootWheelDeltaY = 0
      }

      return
    }

    pendingRootWheelDeltaY = 0
    window.scrollTo({ top: targetScrollY, left: window.scrollX, behavior: 'auto' })
  }

  const scheduleRootWheel = (deltaY: number): void => {
    pendingRootWheelDeltaY += deltaY

    if (pendingRootWheelFrameId !== null) {
      return
    }

    pendingRootWheelBaselineY = window.scrollY
    pendingRootWheelFrameId = window.requestAnimationFrame(flushPendingRootWheel)
  }

  const handleOperatorRootWheel = (event: WheelEvent): void => {
    if (event.ctrlKey || event.metaKey) {
      return
    }

    if (Math.abs(event.deltaX) > Math.abs(event.deltaY)) {
      return
    }

    const deltaY = wheelDeltaToPixels(event, window.innerHeight)
    const shouldForward = shouldForwardWheelToRoot({
      deltaY,
      nestedScrollables: collectNestedVerticalScrollables(event.target)
    })

    if (!shouldForward) {
      return
    }

    scheduleRootWheel(deltaY)
  }

  onMounted(() => {
    document.documentElement.classList.add(operatorScrollRootClass)
    document.body.classList.add(operatorScrollRootClass)
    window.addEventListener('wheel', handleOperatorRootWheel, operatorWheelListenerOptions)
  })

  onBeforeUnmount(() => {
    if (pendingRootWheelFrameId !== null) {
      window.cancelAnimationFrame(pendingRootWheelFrameId)
      pendingRootWheelFrameId = null
    }

    pendingRootWheelBaselineY = null
    pendingRootWheelDeltaY = 0
    window.removeEventListener('wheel', handleOperatorRootWheel, operatorWheelListenerOptions)
    document.documentElement.classList.remove(operatorScrollRootClass)
    document.body.classList.remove(operatorScrollRootClass)
  })
}
