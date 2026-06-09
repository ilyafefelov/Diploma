export interface WheelDeltaLike {
  deltaY: number
  deltaMode: number
}

export interface VerticalScrollState {
  scrollTop: number
  scrollHeight: number
  clientHeight: number
}

export interface RootWheelForwardingInput {
  deltaY: number
  nestedScrollables: VerticalScrollState[]
}

export interface RootWheelTargetInput {
  currentScrollY: number
  deltaY: number
  maxScrollY: number
}

const wheelLinePixels = 16
const scrollTolerancePixels = 1
const wheelScrollableOverflowYValues = new Set(['auto', 'scroll', 'overlay'])

export const wheelDeltaToPixels = (
  event: WheelDeltaLike,
  viewportHeight: number
): number => {
  if (event.deltaMode === 1) {
    return event.deltaY * wheelLinePixels
  }

  if (event.deltaMode === 2) {
    return event.deltaY * viewportHeight
  }

  return event.deltaY
}

export const canConsumeVerticalWheel = (
  scrollable: VerticalScrollState,
  deltaY: number
): boolean => {
  if (deltaY < 0) {
    return scrollable.scrollTop > scrollTolerancePixels
  }

  if (deltaY > 0) {
    return scrollable.scrollTop + scrollable.clientHeight < scrollable.scrollHeight - scrollTolerancePixels
  }

  return false
}

export const shouldForwardWheelToRoot = ({
  deltaY,
  nestedScrollables
}: RootWheelForwardingInput): boolean => {
  if (deltaY === 0) {
    return false
  }

  return !nestedScrollables.some(scrollable => canConsumeVerticalWheel(scrollable, deltaY))
}

export const clampRootWheelTarget = ({
  currentScrollY,
  deltaY,
  maxScrollY
}: RootWheelTargetInput): number => {
  return Math.min(Math.max(currentScrollY + deltaY, 0), maxScrollY)
}

export const isWheelScrollableOverflowY = (overflowY: string): boolean => {
  return wheelScrollableOverflowYValues.has(overflowY)
}

export const collectNestedVerticalScrollables = (target: EventTarget | null): VerticalScrollState[] => {
  if (!(target instanceof Element)) {
    return []
  }

  const scrollables: VerticalScrollState[] = []
  let node: Element | null = target

  while (node && node !== document.body && node !== document.documentElement) {
    const overflowY = window.getComputedStyle(node).overflowY

    if (
      isWheelScrollableOverflowY(overflowY)
      && node.scrollHeight > node.clientHeight
    ) {
      scrollables.push({
        scrollTop: node.scrollTop,
        scrollHeight: node.scrollHeight,
        clientHeight: node.clientHeight
      })
    }

    node = node.parentElement
  }

  return scrollables
}
