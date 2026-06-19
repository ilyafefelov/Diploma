<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, shallowRef, watch } from 'vue'

type DispatchPoint = {
  timestamp?: string
  price_uah_mwh?: number | string
  net_power_mw?: number | string
  soc_after_mwh?: number | string
  net_value_uah?: number | string
}

type ViewMode = 'perspective' | 'plan'
type ThreeModule = typeof import('three')
type ThreeSceneObjects = {
  THREE: ThreeModule
  renderer: import('three').WebGLRenderer
  scene: import('three').Scene
  camera: import('three').PerspectiveCamera
  bars: import('three').Mesh[]
  floorStrips: import('three').Mesh[]
  priceCurtains: import('three').Mesh[]
  priceLine: import('three').Mesh
  priceGlow: import('three').Mesh
  priceRibbon: import('three').Mesh
  priceSurface: import('three').Mesh
  socRibbon: import('three').Mesh
  fieldFrame: import('three').LineSegments
  priceWall: import('three').LineSegments
  wave: import('three').Line
  basePlane: import('three').Mesh
  selectionMarker: import('three').Mesh
  selectionBeam: import('three').Mesh
  selectionNeedle: import('three').Mesh
  selectionCap: import('three').Mesh
  frameId: number | null
  startedAt: number
}

const props = defineProps<{
  schedule: DispatchPoint[]
  sourceStatus: string
  presetLabel: string
  capacityMwh?: number | string
}>()

const emit = defineEmits<{
  fallback: [reason: string]
}>()

const rootEl = ref<HTMLElement | null>(null)
const sceneObjects = shallowRef<ThreeSceneObjects | null>(null)
const reducedMotion = ref(false)
const webglFailed = ref(false)
const selectedIndex = ref(0)
const viewMode = ref<ViewMode>('perspective')
const isSceneNearViewport = ref(true)
let intersectionObserver: IntersectionObserver | null = null
let activePointerId: number | null = null
let pointerStartX = 0
let pointerStartY = 0
let pointerStartRotation = 0
let interactionRotation = 0
let interactionZoom = 0
const viewModeOptions: Array<{ id: ViewMode, label: string, description: string }> = [
  { id: 'perspective', label: '3D', description: 'Perspective dispatch field' },
  { id: 'plan', label: 'Plan', description: 'Flattened plan view' }
]

const visibleRows = computed(() => props.schedule.filter(point => Number.isFinite(numberValue(point.price_uah_mwh))))
const sceneRows = computed(() => visibleRows.value.slice(0, 24))
const hasSceneData = computed(() => visibleRows.value.length >= 2 && !props.sourceStatus.startsWith('blocked'))
const selectedPoint = computed(() => sceneRows.value[selectedIndex.value] || sceneRows.value[bestDefaultIndex(sceneRows.value)] || null)
const selectedAction = computed(() => actionFor(selectedPoint.value))
const selectedSocPercent = computed(() => {
  const capacity = numberValue(props.capacityMwh)
  if (!selectedPoint.value || capacity <= 0) {
    return null
  }
  return numberValue(selectedPoint.value.soc_after_mwh) / capacity * 100
})
const selectedPositionStyle = computed(() => {
  const denominator = Math.max(1, sceneRows.value.length - 1)
  const projectedX = 10 + (selectedIndex.value / denominator) * 78
  return {
    '--bess-selected-x': `${projectedX}%`
  }
})
const priceScaleTicks = computed(() => {
  const prices = sceneRows.value.map(point => numberValue(point.price_uah_mwh))
  if (prices.length === 0) {
    return []
  }
  const minPrice = Math.min(...prices)
  const maxPrice = Math.max(...prices)
  const midpoint = minPrice + (maxPrice - minPrice) / 2
  return [
    { label: formatNumber(maxPrice, 0), position: '19%' },
    { label: formatNumber(midpoint, 0), position: '45%' },
    { label: formatNumber(minPrice, 0), position: '72%' }
  ]
})
const powerScaleTicks = computed(() => {
  const maxPower = Math.max(0.001, ...sceneRows.value.map(point => Math.abs(numberValue(point.net_power_mw))))
  return [
    { label: `+${formatNumber(maxPower, 2)}`, position: '37%' },
    { label: '0.00', position: '52%' },
    { label: `-${formatNumber(maxPower, 2)}`, position: '67%' }
  ]
})
const fieldReceiptRows = computed(() => {
  const rows = sceneRows.value
  if (rows.length === 0) {
    return []
  }
  const prices = rows.map(point => numberValue(point.price_uah_mwh))
  const chargeHours = rows.filter(point => numberValue(point.net_power_mw) < -0.0001).length
  const dischargeHours = rows.filter(point => numberValue(point.net_power_mw) > 0.0001).length
  return [
    {
      label: 'Price span',
      value: `${formatNumber(Math.min(...prices), 0)}-${formatNumber(Math.max(...prices), 0)}`
    },
    {
      label: 'Dispatch hours',
      value: `${dischargeHours} out / ${chargeHours} in`
    },
    {
      label: 'Selected value',
      value: selectedPoint.value ? formatUah(selectedPoint.value.net_value_uah) : 'pending'
    }
  ]
})
const signalMarkers = computed<Array<{ key: string, label: string, value: string, hour: string, index: number, tone: string }>>(() => {
  const rows = sceneRows.value
  if (rows.length === 0) {
    return []
  }
  let peakPriceIndex = 0
  let valleyPriceIndex = 0
  let dispatchIndex = 0
  let peakPrice = -Infinity
  let valleyPrice = Infinity
  let dispatchMagnitude = -Infinity
  rows.forEach((row, index) => {
    const price = numberValue(row.price_uah_mwh)
    const power = numberValue(row.net_power_mw)
    if (price > peakPrice) {
      peakPrice = price
      peakPriceIndex = index
    }
    if (price < valleyPrice) {
      valleyPrice = price
      valleyPriceIndex = index
    }
    if (Math.abs(power) > dispatchMagnitude) {
      dispatchMagnitude = Math.abs(power)
      dispatchIndex = index
    }
  })

  const used = new Set<number>()
  const markers: Array<{ key: string, label: string, value: string, hour: string, index: number, tone: string }> = []
  const addMarker = (key: string, label: string, index: number, value: string, tone: string) => {
    if (used.has(index)) {
      return
    }
    used.add(index)
    markers.push({
      key,
      label,
      index,
      value,
      tone,
      hour: hourLabel(rows[index]?.timestamp)
    })
  }
  addMarker('peak-price', 'Peak price', peakPriceIndex, `${formatNumber(rows[peakPriceIndex]?.price_uah_mwh, 0)} UAH/MWh`, 'peak')
  addMarker('low-price', 'Low price', valleyPriceIndex, `${formatNumber(rows[valleyPriceIndex]?.price_uah_mwh, 0)} UAH/MWh`, 'valley')
  addMarker('largest-move', 'Largest move', dispatchIndex, formatMw(rows[dispatchIndex]?.net_power_mw), actionFor(rows[dispatchIndex] ?? null).className)
  return markers
})

const fallbackPlate = computed(() => {
  const rows = sceneRows.value
  const prices = rows.map(point => numberValue(point.price_uah_mwh))
  const powers = rows.map(point => numberValue(point.net_power_mw))
  const minPrice = Math.min(...prices, 0)
  const maxPrice = Math.max(...prices, 1)
  const priceRange = Math.max(1, maxPrice - minPrice)
  const maxPower = Math.max(0.001, ...powers.map(value => Math.abs(value)))
  const width = 680
  const height = 300
  const baseline = 198
  const left = 32
  const span = width - left * 2
  const bars = rows.map((point, index) => {
    const power = numberValue(point.net_power_mw)
    const magnitude = Math.max(3, Math.abs(power) / maxPower * 116)
    const x = left + (index / Math.max(1, rows.length - 1)) * span
    return {
      x: x - 5,
      y: power >= 0 ? baseline - magnitude : baseline,
      height: magnitude,
      tone: power >= 0 ? 'discharge' : 'charge'
    }
  })
  const line = rows.map((point, index) => {
    const x = left + (index / Math.max(1, rows.length - 1)) * span
    const y = 42 + (1 - ((numberValue(point.price_uah_mwh) - minPrice) / priceRange)) * 112
    return `${x.toFixed(1)},${y.toFixed(1)}`
  }).join(' ')
  return { width, height, baseline, bars, line }
})

onMounted(async () => {
  reducedMotion.value = window.matchMedia('(prefers-reduced-motion: reduce)').matches
  if (reducedMotion.value) {
    emit('fallback', 'reduced_motion')
    return
  }
  if (!hasSceneData.value) {
    emit('fallback', 'no_complete_dispatch_rows')
    return
  }
  await nextTick()
  await mountScene()
})

onBeforeUnmount(() => {
  destroyScene()
})

watch(
  () => [props.schedule, props.sourceStatus],
  async () => {
    if (reducedMotion.value || webglFailed.value) {
      return
    }
    if (!hasSceneData.value) {
      if (sceneObjects.value) {
        destroyScene()
      }
      emit('fallback', 'no_complete_dispatch_rows')
      return
    }
    emit('fallback', '')
    await nextTick()
    if (sceneObjects.value) {
      updateSceneData(sceneObjects.value)
      return
    }
    await mountScene()
  },
  { deep: true }
)

watch(
  sceneRows,
  (rows) => {
    if (rows.length === 0) {
      selectedIndex.value = 0
      return
    }
    if (selectedIndex.value >= rows.length || selectedIndex.value === 0) {
      selectedIndex.value = bestDefaultIndex(rows)
    }
  },
  { immediate: true }
)

watch(selectedIndex, () => {
  if (sceneObjects.value) {
    updateSceneData(sceneObjects.value)
  }
})

watch(viewMode, () => {
  const objects = sceneObjects.value
  if (!objects) {
    return
  }
  frameCamera(objects)
  objects.renderer.render(objects.scene, objects.camera)
})

async function mountScene() {
  if (!rootEl.value) {
    return
  }
  try {
    const THREE = await import('three')
    const renderer = new THREE.WebGLRenderer({
      alpha: true,
      antialias: true,
      powerPreference: 'high-performance'
    })
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.5))
    renderer.setSize(rootEl.value.clientWidth, rootEl.value.clientHeight, false)
    renderer.shadowMap.enabled = true
    renderer.shadowMap.type = THREE.PCFShadowMap
    renderer.outputColorSpace = THREE.SRGBColorSpace
    renderer.toneMapping = THREE.ACESFilmicToneMapping
    renderer.toneMappingExposure = 1.02
    renderer.setClearColor(0xffffff, 0)
    rootEl.value.appendChild(renderer.domElement)

    const scene = new THREE.Scene()
    scene.fog = new THREE.Fog(0xf7fdff, 12.5, 24)
    const camera = new THREE.PerspectiveCamera(34, rootEl.value.clientWidth / rootEl.value.clientHeight, 0.1, 100)

    const ambient = new THREE.AmbientLight(0xf8fdff, 2.05)
    scene.add(ambient)
    const fillLight = new THREE.HemisphereLight(0xffffff, 0xd8f2fb, 1.05)
    scene.add(fillLight)
    const keyLight = new THREE.DirectionalLight(0xe8f8ff, 1.82)
    keyLight.position.set(2.8, 5.8, 5.4)
    keyLight.castShadow = true
    scene.add(keyLight)
    const rimLight = new THREE.DirectionalLight(0x9eddf2, 0.62)
    rimLight.position.set(-5.2, 3.6, -4.4)
    scene.add(rimLight)

    const basePlane = new THREE.Mesh(
      new THREE.PlaneGeometry(14.9, 6.8),
      new THREE.MeshStandardMaterial({
        color: 0xf9fdff,
        roughness: 0.9,
        metalness: 0,
        transparent: true,
        opacity: 0.46,
        side: THREE.DoubleSide
      })
    )
    basePlane.rotation.x = -Math.PI / 2
    basePlane.position.y = -0.08
    basePlane.receiveShadow = true
    scene.add(basePlane)

    const grid = new THREE.GridHelper(14.2, 24, 0x68bdd8, 0xd0e8f1)
    grid.position.y = -0.035
    const gridMaterials = Array.isArray(grid.material) ? grid.material : [grid.material]
    gridMaterials.forEach((material) => {
      material.transparent = true
      material.opacity = 0.38
    })
    scene.add(grid)

    const fieldFrame = new THREE.LineSegments(
      fieldFrameGeometryFor(THREE, 12.2, 5.15),
      new THREE.LineBasicMaterial({
        color: 0x86cde2,
        transparent: true,
        opacity: 0.5,
        depthWrite: false
      })
    )
    fieldFrame.position.y = -0.02
    scene.add(fieldFrame)

    const priceWall = new THREE.LineSegments(
      priceWallGeometryFor(THREE, 12.2, 2.52, 5),
      new THREE.LineBasicMaterial({
        color: 0x78c8df,
        transparent: true,
        opacity: 0.18,
        depthWrite: false
      })
    )
    priceWall.position.z = -2.5
    scene.add(priceWall)

    const bars: import('three').Mesh[] = []
    const floorStrips: import('three').Mesh[] = []
    const priceCurtains: import('three').Mesh[] = []
    const barMaterial = new THREE.MeshPhysicalMaterial({
      color: 0x2e8bea,
      roughness: 0.24,
      metalness: 0.04,
      transmission: 0.08,
      thickness: 0.28,
      clearcoat: 0.56,
      clearcoatRoughness: 0.2,
      transparent: true,
      opacity: 0.86
    })
    const chargeMaterial = new THREE.MeshPhysicalMaterial({
      color: 0xf0bd4f,
      roughness: 0.25,
      metalness: 0.02,
      transmission: 0.06,
      thickness: 0.22,
      clearcoat: 0.5,
      clearcoatRoughness: 0.22,
      transparent: true,
      opacity: 0.84
    })
    for (let index = 0; index < 24; index += 1) {
      const bar = new THREE.Mesh(new THREE.CylinderGeometry(0.165, 0.19, 1, 28, 1), index % 2 ? chargeMaterial.clone() : barMaterial.clone())
      bar.castShadow = true
      scene.add(bar)
      bars.push(bar)

      const floorStrip = new THREE.Mesh(
        new THREE.CircleGeometry(0.34, 36),
      new THREE.MeshBasicMaterial({
        color: 0x8fd7ec,
        transparent: true,
        opacity: 0.14,
          side: THREE.DoubleSide,
          depthWrite: false
        })
      )
      floorStrip.rotation.x = -Math.PI / 2
      floorStrip.position.y = -0.02
      scene.add(floorStrip)
      floorStrips.push(floorStrip)

      const priceCurtain = new THREE.Mesh(
        new THREE.PlaneGeometry(0.095, 1),
        new THREE.MeshBasicMaterial({
          color: 0x9fe4f4,
          transparent: true,
          opacity: 0.045,
          side: THREE.DoubleSide,
          depthWrite: false
        })
      )
      priceCurtain.position.z = -1.08
      scene.add(priceCurtain)
      priceCurtains.push(priceCurtain)
    }

    const priceGlow = new THREE.Mesh(
      new THREE.BufferGeometry(),
      new THREE.MeshBasicMaterial({
        color: 0x8fd7ec,
        transparent: true,
        opacity: 0.28,
        depthWrite: false
      })
    )
    scene.add(priceGlow)

    const priceLine = new THREE.Mesh(
      new THREE.BufferGeometry(),
      new THREE.MeshBasicMaterial({
        color: 0x187fc8,
        transparent: true,
        opacity: 0.92,
        depthWrite: false
      })
    )
    scene.add(priceLine)

    const priceRibbon = new THREE.Mesh(
      new THREE.BufferGeometry(),
      new THREE.MeshBasicMaterial({
        color: 0x8fd7ec,
        transparent: true,
        opacity: 0.34,
        side: THREE.DoubleSide,
        depthWrite: false
      })
    )
    scene.add(priceRibbon)

    const priceSurface = new THREE.Mesh(
      new THREE.BufferGeometry(),
      new THREE.MeshBasicMaterial({
        color: 0x9fe4f4,
        transparent: true,
        opacity: 0.18,
        side: THREE.DoubleSide,
        depthWrite: false
      })
    )
    scene.add(priceSurface)

    const socRibbon = new THREE.Mesh(
      new THREE.BufferGeometry(),
      new THREE.MeshBasicMaterial({
        color: 0x75d5c3,
        transparent: true,
        opacity: 0.32,
        side: THREE.DoubleSide,
        depthWrite: false
      })
    )
    scene.add(socRibbon)

    const wave = new THREE.Line(
      new THREE.BufferGeometry(),
      new THREE.LineBasicMaterial({ color: 0xa8dfef, transparent: true, opacity: 0.3 })
    )
    scene.add(wave)

    const selectionMarker = new THREE.Mesh(
      new THREE.TorusGeometry(0.33, 0.02, 12, 64),
      new THREE.MeshBasicMaterial({ color: 0x1b9dca, transparent: true, opacity: 0.42 })
    )
    selectionMarker.rotation.x = Math.PI / 2
    selectionMarker.position.y = 0.035
    scene.add(selectionMarker)

    const selectionBeam = new THREE.Mesh(
      new THREE.PlaneGeometry(1.55, 3.15),
      new THREE.MeshBasicMaterial({
        color: 0x5cc5e8,
        transparent: true,
        opacity: 0.085,
        side: THREE.DoubleSide,
        depthWrite: false
      })
    )
    selectionBeam.rotation.y = Math.PI / 2
    selectionBeam.position.y = 1.45
    scene.add(selectionBeam)

    const selectionNeedle = new THREE.Mesh(
      new THREE.CylinderGeometry(0.017, 0.017, 1, 12),
      new THREE.MeshBasicMaterial({
        color: 0x0c7eb3,
        transparent: true,
        opacity: 0.42,
        depthWrite: false
      })
    )
    scene.add(selectionNeedle)

    const selectionCap = new THREE.Mesh(
      new THREE.SphereGeometry(0.065, 18, 18),
      new THREE.MeshBasicMaterial({
        color: 0x0c7eb3,
        transparent: true,
        opacity: 0.72,
        depthWrite: false
      })
    )
    scene.add(selectionCap)

    sceneObjects.value = {
      THREE,
      renderer,
      scene,
      camera,
      bars,
      floorStrips,
      priceCurtains,
      priceLine,
      priceGlow,
      priceRibbon,
      priceSurface,
      socRibbon,
      fieldFrame,
      priceWall,
      wave,
      basePlane,
      selectionMarker,
      selectionBeam,
      selectionNeedle,
      selectionCap,
      frameId: null,
      startedAt: performance.now()
    }
    frameCamera(sceneObjects.value)
    updateSceneData(sceneObjects.value)
    window.addEventListener('resize', resizeScene)
    document.addEventListener('visibilitychange', handleVisibilityChange)
    attachVisibilityObserver()
    resumeAnimation()
  } catch (error) {
    webglFailed.value = true
    emit('fallback', error instanceof Error ? error.message : 'webgl_unavailable')
    destroyScene()
  }
}

function updateSceneData(objects: ThreeSceneObjects) {
  const rows = sceneRows.value
  const prices = rows.map(point => numberValue(point.price_uah_mwh))
  const powers = rows.map(point => numberValue(point.net_power_mw))
  const minPrice = Math.min(...prices)
  const maxPrice = Math.max(...prices)
  const priceRange = Math.max(1, maxPrice - minPrice)
  const maxPower = Math.max(0.001, ...powers.map(value => Math.abs(value)))
  const barSpan = 12.2

  objects.bars.forEach((bar, index) => {
    const point = rows[index]
    const floorStrip = objects.floorStrips[index]
    const priceCurtain = objects.priceCurtains[index]
    if (!point || !floorStrip || !priceCurtain) {
      bar.visible = false
      if (floorStrip) {
        floorStrip.visible = false
      }
      if (priceCurtain) {
        priceCurtain.visible = false
      }
      return
    }
    bar.visible = true
    floorStrip.visible = true
    priceCurtain.visible = true
    const x = -barSpan / 2 + (index / Math.max(1, rows.length - 1)) * barSpan
    const power = numberValue(point?.net_power_mw)
    const priceNormalized = (numberValue(point.price_uah_mwh) - minPrice) / priceRange
    const priceHeight = 0.14 + priceNormalized * 1.9
    const isSelected = index === selectedIndex.value
    const isHold = Math.abs(power) < 0.0001
    const normalizedPower = Math.abs(power) / maxPower
    const dispatchHeight = 0.34 + Math.sqrt(normalizedPower) * 1.32
    const holdHeight = 0.48 + priceNormalized * 0.92
    const height = isHold ? holdHeight : dispatchHeight
    const selectedLift = isSelected ? 1.005 : 1
    const widthLift = isSelected ? 1.06 : 1
    const radiusLift = isHold ? 0.98 : 0.96
    const laneZ = isHold ? 0.02 : power >= 0 ? -0.33 : 0.33
    bar.scale.set(widthLift * radiusLift, height * selectedLift, widthLift * radiusLift)
    bar.position.set(x, (isHold || power >= 0) ? height * selectedLift / 2 : -height * selectedLift * 0.42, laneZ)
    bar.rotation.y = 0.08
    bar.userData.baseScaleY = height * selectedLift
    bar.userData.power = power
    bar.userData.isHold = isHold
    bar.userData.phase = index * 0.47
    const material = bar.material as import('three').MeshStandardMaterial
    if (isHold) {
      material.color.set(isSelected ? 0x26b79b : 0x55c6b7)
      material.opacity = isSelected ? 0.8 : 0.66
    } else {
      material.color.set(power >= 0 ? 0x2e8bea : 0xf0bd4f)
      material.opacity = isSelected ? 0.9 : 0.76
    }
    material.emissive.set(isSelected ? (isHold ? 0x1d796c : power >= 0 ? 0x154e8c : 0x805a12) : 0x000000)
    material.emissiveIntensity = isSelected ? 0.1 : 0

    floorStrip.position.x = x
    floorStrip.position.z = laneZ
    floorStrip.scale.set(isSelected ? 1.3 : 0.94, isSelected ? 0.78 : 0.52, 1)
    const floorMaterial = floorStrip.material as import('three').MeshBasicMaterial
    if (isHold) {
      floorMaterial.color.set(0x62cbbb)
      floorMaterial.opacity = isSelected ? 0.28 : 0.15
    } else if (power >= 0) {
      floorMaterial.color.set(0x2e8bea)
      floorMaterial.opacity = isSelected ? 0.32 : 0.16
    } else {
      floorMaterial.color.set(0xf0bd4f)
      floorMaterial.opacity = isSelected ? 0.34 : 0.17
    }

    priceCurtain.position.set(x, Math.max(0.12, priceHeight / 2), -1.08)
    priceCurtain.scale.set(isSelected ? 1.18 : 0.82, Math.max(0.08, priceHeight), 1)
    priceCurtain.userData.baseOpacity = isSelected ? 0.11 : 0.024 + priceNormalized * 0.045
    priceCurtain.userData.phase = index * 0.29
    const curtainMaterial = priceCurtain.material as import('three').MeshBasicMaterial
    curtainMaterial.color.set(isSelected ? 0x5cc5e8 : priceNormalized > 0.66 ? 0x7ecfe9 : 0xb8e9f4)
    curtainMaterial.opacity = Number(priceCurtain.userData.baseOpacity)
  })

  const selectedRow = rows[selectedIndex.value]
  if (selectedRow) {
    const selectedX = -barSpan / 2 + (selectedIndex.value / Math.max(1, rows.length - 1)) * barSpan
    const selectedPriceNormalized = (numberValue(selectedRow.price_uah_mwh) - minPrice) / priceRange
    const selectedPriceHeight = 0.18 + selectedPriceNormalized * 2.16
    objects.selectionMarker.visible = true
    objects.selectionMarker.position.x = selectedX
    objects.selectionMarker.position.z = 0
    objects.selectionBeam.visible = true
    objects.selectionBeam.position.x = selectedX
    objects.selectionNeedle.visible = true
    objects.selectionNeedle.position.set(selectedX, Math.max(0.12, selectedPriceHeight / 2), -1.08)
    objects.selectionNeedle.scale.set(1, Math.max(0.24, selectedPriceHeight), 1)
    objects.selectionNeedle.userData.baseHeight = Math.max(0.24, selectedPriceHeight)
    objects.selectionCap.visible = true
    objects.selectionCap.position.set(selectedX, Math.max(0.18, selectedPriceHeight), -1.08)
  } else {
    objects.selectionMarker.visible = false
    objects.selectionBeam.visible = false
    objects.selectionNeedle.visible = false
    objects.selectionCap.visible = false
  }

  const linePoints = smoothPricePoints(objects.THREE, rows, minPrice, priceRange, barSpan, -0.86, 0.18, 2.16)
  const priceCurve = linePoints.length > 1 ? new objects.THREE.CatmullRomCurve3(linePoints, false, 'centripetal', 0.22) : null
  objects.priceLine.geometry.dispose()
  objects.priceGlow.geometry.dispose()
  objects.priceLine.geometry = priceCurve
    ? new objects.THREE.TubeGeometry(priceCurve, Math.max(96, linePoints.length), 0.024, 10, false)
    : new objects.THREE.BufferGeometry().setFromPoints(linePoints)
  objects.priceGlow.geometry = priceCurve
      ? new objects.THREE.TubeGeometry(priceCurve, Math.max(96, linePoints.length), 0.12, 12, false)
    : new objects.THREE.BufferGeometry().setFromPoints(linePoints)
  objects.priceRibbon.geometry.dispose()
  objects.priceRibbon.geometry = ribbonGeometryFor(objects.THREE, linePoints)
  objects.priceSurface.geometry.dispose()
  objects.priceSurface.geometry = pricePressureSurfaceGeometryFor(objects.THREE, linePoints, 4.75)

  const wavePoints = smoothPricePoints(objects.THREE, rows, minPrice, priceRange, barSpan, 1.22, 0.04, 0.78)
  objects.wave.geometry.dispose()
  objects.wave.geometry = new objects.THREE.BufferGeometry().setFromPoints(wavePoints)

  const socPoints = smoothSocPoints(objects.THREE, rows, barSpan, numberValue(props.capacityMwh))
  objects.socRibbon.geometry.dispose()
  objects.socRibbon.geometry = stateRibbonGeometryFor(objects.THREE, socPoints, 0.34)
}

function resizeScene() {
  const objects = sceneObjects.value
  if (!objects || !rootEl.value) {
    return
  }
  const width = rootEl.value.clientWidth
  const height = rootEl.value.clientHeight
  objects.camera.aspect = width / height
  frameCamera(objects)
  objects.camera.updateProjectionMatrix()
  objects.renderer.setSize(width, height, false)
}

function frameCamera(objects: ThreeSceneObjects) {
  if (!rootEl.value) {
    return
  }
  const aspect = rootEl.value.clientWidth / Math.max(1, rootEl.value.clientHeight)
  objects.camera.up.set(0, 1, 0)
  objects.camera.zoom = clamp(1 + interactionZoom, 0.88, 1.18)
  if (viewMode.value === 'plan') {
    objects.camera.fov = aspect < 1.18 ? 48 : 38
    objects.camera.position.set(0, aspect < 1.18 ? 8.9 : 8.6, aspect < 1.18 ? 13.2 : 8.4)
    objects.camera.lookAt(0, -0.08, 0)
    objects.camera.updateProjectionMatrix()
    return
  }
  if (aspect < 1.18) {
    objects.camera.fov = 44
    objects.camera.position.set(0.22, 6.05, 15.8)
    objects.camera.lookAt(0, 0.28, -0.12)
    objects.camera.updateProjectionMatrix()
    return
  }
  objects.camera.fov = 35
  objects.camera.position.set(0.36, 5.18, 11.45)
  objects.camera.lookAt(0, 0.38, -0.16)
  objects.camera.updateProjectionMatrix()
}

function startAnimation() {
  const objects = sceneObjects.value
  if (!objects || objects.frameId !== null) {
    return
  }
    const animate = () => {
    const currentObjects = sceneObjects.value
    if (!currentObjects) {
      return
    }
    const elapsed = (performance.now() - currentObjects.startedAt) / 1000
    currentObjects.scene.rotation.y = viewMode.value === 'plan' ? 0 : interactionRotation + Math.sin(elapsed * 0.18) * 0.026
    currentObjects.wave.position.y = Math.sin(elapsed * 1.2) * 0.08
    currentObjects.priceLine.position.y = Math.sin(elapsed * 0.8) * 0.035
    currentObjects.priceGlow.position.y = currentObjects.priceLine.position.y
    currentObjects.priceRibbon.position.y = Math.sin(elapsed * 0.8) * 0.035
    currentObjects.priceSurface.position.y = Math.sin(elapsed * 0.8) * 0.026
    currentObjects.socRibbon.position.y = Math.sin(elapsed * 0.95) * 0.024
    const beamMaterial = currentObjects.selectionBeam.material as import('three').MeshBasicMaterial
    beamMaterial.opacity = 0.062 + Math.sin(elapsed * 1.15) * 0.018
    currentObjects.priceWall.position.y = Math.sin(elapsed * 0.64) * 0.018
    currentObjects.priceCurtains.forEach((curtain) => {
      const curtainMaterial = curtain.material as import('three').MeshBasicMaterial
      const baseOpacity = Number(curtain.userData.baseOpacity || 0.06)
      curtainMaterial.opacity = Math.max(0.025, baseOpacity + Math.sin(elapsed * 1.1 + Number(curtain.userData.phase || 0)) * 0.012)
    })
    const needlePulse = 1 + Math.sin(elapsed * 1.7) * 0.08
    currentObjects.selectionNeedle.scale.x = needlePulse
    currentObjects.selectionNeedle.scale.z = needlePulse
    currentObjects.selectionCap.scale.setScalar(1 + Math.sin(elapsed * 1.7) * 0.11)
    currentObjects.bars.forEach((bar) => {
      const baseScaleY = Number(bar.userData.baseScaleY || bar.scale.y)
      const power = Number(bar.userData.power || 0)
      const isHold = Boolean(bar.userData.isHold)
      const pulse = 1 + Math.sin(elapsed * 1.35 + Number(bar.userData.phase || 0)) * 0.018
      const scaleY = baseScaleY * pulse
      bar.scale.y = scaleY
      bar.position.y = (isHold || power >= 0) ? scaleY / 2 : -scaleY * 0.42
    })
    currentObjects.renderer.render(currentObjects.scene, currentObjects.camera)
    currentObjects.frameId = window.requestAnimationFrame(animate)
  }
  objects.frameId = window.requestAnimationFrame(animate)
}

function stopAnimation() {
  const objects = sceneObjects.value
  if (!objects || objects.frameId === null) {
    return
  }
  window.cancelAnimationFrame(objects.frameId)
  objects.frameId = null
}

function resumeAnimation() {
  const objects = sceneObjects.value
  if (!objects || document.hidden || !isSceneNearViewport.value) {
    return
  }
  objects.startedAt = performance.now()
  startAnimation()
}

function handleVisibilityChange() {
  const objects = sceneObjects.value
  if (!objects) {
    return
  }
  if (document.hidden) {
    stopAnimation()
    return
  }
  resumeAnimation()
}

function attachVisibilityObserver() {
  if (!rootEl.value || !('IntersectionObserver' in window)) {
    return
  }
  intersectionObserver?.disconnect()
  intersectionObserver = new IntersectionObserver((entries) => {
    const [entry] = entries
    isSceneNearViewport.value = Boolean(entry?.isIntersecting)
    if (isSceneNearViewport.value) {
      resumeAnimation()
      return
    }
    stopAnimation()
  }, { rootMargin: '220px 0px' })
  intersectionObserver.observe(rootEl.value)
}

function destroyScene() {
  const objects = sceneObjects.value
  if (!objects) {
    return
  }
  stopAnimation()
  intersectionObserver?.disconnect()
  intersectionObserver = null
  window.removeEventListener('resize', resizeScene)
  document.removeEventListener('visibilitychange', handleVisibilityChange)
  objects.renderer.domElement.remove()
  objects.bars.forEach((bar) => {
    bar.geometry.dispose()
    if (Array.isArray(bar.material)) {
      bar.material.forEach(material => material.dispose())
    } else {
      bar.material.dispose()
    }
  })
  objects.floorStrips.forEach((strip) => {
    strip.geometry.dispose()
    if (Array.isArray(strip.material)) {
      strip.material.forEach(material => material.dispose())
    } else {
      strip.material.dispose()
    }
  })
  objects.priceCurtains.forEach((curtain) => {
    curtain.geometry.dispose()
    if (Array.isArray(curtain.material)) {
      curtain.material.forEach(material => material.dispose())
    } else {
      curtain.material.dispose()
    }
  })
  objects.priceLine.geometry.dispose()
  objects.priceGlow.geometry.dispose()
  objects.priceRibbon.geometry.dispose()
  objects.priceSurface.geometry.dispose()
  objects.socRibbon.geometry.dispose()
  objects.fieldFrame.geometry.dispose()
  objects.priceWall.geometry.dispose()
  objects.wave.geometry.dispose()
  objects.basePlane.geometry.dispose()
  objects.selectionMarker.geometry.dispose()
  objects.selectionBeam.geometry.dispose()
  objects.selectionNeedle.geometry.dispose()
  objects.selectionCap.geometry.dispose()
  disposeMaterial(objects.priceLine.material)
  disposeMaterial(objects.priceGlow.material)
  disposeMaterial(objects.priceRibbon.material)
  disposeMaterial(objects.priceSurface.material)
  disposeMaterial(objects.socRibbon.material)
  disposeMaterial(objects.fieldFrame.material)
  disposeMaterial(objects.priceWall.material)
  disposeMaterial(objects.basePlane.material)
  disposeMaterial(objects.selectionMarker.material)
  disposeMaterial(objects.selectionBeam.material)
  disposeMaterial(objects.selectionNeedle.material)
  disposeMaterial(objects.selectionCap.material)
  objects.renderer.dispose()
  sceneObjects.value = null
}

function fieldFrameGeometryFor(THREE: ThreeModule, width: number, depth: number): import('three').BufferGeometry {
  const halfWidth = width / 2
  const halfDepth = depth / 2
  const positions: number[] = []
  const addLine = (startX: number, startY: number, startZ: number, endX: number, endY: number, endZ: number) => {
    positions.push(startX, startY, startZ, endX, endY, endZ)
  }
  for (let index = 0; index <= 24; index += 1) {
    const x = -halfWidth + (index / 24) * width
    addLine(x, 0, -halfDepth, x, 0, halfDepth)
  }
  for (let index = 0; index <= 8; index += 1) {
    const z = -halfDepth + (index / 8) * depth
    addLine(-halfWidth, 0, z, halfWidth, 0, z)
  }
  addLine(-halfWidth, 0, -halfDepth, -halfWidth, 1.8, -halfDepth)
  addLine(halfWidth, 0, -halfDepth, halfWidth, 2.65, -halfDepth)
  addLine(halfWidth, 0, halfDepth, halfWidth, 2.2, halfDepth)
  addLine(-halfWidth, 0, halfDepth, -halfWidth, 1.5, halfDepth)
  addLine(-halfWidth, 0, -halfDepth, halfWidth, 0, -halfDepth)
  addLine(-halfWidth, 0, halfDepth, halfWidth, 0, halfDepth)
  addLine(-halfWidth, 0, -halfDepth, -halfWidth, 0, halfDepth)
  addLine(halfWidth, 0, -halfDepth, halfWidth, 0, halfDepth)
  const geometry = new THREE.BufferGeometry()
  geometry.setAttribute('position', new THREE.BufferAttribute(new Float32Array(positions), 3))
  return geometry
}

function priceWallGeometryFor(THREE: ThreeModule, width: number, height: number, levels: number): import('three').BufferGeometry {
  const halfWidth = width / 2
  const positions: number[] = []
  const addLine = (startX: number, startY: number, endX: number, endY: number) => {
    positions.push(startX, startY, 0, endX, endY, 0)
  }
  for (let index = 0; index <= 24; index += 2) {
    const x = -halfWidth + (index / 24) * width
    addLine(x, 0, x, height)
  }
  for (let level = 0; level <= levels; level += 1) {
    const y = (level / levels) * height
    addLine(-halfWidth, y, halfWidth, y)
  }
  const geometry = new THREE.BufferGeometry()
  geometry.setAttribute('position', new THREE.BufferAttribute(new Float32Array(positions), 3))
  return geometry
}

function ribbonGeometryFor(THREE: ThreeModule, linePoints: import('three').Vector3[]): import('three').BufferGeometry {
  const vertices: number[] = []
  const indices: number[] = []
  linePoints.forEach((point, index) => {
    const floorY = 0.02 + Math.sin(index * 0.18) * 0.018
    vertices.push(point.x, point.y, point.z - 0.03)
    vertices.push(point.x, floorY, point.z - 0.03)
  })
  for (let index = 0; index < linePoints.length - 1; index += 1) {
    const base = index * 2
    indices.push(base, base + 1, base + 2)
    indices.push(base + 1, base + 3, base + 2)
  }
  const geometry = new THREE.BufferGeometry()
  geometry.setAttribute('position', new THREE.BufferAttribute(new Float32Array(vertices), 3))
  geometry.setIndex(indices)
  geometry.computeVertexNormals()
  return geometry
}

function pricePressureSurfaceGeometryFor(
  THREE: ThreeModule,
  linePoints: import('three').Vector3[],
  width = 4.75
): import('three').BufferGeometry {
  const vertices: number[] = []
  const indices: number[] = []
  const halfWidth = width / 2
  linePoints.forEach((point, index) => {
    const shoulder = Math.sin(index * 0.16) * 0.055
    const surfaceY = Math.max(0.04, point.y - 0.2 + shoulder)
    vertices.push(point.x, surfaceY, -halfWidth)
    vertices.push(point.x, Math.max(0.02, surfaceY - 0.22), 0)
    vertices.push(point.x, surfaceY + shoulder * 0.5, halfWidth)
  })
  for (let index = 0; index < linePoints.length - 1; index += 1) {
    const base = index * 3
    indices.push(base, base + 1, base + 3)
    indices.push(base + 1, base + 4, base + 3)
    indices.push(base + 1, base + 2, base + 4)
    indices.push(base + 2, base + 5, base + 4)
  }
  const geometry = new THREE.BufferGeometry()
  geometry.setAttribute('position', new THREE.BufferAttribute(new Float32Array(vertices), 3))
  geometry.setIndex(indices)
  geometry.computeVertexNormals()
  return geometry
}

function stateRibbonGeometryFor(THREE: ThreeModule, linePoints: import('three').Vector3[], width = 0.32): import('three').BufferGeometry {
  const vertices: number[] = []
  const indices: number[] = []
  linePoints.forEach((point, index) => {
    const shoulder = Math.sin(index * 0.14) * 0.012
    vertices.push(point.x, point.y + shoulder, point.z - width)
    vertices.push(point.x, Math.max(0.08, point.y - 0.16), point.z + width)
  })
  for (let index = 0; index < linePoints.length - 1; index += 1) {
    const base = index * 2
    indices.push(base, base + 1, base + 2)
    indices.push(base + 1, base + 3, base + 2)
  }
  const geometry = new THREE.BufferGeometry()
  geometry.setAttribute('position', new THREE.BufferAttribute(new Float32Array(vertices), 3))
  geometry.setIndex(indices)
  geometry.computeVertexNormals()
  return geometry
}

function smoothPricePoints(
  THREE: ThreeModule,
  rows: DispatchPoint[],
  minPrice: number,
  priceRange: number,
  barSpan: number,
  z: number,
  baseY: number,
  ySpan: number
): import('three').Vector3[] {
  const rawPoints = rows.map((point, index) => {
    const x = -barSpan / 2 + (index / Math.max(1, rows.length - 1)) * barSpan
    const y = baseY + ((numberValue(point.price_uah_mwh) - minPrice) / priceRange) * ySpan
    return new THREE.Vector3(x, y, z)
  })
  if (rawPoints.length < 3) {
    return rawPoints
  }
  const curve = new THREE.CatmullRomCurve3(rawPoints, false, 'centripetal', 0.32)
  return curve.getPoints(Math.max(72, rawPoints.length * 5))
}

function smoothSocPoints(
  THREE: ThreeModule,
  rows: DispatchPoint[],
  barSpan: number,
  capacityMwh: number
): import('three').Vector3[] {
  const socValues = rows.map(point => numberValue(point.soc_after_mwh)).filter(value => Number.isFinite(value))
  const minSoc = Math.min(...socValues, 0)
  const maxSoc = Math.max(...socValues, capacityMwh > 0 ? capacityMwh : 1)
  const range = Math.max(0.001, capacityMwh > 0 ? capacityMwh : maxSoc - minSoc)
  const rawPoints = rows.map((point, index) => {
    const x = -barSpan / 2 + (index / Math.max(1, rows.length - 1)) * barSpan
    const soc = numberValue(point.soc_after_mwh)
    const normalized = capacityMwh > 0 ? soc / range : (soc - minSoc) / range
    const y = 0.12 + Math.min(1, Math.max(0, normalized)) * 1.34
    return new THREE.Vector3(x, y, 0.92)
  })
  if (rawPoints.length < 3) {
    return rawPoints
  }
  const curve = new THREE.CatmullRomCurve3(rawPoints, false, 'centripetal', 0.28)
  return curve.getPoints(Math.max(72, rawPoints.length * 5))
}

function disposeMaterial(material: import('three').Material | import('three').Material[]) {
  if (Array.isArray(material)) {
    material.forEach(entry => entry.dispose())
    return
  }
  material.dispose()
}

function numberValue(value: unknown): number {
  const numeric = Number(value || 0)
  return Number.isFinite(numeric) ? numeric : 0
}

function bestDefaultIndex(rows: DispatchPoint[]): number {
  if (rows.length === 0) {
    return 0
  }
  let bestValueIndex = 0
  let bestValue = -Infinity
  rows.forEach((row, index) => {
    const value = numberValue(row.net_value_uah)
    if (value > bestValue) {
      bestValue = value
      bestValueIndex = index
    }
  })
  if (bestValue > 0.0001) {
    return bestValueIndex
  }
  let bestIndex = 0
  let bestMagnitude = -1
  rows.forEach((row, index) => {
    const magnitude = Math.abs(numberValue(row.net_power_mw))
    if (magnitude > bestMagnitude) {
      bestMagnitude = magnitude
      bestIndex = index
    }
  })
  if (bestMagnitude > 0.0001) {
    return bestIndex
  }
  rows.forEach((row, index) => {
    const price = numberValue(row.price_uah_mwh)
    if (price > bestMagnitude) {
      bestMagnitude = price
      bestIndex = index
    }
  })
  return bestIndex
}

function selectHour(index: number) {
  selectedIndex.value = index
}

function selectHourFromKeyboard(event: KeyboardEvent, index: number) {
  const rows = sceneRows.value
  if (rows.length === 0) {
    return
  }
  let nextIndex = index
  if (event.key === 'ArrowRight' || event.key === 'ArrowDown') {
    nextIndex = Math.min(rows.length - 1, index + 1)
  } else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') {
    nextIndex = Math.max(0, index - 1)
  } else if (event.key === 'Home') {
    nextIndex = 0
  } else if (event.key === 'End') {
    nextIndex = rows.length - 1
  } else if (event.key === 'Enter' || event.key === ' ') {
    nextIndex = index
  } else {
    return
  }
  event.preventDefault()
  selectedIndex.value = nextIndex
  const target = event.currentTarget
  if (target instanceof HTMLElement) {
    const buttons = Array.from(target.parentElement?.querySelectorAll<HTMLButtonElement>('.bess-field__hour') || [])
    buttons[nextIndex]?.focus()
  }
}

function selectSignal(index: number) {
  selectHour(index)
}

function setViewMode(mode: ViewMode) {
  viewMode.value = mode
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value))
}

function markerPositionStyle(index: number, tone: string) {
  const denominator = Math.max(1, sceneRows.value.length - 1)
  const projectedX = 10 + (index / denominator) * 78
  const projectedY = tone === 'peak' ? 30 : tone === 'valley' || tone === 'charge' ? 64 : 48
  return {
    '--bess-marker-x': `${projectedX}%`,
    '--bess-marker-y': `${projectedY}%`
  }
}

function hourLabel(value: string | undefined): string {
  return value ? value.slice(11, 16) : '--:--'
}

function actionFor(point: DispatchPoint | null) {
  const power = numberValue(point?.net_power_mw)
  if (power > 0.0001) {
    return { label: 'Discharge', className: 'discharge' }
  }
  if (power < -0.0001) {
    return { label: 'Charge', className: 'charge' }
  }
  return { label: 'Hold', className: 'hold' }
}

function selectNearestHourFromPointer(event: PointerEvent) {
  const target = event.target
  if (target instanceof Element && target.closest('button, a')) {
    return
  }
  const current = event.currentTarget
  if (!(current instanceof HTMLElement)) {
    return
  }
  const rows = sceneRows.value
  if (rows.length === 0) {
    return
  }
  const rect = current.getBoundingClientRect()
  const rawRatio = (event.clientX - rect.left) / Math.max(1, rect.width)
  const fieldRatio = Math.min(1, Math.max(0, (rawRatio - 0.1) / 0.78))
  selectedIndex.value = Math.round(fieldRatio * (rows.length - 1))
}

function handleFieldPointerDown(event: PointerEvent) {
  selectNearestHourFromPointer(event)
  const target = event.target
  if (target instanceof Element && target.closest('button, a')) {
    return
  }
  const current = event.currentTarget
  if (!(current instanceof HTMLElement) || viewMode.value === 'plan') {
    return
  }
  activePointerId = event.pointerId
  pointerStartX = event.clientX
  pointerStartY = event.clientY
  pointerStartRotation = interactionRotation
  current.setPointerCapture?.(event.pointerId)
}

function handleFieldPointerMove(event: PointerEvent) {
  if (activePointerId !== event.pointerId || viewMode.value === 'plan') {
    return
  }
  const deltaX = event.clientX - pointerStartX
  const deltaY = event.clientY - pointerStartY
  if (Math.abs(deltaX) < 4 && Math.abs(deltaY) < 4) {
    return
  }
  interactionRotation = clamp(pointerStartRotation + deltaX * 0.0028, -0.28, 0.28)
  const objects = sceneObjects.value
  if (objects) {
    objects.renderer.render(objects.scene, objects.camera)
  }
}

function handleFieldPointerEnd(event: PointerEvent) {
  if (activePointerId !== event.pointerId) {
    return
  }
  const current = event.currentTarget
  if (current instanceof HTMLElement) {
    current.releasePointerCapture?.(event.pointerId)
  }
  activePointerId = null
}

function handleFieldWheel(event: WheelEvent) {
  if (viewMode.value === 'plan' || !sceneObjects.value) {
    return
  }
  event.preventDefault()
  interactionZoom = clamp(interactionZoom - event.deltaY * 0.0008, -0.12, 0.18)
  frameCamera(sceneObjects.value)
  sceneObjects.value.renderer.render(sceneObjects.value.scene, sceneObjects.value.camera)
}

function formatNumber(value: unknown, digits = 0): string {
  const numeric = Number(value || 0)
  const fixed = Number.isFinite(numeric) ? numeric.toFixed(digits) : (0).toFixed(digits)
  const [integerPart = '0', decimalPart] = fixed.split('.')
  const sign = integerPart.startsWith('-') ? '-' : ''
  const unsignedInteger = integerPart.replace('-', '')
  const groupedInteger = unsignedInteger.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  return decimalPart ? `${sign}${groupedInteger}.${decimalPart}` : `${sign}${groupedInteger}`
}

function formatMw(value: unknown): string {
  return `${formatNumber(value, 3)} MW`
}

function formatMwh(value: unknown): string {
  return `${formatNumber(value, 3)} MWh`
}

function formatUah(value: unknown): string {
  return `${formatNumber(value, 0)} UAH`
}
</script>

<template>
  <div
    class="bess-field"
    :class="`bess-field--${viewMode}`"
    :style="selectedPositionStyle"
    @pointerdown="handleFieldPointerDown"
    @pointermove="handleFieldPointerMove"
    @pointerup="handleFieldPointerEnd"
    @pointercancel="handleFieldPointerEnd"
    @wheel="handleFieldWheel"
  >
    <div class="bess-field__canvas" ref="rootEl" aria-hidden="true" />
    <div class="bess-field__title">
      <strong>BESS Dispatch Field</strong>
      <span>WebGL / Three.js</span>
    </div>
    <div class="bess-field__mode-toggle" role="group" aria-label="Dispatch field view mode">
      <button
        v-for="option in viewModeOptions"
        :key="option.id"
        type="button"
        :aria-pressed="viewMode === option.id"
        :title="option.description"
        :class="{ 'is-active': viewMode === option.id }"
        @click="setViewMode(option.id)"
      >
        {{ option.label }}
      </button>
    </div>
    <div class="bess-field__legend" aria-hidden="true">
      <span><i class="bess-field__key bess-field__key--discharge" /> Discharge</span>
      <span><i class="bess-field__key bess-field__key--charge" /> Charge</span>
      <span><i class="bess-field__key bess-field__key--hold" /> SOC state</span>
      <span><i class="bess-field__line" /> DAM price ribbon</span>
    </div>
    <div class="bess-field__axis-label bess-field__axis-label--power">Power (MW)</div>
    <div class="bess-field__axis-label bess-field__axis-label--price">Price (UAH/MWh)</div>
    <div class="bess-field__scale bess-field__scale--power" aria-hidden="true">
      <span v-for="(tick, index) in powerScaleTicks" :key="`power-${index}-${tick.label}`" :style="{ top: tick.position }">
        <i />
        {{ tick.label }} MW
      </span>
    </div>
    <div class="bess-field__scale bess-field__scale--price" aria-hidden="true">
      <span v-for="(tick, index) in priceScaleTicks" :key="`price-${index}-${tick.label}`" :style="{ top: tick.position }">
        <i />
        {{ tick.label }}
      </span>
    </div>
    <div
      v-if="selectedPoint"
      class="bess-field__crosshair"
      :class="`bess-field__crosshair--${selectedAction.className}`"
      :style="selectedPositionStyle"
      aria-hidden="true"
    >
      <span />
    </div>
    <aside v-if="selectedPoint" class="bess-field__annotation" aria-live="polite">
      <div>
        <span>Selected hour</span>
        <strong>{{ hourLabel(selectedPoint.timestamp) }}</strong>
      </div>
      <dl>
        <div>
          <dt>Action</dt>
          <dd :class="`is-${selectedAction.className}`">{{ selectedAction.label }}</dd>
        </div>
        <div>
          <dt>Price</dt>
          <dd>{{ formatNumber(selectedPoint.price_uah_mwh, 0) }} UAH/MWh</dd>
        </div>
        <div>
          <dt>Power</dt>
          <dd>{{ formatMw(selectedPoint.net_power_mw) }}</dd>
        </div>
        <div>
          <dt>SoC</dt>
          <dd>{{ selectedSocPercent === null ? formatMwh(selectedPoint.soc_after_mwh) : `${formatNumber(selectedSocPercent, 1)}%` }}</dd>
        </div>
        <div>
          <dt>Net value</dt>
          <dd>{{ formatUah(selectedPoint.net_value_uah) }}</dd>
        </div>
      </dl>
    </aside>
    <div v-if="signalMarkers.length > 0" class="bess-field__signal-stack" aria-label="Dispatch field signal shortcuts">
      <button
        v-for="marker in signalMarkers"
        :key="marker.key"
        type="button"
        :class="['bess-field__signal', `bess-field__signal--${marker.tone}`, { 'is-selected': selectedIndex === marker.index }]"
        :style="markerPositionStyle(marker.index, marker.tone)"
        :aria-pressed="selectedIndex === marker.index"
        :aria-label="`${marker.label}: ${marker.hour}, ${marker.value}`"
        :data-label="`${marker.label} ${marker.hour}`"
        :data-value="marker.value"
        @pointerenter="selectSignal(marker.index)"
        @focus="selectSignal(marker.index)"
        @click="selectSignal(marker.index)"
      >
        <span>{{ marker.label }}</span>
        <strong>{{ marker.hour }}</strong>
        <em>{{ marker.value }}</em>
      </button>
    </div>
    <div class="bess-field__hud">
      <div>
        <span>Dispatch field</span>
        <strong>{{ presetLabel || 'Battery preset pending' }}</strong>
      </div>
      <div>
        <span>Source</span>
        <strong>{{ sourceStatus || 'pending' }}</strong>
      </div>
    </div>
    <div class="bess-field__interaction-hints" aria-hidden="true">
      <span>Drag to rotate</span>
      <span>Scroll to zoom</span>
      <span>Click hour to inspect</span>
    </div>
    <div v-if="fieldReceiptRows.length > 0" class="bess-field__analysis-tape" aria-label="Dispatch field quick receipt">
      <div v-for="row in fieldReceiptRows" :key="row.label">
        <span>{{ row.label }}</span>
        <strong>{{ row.value }}</strong>
      </div>
    </div>
    <div v-if="sceneRows.length > 0" class="bess-field__hour-rail" role="listbox" aria-label="Select dispatch hour">
      <button
        v-for="(row, index) in sceneRows"
        :key="`${row.timestamp || index}-${index}`"
        type="button"
        :class="[
          'bess-field__hour',
          `bess-field__hour--${actionFor(row).className}`,
          { 'is-selected': selectedIndex === index }
        ]"
        role="option"
        :aria-selected="selectedIndex === index"
        :tabindex="selectedIndex === index ? 0 : -1"
        :aria-label="`${hourLabel(row.timestamp)} ${actionFor(row).label}, ${formatNumber(row.price_uah_mwh, 0)} UAH per MWh, ${formatMw(row.net_power_mw)}`"
        @pointerenter="selectHour(index)"
        @focus="selectHour(index)"
        @click="selectHour(index)"
        @keydown="selectHourFromKeyboard($event, index)"
      >
        <span>{{ hourLabel(row.timestamp).slice(0, 2) }}</span>
      </button>
    </div>
    <div v-if="reducedMotion || webglFailed || !hasSceneData" class="bess-field__fallback">
      <svg
        class="bess-field__fallback-plate"
        :viewBox="`0 0 ${fallbackPlate.width} ${fallbackPlate.height}`"
        role="img"
        aria-label="Static dispatch field with price line and charge-discharge bars"
      >
        <defs>
          <linearGradient id="bessFallbackRibbon" x1="0" x2="1" y1="0" y2="0">
            <stop offset="0%" stop-color="#8bd9ef" stop-opacity="0.1" />
            <stop offset="52%" stop-color="#f2ca62" stop-opacity="0.32" />
            <stop offset="100%" stop-color="#78c978" stop-opacity="0.16" />
          </linearGradient>
        </defs>
        <rect x="18" y="20" width="644" height="230" rx="6" fill="rgba(245, 252, 255, 0.72)" />
        <path d="M32 72 C140 35 248 88 356 58 S570 82 648 44 L648 196 L32 196 Z" fill="url(#bessFallbackRibbon)" />
        <line x1="32" :y1="fallbackPlate.baseline" x2="648" :y2="fallbackPlate.baseline" stroke="#9fcde0" stroke-dasharray="6 8" />
        <rect
          v-for="(bar, index) in fallbackPlate.bars"
          :key="index"
          :x="bar.x"
          :y="bar.y"
          width="10"
          :height="bar.height"
          rx="3"
          :fill="bar.tone === 'discharge' ? '#78c978' : '#f0bf4f'"
          fill-opacity="0.88"
        />
        <polyline
          :points="fallbackPlate.line"
          fill="none"
          stroke="#178cc4"
          stroke-width="3"
          stroke-linecap="round"
          stroke-linejoin="round"
        />
      </svg>
      <div class="bess-field__fallback-copy">
        <strong>Static dispatch plate</strong>
        <span>Animation is paused; the same source-backed schedule remains visible.</span>
      </div>
    </div>
  </div>
</template>

<style scoped>
.bess-field {
  container-name: bess-field;
  container-type: inline-size;
  position: relative;
  min-height: 460px;
  overflow: hidden;
  overflow: clip;
  border: 1px solid rgba(64, 129, 166, 0.13);
  border-radius: 8px;
  background:
    linear-gradient(90deg, rgba(70, 136, 175, 0.05) 1px, transparent 1px),
    linear-gradient(0deg, rgba(70, 136, 175, 0.046) 1px, transparent 1px),
    linear-gradient(135deg, rgba(149, 215, 238, 0.14) 0 18%, transparent 18% 100%),
    linear-gradient(145deg, rgba(251, 254, 255, 0.84), rgba(232, 247, 252, 0.5));
  background-size: 32px 32px, 32px 32px, auto, auto;
  font-family: var(--bess-font-detail, "Noto Sans", sans-serif);
  font-size-adjust: from-font;
  font-optical-sizing: auto;
  font-synthesis-style: none;
  box-shadow:
    0 12px 24px rgba(41, 111, 151, 0.045),
    inset 0 1px 0 rgba(255, 255, 255, 0.82);
  contain: layout style paint;
  touch-action: pan-y;
}

.bess-field--perspective {
  cursor: grab;
}

.bess-field--perspective:active {
  cursor: grabbing;
}

.bess-field::before,
.bess-field::after {
  content: '';
  position: absolute;
  inset: 0;
  pointer-events: none;
}

.bess-field::before {
  background:
    linear-gradient(115deg, rgba(255, 255, 255, 0.5) 0 18%, transparent 18% 100%),
    linear-gradient(90deg, transparent 0 48%, rgba(12, 126, 179, 0.035) 48% 49%, transparent 49% 100%);
}

.bess-field::after {
  width: 30%;
  background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.28), transparent);
  animation: bessFieldScan 10s ease-in-out infinite;
  transform: translateX(-120%);
}

.bess-field__canvas {
  position: absolute;
  inset: 0;
  z-index: 1;
}

.bess-field__canvas :deep(canvas) {
  display: block;
  width: 100%;
  height: 100%;
  filter: saturate(1.1) contrast(1.03);
}

.bess-field__title {
  position: absolute;
  z-index: 6;
  top: 13px;
  left: 14px;
  display: flex;
  align-items: center;
  gap: 8px;
  color: #09254a;
  font-family: var(--bess-font-ui, "Noto Sans", sans-serif);
  pointer-events: none;
}

.bess-field__title strong {
  font-size: 16px;
  font-weight: 800;
  line-height: 1;
  text-transform: uppercase;
}

.bess-field__title span {
  border: 1px solid rgba(12, 126, 179, 0.2);
  border-radius: 4px;
  padding: 2px 5px;
  color: #075fd1;
  background: rgba(255, 255, 255, 0.62);
  font-size: 9px;
  font-weight: 800;
  line-height: 1;
}

.bess-field__mode-toggle {
  position: absolute;
  z-index: 7;
  top: 12px;
  right: 14px;
  display: inline-grid;
  grid-template-columns: repeat(2, minmax(38px, 1fr));
  gap: 2px;
  border: 1px solid rgba(12, 126, 179, 0.18);
  border-radius: 6px;
  padding: 2px;
  background: rgba(250, 254, 255, 0.72);
  box-shadow: 0 8px 18px rgba(32, 103, 145, 0.07);
  backdrop-filter: blur(10px);
}

.bess-field__mode-toggle button {
  min-width: 0;
  min-height: 22px;
  border: 0;
  border-radius: 4px;
  padding: 0 8px;
  color: #23516d;
  background: transparent;
  font-family: var(--bess-font-ui, "Noto Sans", sans-serif);
  font-size: 10px;
  font-weight: 800;
  line-height: 1;
  cursor: pointer;
}

.bess-field__mode-toggle button.is-active {
  color: #ffffff;
  background: #0c7eb3;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.32);
}

.bess-field__mode-toggle button:focus-visible {
  outline: 2px solid #0c7eb3;
  outline-offset: 2px;
}

.bess-field__legend {
  position: absolute;
  z-index: 5;
  left: 14px;
  top: 43px;
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
  max-width: min(520px, calc(100% - 28px));
  pointer-events: none;
}

.bess-field__legend span {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  border: 1px solid rgba(64, 129, 166, 0.11);
  border-radius: 5px;
  padding: 5px 7px;
  color: #315c75;
  background: rgba(255, 255, 255, 0.52);
  backdrop-filter: blur(8px);
  font-size: 9px;
  font-weight: 850;
  line-height: 1;
  text-transform: uppercase;
}

.bess-field__key,
.bess-field__line {
  display: inline-block;
  width: 14px;
  height: 8px;
  border-radius: 2px;
  background: #55bca4;
}

.bess-field__key--charge {
  background: #efbf53;
}

.bess-field__key--hold {
  background: #8fd7ec;
}

.bess-field__line {
  height: 3px;
  border-radius: 999px;
  background: #178cc4;
}

.bess-field__axis-label {
  position: absolute;
  z-index: 4;
  color: rgba(30, 82, 113, 0.78);
  font-family: var(--bess-font-data, "Anonymous Pro", "Noto Sans Mono", "Noto Sans", monospace);
  font-size: 10px;
  font-weight: 700;
  line-height: 1;
  letter-spacing: 0;
  pointer-events: none;
  text-transform: uppercase;
}

.bess-field__axis-label--power {
  top: 48%;
  left: 18px;
  transform: rotate(-90deg) translateX(-50%);
  transform-origin: 0 0;
}

.bess-field__axis-label--price {
  top: 190px;
  right: 16px;
  writing-mode: vertical-rl;
}

.bess-field__scale {
  position: absolute;
  z-index: 4;
  pointer-events: none;
}

.bess-field__scale span {
  position: absolute;
  display: inline-flex;
  align-items: center;
  gap: 5px;
  color: rgba(30, 82, 113, 0.7);
  font-family: var(--bess-font-data, "Anonymous Pro", "Noto Sans Mono", "Noto Sans", monospace);
  font-size: 9px;
  font-weight: 700;
  line-height: 1;
  white-space: nowrap;
}

.bess-field__scale i {
  display: block;
  width: 18px;
  border-top: 1px solid rgba(23, 140, 196, 0.32);
}

.bess-field__scale--power {
  top: 132px;
  bottom: 118px;
  left: 34px;
  width: 90px;
}

.bess-field__scale--power span {
  left: 0;
}

.bess-field__scale--price {
  top: 210px;
  right: 18px;
  bottom: 84px;
  width: 78px;
}

.bess-field__scale--price span {
  right: 0;
  flex-direction: row-reverse;
  text-align: right;
}

.bess-field__crosshair {
  position: absolute;
  z-index: 4;
  top: 82px;
  bottom: 84px;
  left: var(--bess-selected-x);
  width: 1px;
  pointer-events: none;
}

.bess-field__crosshair::before {
  content: '';
  position: absolute;
  inset: 0;
  border-left: 1px dashed rgba(10, 119, 168, 0.34);
}

.bess-field__crosshair span {
  position: absolute;
  bottom: 28px;
  left: 50%;
  width: 34px;
  height: 34px;
  border: 1px solid rgba(10, 119, 168, 0.42);
  border-radius: 999px;
  transform: translateX(-50%);
  background:
    radial-gradient(circle, rgba(10, 119, 168, 0.22) 0 2px, transparent 3px),
    radial-gradient(circle, rgba(10, 119, 168, 0.16), transparent 62%);
  box-shadow:
    0 0 0 8px rgba(10, 119, 168, 0.08),
    0 16px 26px rgba(10, 119, 168, 0.14);
}

.bess-field__crosshair span::before,
.bess-field__crosshair span::after {
  content: '';
  position: absolute;
  inset: -9px;
  border: 1px solid rgba(10, 119, 168, 0.34);
  border-radius: inherit;
  animation: bessFieldRipple 2.6s ease-out infinite;
}

.bess-field__crosshair span::after {
  inset: -17px;
  border-color: rgba(10, 119, 168, 0.2);
  animation-delay: 0.72s;
}

.bess-field__crosshair--charge::before {
  border-left-color: rgba(225, 179, 77, 0.42);
}

.bess-field__crosshair--charge span {
  border-color: rgba(225, 179, 77, 0.58);
  background:
    radial-gradient(circle, rgba(225, 179, 77, 0.28) 0 2px, transparent 3px),
    radial-gradient(circle, rgba(225, 179, 77, 0.2), transparent 62%);
  box-shadow:
    0 0 0 8px rgba(225, 179, 77, 0.1),
    0 16px 26px rgba(122, 82, 14, 0.12);
}

.bess-field__crosshair--discharge::before {
  border-left-color: rgba(27, 157, 126, 0.42);
}

.bess-field__crosshair--discharge span {
  border-color: rgba(27, 157, 126, 0.55);
  background:
    radial-gradient(circle, rgba(27, 157, 126, 0.26) 0 2px, transparent 3px),
    radial-gradient(circle, rgba(27, 157, 126, 0.18), transparent 62%);
}

.bess-field__annotation {
  position: absolute;
  z-index: 5;
  top: 76px;
  right: 16px;
  width: min(208px, calc(100% - 32px));
  border: 1px solid rgba(64, 129, 166, 0.13);
  border-radius: 8px;
  padding: 8px 9px;
  color: #123552;
  background: rgba(255, 255, 255, 0.54);
  backdrop-filter: blur(12px);
  box-shadow: 0 8px 18px rgba(41, 111, 151, 0.045);
}

.bess-field__annotation > div {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 10px;
  border-bottom: 1px solid rgba(64, 129, 166, 0.1);
  padding-bottom: 6px;
}

.bess-field__annotation span,
.bess-field__annotation dt {
  color: #55758c;
  font-size: 8px;
  font-weight: 900;
  line-height: 1;
  text-transform: uppercase;
}

.bess-field__annotation strong {
  color: #061b32;
  font-size: 16px;
  font-weight: 900;
}

.bess-field__annotation dl {
  display: grid;
  gap: 5px;
  margin: 7px 0 0;
}

.bess-field__annotation dl > div {
  display: flex;
  justify-content: space-between;
  gap: 9px;
}

.bess-field__annotation dd {
  margin: 0;
  color: #123552;
  font-size: 9.5px;
  font-weight: 850;
  text-align: right;
}

.bess-field__annotation dd.is-discharge {
  color: #168169;
}

.bess-field__annotation dd.is-charge {
  color: #986600;
}

.bess-field__annotation dd.is-hold {
  color: #14739f;
}

.bess-field__signal-stack {
  position: absolute;
  z-index: 6;
  top: 74px;
  left: 50%;
  display: flex;
  width: min(380px, calc(100% - 520px));
  min-width: 300px;
  gap: 5px;
  transform: translateX(-50%);
}

.bess-field__signal-stack button {
  display: grid;
  flex: 1 1 0;
  min-width: 0;
  min-height: 40px;
  border: 1px solid rgba(64, 129, 166, 0.11);
  border-radius: 7px;
  padding: 6px 7px;
  color: #123552;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.58), rgba(241, 250, 253, 0.46)),
    linear-gradient(90deg, rgba(64, 129, 166, 0.035) 1px, transparent 1px);
  background-size: auto, 12px 12px;
  box-shadow: 0 6px 14px rgba(41, 111, 151, 0.04);
  cursor: pointer;
  backdrop-filter: blur(10px);
}

.bess-field__signal-stack button:hover,
.bess-field__signal-stack button.is-selected {
  border-color: rgba(10, 119, 168, 0.34);
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.76), rgba(231, 247, 253, 0.58)),
    linear-gradient(90deg, rgba(64, 129, 166, 0.045) 1px, transparent 1px);
  transform: translateY(-1px);
}

.bess-field__signal-stack button:focus-visible {
  outline: 2px solid #0c7eb3;
  outline-offset: 2px;
}

.bess-field__signal-stack span,
.bess-field__signal-stack strong,
.bess-field__signal-stack em {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.bess-field__signal-stack span {
  color: #55758c;
  font-size: 8px;
  font-weight: 900;
  line-height: 1;
  text-transform: uppercase;
}

.bess-field__signal-stack strong {
  margin-top: 3px;
  color: #061b32;
  font-family: var(--bess-font-data, "Anonymous Pro", "Noto Sans Mono", "Noto Sans", monospace);
  font-size: 12px;
  font-weight: 700;
  line-height: 1;
}

.bess-field__signal-stack em {
  margin-top: 3px;
  color: #315c75;
  font-family: var(--bess-font-data, "Anonymous Pro", "Noto Sans Mono", "Noto Sans", monospace);
  font-size: 9px;
  font-style: normal;
  font-weight: 700;
  line-height: 1;
}

.bess-field__signal--peak {
  box-shadow:
    inset 2px 0 0 rgba(12, 126, 179, 0.58),
    0 6px 14px rgba(41, 111, 151, 0.04);
}

.bess-field__signal--valley,
.bess-field__signal--charge {
  box-shadow:
    inset 2px 0 0 rgba(225, 179, 77, 0.62),
    0 6px 14px rgba(122, 82, 14, 0.035);
}

.bess-field__signal--discharge {
  box-shadow:
    inset 2px 0 0 rgba(27, 157, 126, 0.58),
    0 6px 14px rgba(32, 103, 145, 0.04);
}

.bess-field__signal--hold {
  box-shadow:
    inset 2px 0 0 rgba(143, 215, 236, 0.66),
    0 6px 14px rgba(32, 103, 145, 0.04);
}

.bess-field__hud {
  position: absolute;
  z-index: 3;
  bottom: 14px;
  left: 14px;
  width: min(520px, calc(100% - 28px));
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  gap: 10px;
  pointer-events: none;
}

.bess-field__hud > div {
  flex: 1 1 0;
  min-width: 0;
  border: 1px solid rgba(64, 129, 166, 0.18);
  border-radius: 8px;
  padding: 8px 10px;
  background: rgba(255, 255, 255, 0.72);
  backdrop-filter: blur(10px);
}

.bess-field__hud span,
.bess-field__hud strong {
  display: block;
  max-width: 260px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.bess-field__hud span {
  color: #55758c;
  font-size: 10px;
  font-weight: 850;
  text-transform: uppercase;
}

.bess-field__hud strong {
  margin-top: 3px;
  color: #123552;
  font-size: 12px;
  font-weight: 850;
}

.bess-field__analysis-tape {
  position: absolute;
  z-index: 5;
  right: 14px;
  bottom: 12px;
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  width: clamp(280px, 29vw, 390px);
  border: 1px solid rgba(64, 129, 166, 0.12);
  border-radius: 7px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.48), rgba(241, 250, 253, 0.42)),
    repeating-linear-gradient(
      180deg,
      transparent 0,
      transparent 16px,
      rgba(64, 129, 166, 0.04) 16px,
      rgba(64, 129, 166, 0.04) 17px
    );
  backdrop-filter: blur(8px);
  box-shadow: 0 8px 16px rgba(41, 111, 151, 0.035);
  pointer-events: none;
}

.bess-field__analysis-tape div {
  min-width: 0;
  border-right: 1px solid rgba(64, 129, 166, 0.14);
  padding: 6px 8px;
}

.bess-field__analysis-tape div:last-child {
  border-right: 0;
}

.bess-field__analysis-tape span,
.bess-field__analysis-tape strong {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.bess-field__analysis-tape span {
  color: #55758c;
  font-size: 8px;
  font-weight: 900;
  line-height: 1;
  text-transform: uppercase;
}

.bess-field__analysis-tape strong {
  margin-top: 3px;
  color: #123552;
  font-size: 10px;
  font-weight: 900;
}

.bess-field__interaction-hints {
  position: absolute;
  z-index: 5;
  right: 238px;
  bottom: 96px;
  left: 238px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 12px;
  color: rgba(49, 92, 117, 0.72);
  font-family: var(--bess-font-data, "Anonymous Pro", "Noto Sans Mono", "Noto Sans", monospace);
  font-size: 9px;
  font-weight: 700;
  line-height: 1;
  pointer-events: none;
}

.bess-field__interaction-hints span {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  white-space: nowrap;
}

.bess-field__interaction-hints span::before {
  content: '';
  width: 6px;
  height: 6px;
  border: 1px solid rgba(12, 126, 179, 0.32);
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.64);
  box-shadow: 0 0 0 3px rgba(12, 126, 179, 0.04);
}

@container bess-field (max-width: 760px) {
  .bess-field__interaction-hints {
    display: none;
  }
}

.bess-field__hour-rail {
  position: absolute;
  z-index: 6;
  right: 14px;
  bottom: 62px;
  left: 14px;
  display: grid;
  grid-template-columns: repeat(24, minmax(8px, 1fr));
  gap: 2px;
  border: 1px solid rgba(64, 129, 166, 0.08);
  border-radius: 7px;
  padding: 4px;
  background: rgba(255, 255, 255, 0.28);
  backdrop-filter: blur(8px);
}

.bess-field__hour {
  display: grid;
  min-width: 0;
  min-height: 20px;
  place-items: center;
  border: 0;
  border-radius: 4px;
  padding: 0;
  color: #315c75;
  background: rgba(143, 215, 236, 0.18);
  cursor: pointer;
}

.bess-field__hour span {
  font-size: 8px;
  font-weight: 900;
  line-height: 1;
}

.bess-field__hour--discharge {
  background: rgba(85, 188, 164, 0.28);
}

.bess-field__hour--charge {
  background: rgba(239, 191, 83, 0.32);
}

.bess-field__hour.is-selected {
  box-shadow:
    0 0 0 1px #0a77a8,
    0 6px 12px rgba(10, 119, 168, 0.12);
  color: #061b32;
  background: rgba(255, 255, 255, 0.82);
}

.bess-field__fallback {
  position: absolute;
  z-index: 2;
  inset: 0;
  display: grid;
  align-content: center;
  gap: 10px;
  padding: 28px;
  color: #355b75;
  text-align: center;
  background: rgba(246, 252, 255, 0.82);
}

.bess-field__fallback-plate {
  width: min(100%, 680px);
  margin: 0 auto;
  filter: drop-shadow(0 18px 28px rgba(32, 103, 145, 0.1));
}

.bess-field__fallback-copy {
  display: grid;
  gap: 5px;
}

@keyframes bessFieldScan {
  0%, 18% {
    transform: translateX(-120%);
  }

  58%, 100% {
    transform: translateX(270%);
  }
}

@keyframes bessFieldRipple {
  0% {
    opacity: 0.72;
    transform: scale(0.72);
  }

  72%, 100% {
    opacity: 0;
    transform: scale(1.34);
  }
}

@media (prefers-reduced-motion: reduce) {
  .bess-field::after {
    animation: none;
    opacity: 0;
  }

  .bess-field__crosshair span::before,
  .bess-field__crosshair span::after {
    animation: none;
    opacity: 0.28;
  }
}

@media (max-width: 880px) {
  .bess-field__signal-stack,
  .bess-field__analysis-tape {
    display: none;
  }
}

.bess-field__fallback-copy strong {
  color: #123552;
  font-size: 16px;
  font-weight: 850;
}

.bess-field__fallback-copy span {
  font-size: 13px;
  font-weight: 700;
}

@media (max-width: 700px) {
  .bess-field {
    min-height: 360px;
  }

  .bess-field__legend {
    top: 40px;
    right: 12px;
    gap: 5px;
  }

  .bess-field__mode-toggle {
    top: 42px;
    right: 10px;
    grid-template-columns: repeat(2, minmax(35px, 1fr));
  }

  .bess-field__title {
    right: 12px;
    flex-wrap: wrap;
  }

  .bess-field__legend span {
    padding: 5px 6px;
    font-size: 9px;
  }

  .bess-field__legend span:nth-child(4) {
    display: none;
  }

  .bess-field__annotation {
    top: 74px;
    right: 10px;
    left: auto;
    width: min(188px, calc(100% - 20px));
    padding: 9px;
  }

  .bess-field__annotation > div {
    padding-bottom: 6px;
  }

  .bess-field__annotation strong {
    font-size: 17px;
  }

  .bess-field__annotation dl {
    gap: 5px;
    margin-top: 8px;
  }

  .bess-field__annotation span,
  .bess-field__annotation dt {
    font-size: 8px;
  }

  .bess-field__annotation dd {
    font-size: 10px;
  }

  .bess-field__axis-label {
    display: none;
  }

  .bess-field__scale,
  .bess-field__analysis-tape,
  .bess-field__crosshair {
    display: none;
  }

  .bess-field__hud {
    display: none;
  }

  .bess-field__hour-rail {
    bottom: 10px;
    grid-template-columns: repeat(12, minmax(14px, 1fr));
  }
}

@container bess-field (max-width: 520px) {
  .bess-field__title {
    top: 10px;
    left: 10px;
    right: 10px;
    gap: 6px;
  }

  .bess-field__title strong {
    font-size: 14px;
  }

  .bess-field__title span {
    padding-inline: 4px;
    font-size: 8px;
  }

  .bess-field__legend {
    top: 38px;
    left: 10px;
    right: 96px;
    max-width: calc(100% - 106px);
    gap: 4px;
  }

  .bess-field__mode-toggle {
    top: 38px;
    right: 10px;
  }

  .bess-field__mode-toggle button {
    min-height: 24px;
    padding-inline: 7px;
    font-size: 9px;
  }

  .bess-field__legend span {
    padding: 4px 5px;
    font-size: 8px;
  }

  .bess-field__annotation {
    top: 82px;
    right: 9px;
    width: min(176px, calc(100% - 18px));
    border-color: rgba(64, 129, 166, 0.22);
    background: rgba(250, 254, 255, 0.88);
    backdrop-filter: blur(12px);
  }

  .bess-field__annotation strong {
    font-size: 16px;
  }

  .bess-field__hour-rail {
    right: 8px;
    bottom: 8px;
    left: 8px;
    gap: 4px;
    padding: 6px;
  }

  .bess-field__hour {
    min-height: 20px;
    border-radius: 4px;
  }
}

@media (min-width: 900px) {
  .bess-field__fallback {
    justify-items: start;
    padding-right: 42%;
  }

  .bess-field__fallback-plate,
  .bess-field__fallback-copy {
    width: min(100%, 680px);
    margin-left: 4%;
    margin-right: 0;
  }
}

/* Academic field overlay polish: keep evidence labels visible, but reduce HUD weight. */
.bess-field__title strong {
  color: #12395a;
  font-size: 15px;
  font-weight: 740;
}

.bess-field__title span,
.bess-field__mode-toggle,
.bess-field__legend span,
.bess-field__annotation,
.bess-field__signal-stack button,
.bess-field__hud > div,
.bess-field__hour-rail {
  border-color: rgba(64, 129, 166, 0.09);
  background-color: rgba(255, 255, 255, 0.42);
  box-shadow: 0 5px 12px rgba(41, 111, 151, 0.026);
}

.bess-field__title span,
.bess-field__legend span {
  font-size: 8px;
  font-weight: 700;
}

.bess-field__mode-toggle button {
  font-size: 9px;
  font-weight: 760;
}

.bess-field__legend {
  gap: 4px;
}

.bess-field__legend span {
  padding: 4px 6px;
  color: #42697f;
}

.bess-field__annotation {
  width: min(194px, calc(100% - 32px));
  background: rgba(255, 255, 255, 0.62);
}

.bess-field__annotation span,
.bess-field__annotation dt,
.bess-field__signal-stack span {
  color: #607f93;
  font-size: 7.5px;
  font-weight: 740;
}

.bess-field__annotation strong {
  color: #12395a;
  font-size: 15px;
  font-weight: 780;
}

.bess-field__annotation dd {
  font-size: 9px;
  font-weight: 760;
}

.bess-field__signal-stack button {
  min-height: 36px;
  padding: 5px 6px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.5), rgba(245, 252, 254, 0.38)),
    linear-gradient(90deg, rgba(64, 129, 166, 0.025) 1px, transparent 1px);
  background-size: auto, 12px 12px;
}

.bess-field__signal-stack strong {
  font-size: 11px;
}

.bess-field__signal-stack em {
  font-size: 8px;
}

.bess-field__hour-rail {
  background: rgba(255, 255, 255, 0.22);
}

.bess-field__hour span {
  font-size: 7.5px;
  font-weight: 760;
}

@container bess-field (max-width: 520px) {
  .bess-field__annotation {
    right: 10px;
    width: min(176px, calc(100% - 20px));
  }

  .bess-field__legend span {
    padding: 4px 5px;
  }
}

/* Concept-grade 3D field polish: make the canvas the hero, keep overlays quiet. */
.bess-field {
  border-color: rgba(64, 129, 166, 0.1);
  background:
    linear-gradient(90deg, rgba(70, 136, 175, 0.032) 1px, transparent 1px),
    linear-gradient(0deg, rgba(70, 136, 175, 0.03) 1px, transparent 1px),
    linear-gradient(135deg, rgba(149, 215, 238, 0.08) 0 18%, transparent 18% 100%),
    linear-gradient(145deg, rgba(252, 254, 255, 0.9), rgba(239, 250, 254, 0.44));
  background-size: 32px 32px, 32px 32px, auto, auto;
  box-shadow:
    0 7px 16px rgba(41, 111, 151, 0.026),
    inset 0 1px 0 rgba(255, 255, 255, 0.9);
}

.bess-field::before {
  opacity: 0.62;
  background:
    linear-gradient(115deg, rgba(255, 255, 255, 0.46) 0 16%, transparent 16% 100%),
    linear-gradient(90deg, transparent 0 48%, rgba(12, 126, 179, 0.022) 48% 49%, transparent 49% 100%);
}

.bess-field::after {
  opacity: 0.42;
}

.bess-field__title {
  top: 14px;
}

.bess-field__title strong {
  font-size: 14px;
  font-weight: 760;
}

.bess-field__legend {
  top: 42px;
}

.bess-field__legend span,
.bess-field__mode-toggle,
.bess-field__annotation,
.bess-field__signal-stack button,
.bess-field__hud > div,
.bess-field__analysis-tape,
.bess-field__hour-rail {
  border-color: rgba(64, 129, 166, 0.075);
  background-color: rgba(255, 255, 255, 0.34);
  box-shadow: 0 4px 10px rgba(41, 111, 151, 0.02);
}

.bess-field__legend span {
  padding: 3px 6px;
  color: #40677d;
}

.bess-field__key {
  background: #2e8bea;
}

.bess-field__key--charge {
  background: #f0bd4f;
}

.bess-field__key--hold {
  background: #75d5c3;
}

.bess-field__line {
  background: #187fc8;
}

.bess-field__annotation {
  top: 86px;
  right: auto;
  left: clamp(132px, calc(var(--bess-selected-x) - 90px), calc(100% - 210px));
  width: min(184px, calc(100% - 30px));
  background: rgba(255, 255, 255, 0.5);
}

.bess-field__signal-stack {
  top: 72px;
}

.bess-field__signal-stack button {
  min-height: 34px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.42), rgba(245, 252, 254, 0.3)),
    linear-gradient(90deg, rgba(64, 129, 166, 0.018) 1px, transparent 1px);
  background-size: auto, 12px 12px;
}

.bess-field__crosshair::before {
  border-left-color: rgba(10, 119, 168, 0.22);
}

.bess-field__crosshair span {
  width: 30px;
  height: 30px;
  border-color: rgba(10, 119, 168, 0.32);
  box-shadow:
    0 0 0 7px rgba(10, 119, 168, 0.055),
    0 12px 20px rgba(10, 119, 168, 0.08);
}

.bess-field__hour-rail {
  bottom: 58px;
  border-color: transparent;
  padding-inline: 8px;
  background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.16) 16%, rgba(255, 255, 255, 0.16) 84%, transparent);
  box-shadow: none;
}

.bess-field__hour {
  position: relative;
  min-height: 18px;
  color: #4d7890;
  background: transparent;
}

.bess-field__hour::before {
  content: '';
  position: absolute;
  top: 2px;
  left: 50%;
  width: 2px;
  height: 5px;
  border-radius: 999px;
  background: rgba(117, 213, 195, 0.5);
  transform: translateX(-50%);
}

.bess-field__hour--discharge {
  background: transparent;
  color: #0b67b4;
}

.bess-field__hour--discharge::before {
  background: rgba(46, 139, 234, 0.56);
}

.bess-field__hour--charge {
  background: transparent;
  color: #9a6d11;
}

.bess-field__hour--charge::before {
  background: rgba(240, 189, 79, 0.62);
}

.bess-field__hour.is-selected {
  border-color: rgba(10, 119, 168, 0.42);
  color: #061b32;
  background: rgba(255, 255, 255, 0.68);
  box-shadow:
    0 0 0 1px rgba(10, 119, 168, 0.22),
    0 6px 12px rgba(10, 119, 168, 0.08);
}

.bess-field__hour.is-selected::before {
  height: 7px;
  background: #0c7eb3;
}

/* In-field signal pins: keep shortcuts, remove card clutter from the 3D plane. */
.bess-field__signal-stack {
  inset: 0;
  display: block;
  width: 100%;
  min-width: 0;
  transform: none;
  pointer-events: none;
}

.bess-field__signal-stack button {
  position: absolute;
  top: var(--bess-marker-y);
  left: var(--bess-marker-x);
  display: grid;
  width: 22px;
  height: 22px;
  min-height: 22px;
  min-width: 22px;
  place-items: center;
  border-color: rgba(10, 119, 168, 0.22);
  border-radius: 999px;
  padding: 0;
  overflow: visible;
  background:
    radial-gradient(circle at 50% 50%, rgba(255, 255, 255, 0.95) 0 3px, rgba(255, 255, 255, 0.64) 4px 9px, transparent 10px),
    radial-gradient(circle at 50% 50%, rgba(10, 119, 168, 0.2), transparent 68%);
  box-shadow:
    0 0 0 4px rgba(10, 119, 168, 0.055),
    0 8px 14px rgba(32, 103, 145, 0.07);
  transform: translate(-50%, -50%);
  pointer-events: auto;
}

.bess-field__signal-stack button::before {
  content: '';
  width: 6px;
  height: 6px;
  border-radius: 999px;
  background: #187fc8;
  box-shadow: 0 0 0 2px rgba(255, 255, 255, 0.9);
}

.bess-field__signal-stack button::after {
  content: attr(data-label) '\A' attr(data-value);
  position: absolute;
  bottom: calc(100% + 7px);
  left: 50%;
  width: max-content;
  max-width: 92px;
  border: 1px solid rgba(64, 129, 166, 0.12);
  border-radius: 6px;
  padding: 3px 5px 4px;
  color: #123552;
  background: rgba(255, 255, 255, 0.66);
  box-shadow: 0 6px 12px rgba(41, 111, 151, 0.045);
  font-family: var(--bess-font-data, "Anonymous Pro", "Noto Sans Mono", "Noto Sans", monospace);
  font-size: 7.5px;
  font-weight: 800;
  line-height: 1.1;
  opacity: 0.82;
  text-align: center;
  transform: translate(-50%, 0);
  transition: opacity 160ms ease, transform 160ms ease;
  white-space: pre;
}

.bess-field__signal-stack button:hover,
.bess-field__signal-stack button.is-selected {
  border-color: rgba(10, 119, 168, 0.38);
  background:
    radial-gradient(circle at 50% 50%, rgba(255, 255, 255, 0.98) 0 5px, rgba(255, 255, 255, 0.76) 6px 13px, transparent 14px),
    radial-gradient(circle at 50% 50%, rgba(10, 119, 168, 0.28), transparent 70%);
  box-shadow:
    0 0 0 6px rgba(10, 119, 168, 0.075),
    0 10px 18px rgba(32, 103, 145, 0.1);
  transform: translate(-50%, -50%);
}

.bess-field__signal-stack button:hover::after,
.bess-field__signal-stack button.is-selected::after,
.bess-field__signal-stack button:focus-visible::after {
  opacity: 1;
  transform: translate(-50%, -1px);
}

.bess-field__signal-stack span,
.bess-field__signal-stack strong,
.bess-field__signal-stack em {
  position: absolute;
  width: 1px;
  height: 1px;
  overflow: hidden;
  clip-path: inset(50%);
  white-space: nowrap;
}

.bess-field__signal--valley::before,
.bess-field__signal--charge::before {
  background: #f0bd4f;
}

.bess-field__signal--discharge::before {
  background: #2e8bea;
}

.bess-field__signal--hold::before {
  background: #75d5c3;
}

@container bess-field (max-width: 520px) {
  .bess-field__annotation {
    top: 84px;
    left: auto;
    right: 9px;
    width: min(170px, calc(100% - 18px));
  }

  .bess-field__hour-rail {
    bottom: 8px;
  }
}

@media (max-width: 880px) {
  .bess-field__signal-stack {
    display: none;
  }
}

/* Concept-depth Three.js pass: price wall, translucent curtains, and selected-hour needle. */
/* Dispatch field density pass: make the WebGL evidence plane read closer to the reference concept. */
.bess-field {
  border-color: rgba(12, 126, 179, 0.22);
  background:
    linear-gradient(90deg, rgba(70, 136, 175, 0.05) 1px, transparent 1px),
    linear-gradient(0deg, rgba(70, 136, 175, 0.044) 1px, transparent 1px),
    linear-gradient(135deg, rgba(149, 215, 238, 0.13) 0 18%, transparent 18% 100%),
    linear-gradient(145deg, rgba(253, 255, 255, 0.92), rgba(232, 247, 252, 0.58));
  box-shadow:
    0 10px 20px rgba(41, 111, 151, 0.04),
    inset 0 0 0 1px rgba(115, 190, 220, 0.12),
    inset 0 1px 0 rgba(255, 255, 255, 0.92);
}

.bess-field__canvas :deep(canvas) {
  filter: saturate(1.24) contrast(1.08);
}

.bess-field__legend span,
.bess-field__mode-toggle,
.bess-field__annotation,
.bess-field__analysis-tape {
  background-color: rgba(255, 255, 255, 0.48);
}

/* Interaction fidelity pass: visible shortcuts, keyboard focus, and stronger selected-hour ripple. */
.bess-field__crosshair span {
  width: 34px;
  height: 34px;
  border-width: 2px;
  background:
    radial-gradient(circle at center, rgba(255, 255, 255, 0.92) 0 15%, rgba(12, 126, 179, 0.08) 16% 100%);
  box-shadow:
    0 0 0 8px rgba(10, 119, 168, 0.06),
    0 0 0 18px rgba(10, 119, 168, 0.028),
    0 14px 22px rgba(10, 119, 168, 0.1);
}

.bess-field__signal-stack button {
  width: 20px;
  height: 20px;
  min-width: 20px;
  min-height: 20px;
}

.bess-field__signal-stack button::after {
  max-width: 86px;
  border-color: rgba(64, 129, 166, 0.14);
  padding: 3px 5px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.78), rgba(242, 251, 254, 0.56)),
    linear-gradient(90deg, rgba(64, 129, 166, 0.05) 1px, transparent 1px);
  background-size: auto, 10px 10px;
  font-size: 7px;
  opacity: 0.74;
}

.bess-field__signal-stack button:hover::after,
.bess-field__signal-stack button.is-selected::after,
.bess-field__signal-stack button:focus-visible::after {
  opacity: 1;
}

.bess-field__hour:focus-visible,
.bess-field__signal-stack button:focus-visible {
  outline: none;
  box-shadow:
    0 0 0 1px rgba(10, 119, 168, 0.34),
    0 0 0 5px rgba(10, 119, 168, 0.12),
    0 8px 16px rgba(10, 119, 168, 0.08);
}

.bess-field__hour:focus-visible {
  background: rgba(255, 255, 255, 0.78);
}
</style>
