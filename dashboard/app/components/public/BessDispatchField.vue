<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, shallowRef, watch } from 'vue'

type DispatchPoint = {
  timestamp?: string
  price_uah_mwh?: number | string
  net_power_mw?: number | string
}

type ThreeModule = typeof import('three')
type ThreeSceneObjects = {
  THREE: ThreeModule
  renderer: import('three').WebGLRenderer
  scene: import('three').Scene
  camera: import('three').PerspectiveCamera
  bars: import('three').Mesh[]
  priceLine: import('three').Line
  priceRibbon: import('three').Mesh
  wave: import('three').Line
  basePlane: import('three').Mesh
  frameId: number | null
  startedAt: number
}

const props = defineProps<{
  schedule: DispatchPoint[]
  sourceStatus: string
  presetLabel: string
}>()

const emit = defineEmits<{
  fallback: [reason: string]
}>()

const rootEl = ref<HTMLElement | null>(null)
const sceneObjects = shallowRef<ThreeSceneObjects | null>(null)
const reducedMotion = ref(false)
const webglFailed = ref(false)
const visibleRows = computed(() => props.schedule.filter(point => Number.isFinite(numberValue(point.price_uah_mwh))))
const hasSceneData = computed(() => visibleRows.value.length >= 2 && !props.sourceStatus.startsWith('blocked'))
const fallbackPlate = computed(() => {
  const rows = visibleRows.value.slice(0, 24)
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
    rootEl.value.appendChild(renderer.domElement)

    const scene = new THREE.Scene()
    const camera = new THREE.PerspectiveCamera(34, rootEl.value.clientWidth / rootEl.value.clientHeight, 0.1, 100)
    camera.position.set(0, 4.8, 9.2)
    camera.lookAt(0, 0.45, 0)

    const ambient = new THREE.AmbientLight(0xf6fbff, 2.2)
    scene.add(ambient)
    const keyLight = new THREE.DirectionalLight(0xd9f5ff, 1.4)
    keyLight.position.set(2.5, 5, 5)
    keyLight.castShadow = true
    scene.add(keyLight)

    const basePlane = new THREE.Mesh(
      new THREE.PlaneGeometry(14.2, 6.4),
      new THREE.MeshStandardMaterial({
        color: 0xf5fcff,
        roughness: 0.9,
        metalness: 0,
        transparent: true,
        opacity: 0.62,
        side: THREE.DoubleSide
      })
    )
    basePlane.rotation.x = -Math.PI / 2
    basePlane.position.y = -0.08
    basePlane.receiveShadow = true
    scene.add(basePlane)

    const grid = new THREE.GridHelper(13.5, 24, 0x6fb4d2, 0xb7d8e8)
    grid.position.y = -0.035
    scene.add(grid)

    const bars: import('three').Mesh[] = []
    const barMaterial = new THREE.MeshStandardMaterial({
      color: 0x79c97c,
      roughness: 0.72,
      metalness: 0.06,
      transparent: true,
      opacity: 0.88
    })
    const chargeMaterial = new THREE.MeshStandardMaterial({
      color: 0xf1c45b,
      roughness: 0.72,
      metalness: 0.04,
      transparent: true,
      opacity: 0.86
    })
    for (let index = 0; index < 24; index += 1) {
      const bar = new THREE.Mesh(new THREE.BoxGeometry(0.34, 1, 0.58), index % 2 ? chargeMaterial.clone() : barMaterial.clone())
      bar.castShadow = true
      scene.add(bar)
      bars.push(bar)
    }

    const priceLine = new THREE.Line(
      new THREE.BufferGeometry(),
      new THREE.LineBasicMaterial({ color: 0x178cc4, linewidth: 2, transparent: true, opacity: 0.95 })
    )
    scene.add(priceLine)

    const priceRibbon = new THREE.Mesh(
      new THREE.BufferGeometry(),
      new THREE.MeshBasicMaterial({
        color: 0x8fd7ec,
        transparent: true,
        opacity: 0.22,
        side: THREE.DoubleSide,
        depthWrite: false
      })
    )
    scene.add(priceRibbon)

    const wave = new THREE.Line(
      new THREE.BufferGeometry(),
      new THREE.LineBasicMaterial({ color: 0x92d7ee, transparent: true, opacity: 0.48 })
    )
    scene.add(wave)

    sceneObjects.value = {
      THREE,
      renderer,
      scene,
      camera,
      bars,
      priceLine,
      priceRibbon,
      wave,
      basePlane,
      frameId: null,
      startedAt: performance.now()
    }
    updateSceneData(sceneObjects.value)
    window.addEventListener('resize', resizeScene)
    document.addEventListener('visibilitychange', handleVisibilityChange)
    startAnimation()
  } catch (error) {
    webglFailed.value = true
    emit('fallback', error instanceof Error ? error.message : 'webgl_unavailable')
    destroyScene()
  }
}

function updateSceneData(objects: ThreeSceneObjects) {
  const rows = visibleRows.value.slice(0, 24)
  const prices = rows.map(point => numberValue(point.price_uah_mwh))
  const powers = rows.map(point => numberValue(point.net_power_mw))
  const minPrice = Math.min(...prices)
  const maxPrice = Math.max(...prices)
  const priceRange = Math.max(1, maxPrice - minPrice)
  const maxPower = Math.max(0.001, ...powers.map(value => Math.abs(value)))
  const barSpan = 11.6

  objects.bars.forEach((bar, index) => {
    const point = rows[index]
    const x = -barSpan / 2 + (index / Math.max(1, rows.length - 1)) * barSpan
    const power = numberValue(point?.net_power_mw)
    const height = Math.max(0.04, Math.abs(power) / maxPower * 3.1)
    bar.scale.set(1, height, 1)
    bar.position.set(x, power >= 0 ? height / 2 : -height / 2, 0)
    bar.rotation.y = 0.11
    const material = bar.material as import('three').MeshStandardMaterial
    material.color.set(power >= 0 ? 0x79c97c : 0xf1c45b)
  })

  const linePoints = rows.map((point, index) => {
    const x = -barSpan / 2 + (index / Math.max(1, rows.length - 1)) * barSpan
    const y = 0.22 + ((numberValue(point.price_uah_mwh) - minPrice) / priceRange) * 2.75
    return new objects.THREE.Vector3(x, y, -1.25)
  })
  objects.priceLine.geometry.dispose()
  objects.priceLine.geometry = new objects.THREE.BufferGeometry().setFromPoints(linePoints)
  objects.priceRibbon.geometry.dispose()
  objects.priceRibbon.geometry = ribbonGeometryFor(objects.THREE, linePoints)

  const wavePoints = rows.map((point, index) => {
    const x = -barSpan / 2 + (index / Math.max(1, rows.length - 1)) * barSpan
    const y = 0.06 + ((numberValue(point.price_uah_mwh) - minPrice) / priceRange) * 0.6
    return new objects.THREE.Vector3(x, y, 1.35)
  })
  objects.wave.geometry.dispose()
  objects.wave.geometry = new objects.THREE.BufferGeometry().setFromPoints(wavePoints)
}

function resizeScene() {
  const objects = sceneObjects.value
  if (!objects || !rootEl.value) {
    return
  }
  const width = rootEl.value.clientWidth
  const height = rootEl.value.clientHeight
  objects.camera.aspect = width / height
  objects.camera.updateProjectionMatrix()
  objects.renderer.setSize(width, height, false)
}

function startAnimation() {
  const objects = sceneObjects.value
  if (!objects) {
    return
  }
  const animate = () => {
    const currentObjects = sceneObjects.value
    if (!currentObjects) {
      return
    }
    const elapsed = (performance.now() - currentObjects.startedAt) / 1000
    currentObjects.scene.rotation.y = Math.sin(elapsed * 0.18) * 0.035
    currentObjects.wave.position.y = Math.sin(elapsed * 1.2) * 0.08
    currentObjects.priceLine.position.y = Math.sin(elapsed * 0.8) * 0.035
    currentObjects.priceRibbon.position.y = Math.sin(elapsed * 0.8) * 0.035
    currentObjects.renderer.render(currentObjects.scene, currentObjects.camera)
    currentObjects.frameId = window.requestAnimationFrame(animate)
  }
  objects.frameId = window.requestAnimationFrame(animate)
}

function handleVisibilityChange() {
  const objects = sceneObjects.value
  if (!objects) {
    return
  }
  if (document.hidden && objects.frameId !== null) {
    window.cancelAnimationFrame(objects.frameId)
    objects.frameId = null
    return
  }
  if (!document.hidden && objects.frameId === null) {
    objects.startedAt = performance.now()
    startAnimation()
  }
}

function destroyScene() {
  const objects = sceneObjects.value
  if (!objects) {
    return
  }
  if (objects.frameId !== null) {
    window.cancelAnimationFrame(objects.frameId)
  }
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
  objects.priceLine.geometry.dispose()
  objects.priceRibbon.geometry.dispose()
  objects.wave.geometry.dispose()
  objects.basePlane.geometry.dispose()
  disposeMaterial(objects.priceRibbon.material)
  disposeMaterial(objects.basePlane.material)
  objects.renderer.dispose()
  sceneObjects.value = null
}

function ribbonGeometryFor(THREE: ThreeModule, linePoints: import('three').Vector3[]): import('three').BufferGeometry {
  const vertices: number[] = []
  const indices: number[] = []
  linePoints.forEach((point, index) => {
    const floorY = 0.02 + Math.sin(index * 0.72) * 0.025
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
</script>

<template>
  <div class="bess-field">
    <div class="bess-field__canvas" ref="rootEl" aria-hidden="true" />
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
  position: relative;
  min-height: 460px;
  overflow: hidden;
  border: 1px solid rgba(64, 129, 166, 0.24);
  border-radius: 8px;
  background:
    linear-gradient(90deg, rgba(70, 136, 175, 0.08) 1px, transparent 1px),
    linear-gradient(0deg, rgba(70, 136, 175, 0.08) 1px, transparent 1px),
    linear-gradient(135deg, rgba(149, 215, 238, 0.38) 0 18%, transparent 18% 100%),
    linear-gradient(145deg, rgba(248, 253, 255, 0.96), rgba(224, 243, 249, 0.9));
  background-size: 32px 32px, 32px 32px, auto, auto;
  box-shadow:
    0 22px 42px rgba(41, 111, 151, 0.12),
    inset 0 1px 0 rgba(255, 255, 255, 0.72);
  contain: layout style paint;
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
    linear-gradient(115deg, rgba(255, 255, 255, 0.54) 0 18%, transparent 18% 100%),
    linear-gradient(90deg, transparent 0 48%, rgba(12, 126, 179, 0.08) 48% 49%, transparent 49% 100%);
}

.bess-field::after {
  width: 38%;
  background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.36), transparent);
  animation: bessFieldScan 8s ease-in-out infinite;
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
}

.bess-field__hud {
  position: absolute;
  z-index: 3;
  right: 14px;
  bottom: 14px;
  left: 14px;
  display: flex;
  justify-content: space-between;
  gap: 10px;
  pointer-events: none;
}

.bess-field__hud > div {
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

.bess-field__fallback {
  position: absolute;
  z-index: 4;
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

@media (prefers-reduced-motion: reduce) {
  .bess-field::after {
    animation: none;
    opacity: 0;
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
    min-height: 330px;
  }

  .bess-field__hud {
    flex-direction: column;
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
</style>
