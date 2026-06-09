<script setup lang="ts">
import { defineAsyncComponent } from 'vue'

defineOptions({
  inheritAttrs: false
})

type ChartAutoresize = boolean | {
  throttle?: number
}

const props = withDefaults(
  defineProps<{
    option: Record<string, unknown>
    autoresize?: ChartAutoresize
  }>(),
  {
    autoresize: true
  }
)

const VChart = defineAsyncComponent(async () => {
  const [
    chartsModule,
    componentsModule,
    coreModule,
    rendererModule,
    vueEchartsModule
  ] = await Promise.all([
    import('echarts/charts'),
    import('echarts/components'),
    import('echarts/core'),
    import('echarts/renderers'),
    import('vue-echarts')
  ])

  coreModule.use([
    rendererModule.CanvasRenderer,
    chartsModule.BarChart,
    chartsModule.LineChart,
    chartsModule.ScatterChart,
    componentsModule.GridComponent,
    componentsModule.GraphicComponent,
    componentsModule.LegendComponent,
    componentsModule.TooltipComponent
  ])

  return vueEchartsModule.default
})
</script>

<template>
  <div
    class="client-v-chart-shell"
    v-bind="$attrs"
  >
    <ClientOnly>
      <component
        :is="VChart"
        :autoresize="props.autoresize"
        :option="props.option"
        class="client-v-chart-shell__chart"
      />
    </ClientOnly>
  </div>
</template>

<style scoped>
.client-v-chart-shell {
  display: block;
  width: 100%;
  min-width: 0;
  min-height: inherit;
  pointer-events: none;
  touch-action: pan-y;
}

.client-v-chart-shell__chart {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  pointer-events: none;
  touch-action: pan-y;
}

.client-v-chart-shell :deep(.echarts),
.client-v-chart-shell :deep(canvas) {
  width: 100% !important;
  height: 100% !important;
  pointer-events: none;
  touch-action: pan-y;
}
</style>
