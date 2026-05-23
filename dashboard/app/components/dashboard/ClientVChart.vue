<script setup lang="ts">
import { defineAsyncComponent } from 'vue'

defineOptions({
  inheritAttrs: false
})

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
    componentsModule.LegendComponent,
    componentsModule.TooltipComponent
  ])

  return vueEchartsModule.default
})
</script>

<template>
  <ClientOnly>
    <component
      :is="VChart"
      v-bind="$attrs"
    />
  </ClientOnly>
</template>
