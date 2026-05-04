<script setup lang="ts">
import { computed } from 'vue'
import { LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

import type { SignalPreview } from '~/types/control-plane'
import { buildMarketSignalHeroChartOption } from '~/utils/dashboardChartTheme'

use([CanvasRenderer, LineChart, GridComponent, TooltipComponent, LegendComponent])

const props = defineProps<{
  signalPreview: SignalPreview | null
  isLoading: boolean
  lastLoadedLabel: string
}>()

const chartOption = computed(() => buildMarketSignalHeroChartOption(props.signalPreview))
</script>

<template>
  <div class="market-signal-hero">
    <div class="market-signal-hero__toolbar">
      <div class="market-signal-hero__tabs">
        <UButton
          label="DAM"
          color="info"
          variant="ghost"
          class="market-signal-hero__tab market-signal-hero__tab-active"
        />
        <UButton
          label="IDM"
          color="info"
          variant="ghost"
          class="market-signal-hero__tab"
        />
        <UButton
          label="Both"
          color="info"
          variant="ghost"
          class="market-signal-hero__tab"
        />
      </div>

      <div class="market-signal-hero__range">
        <span>24H</span>
        <span>7D</span>
        <span>30D</span>
      </div>

      <UBadge
        class="market-signal-hero__updated"
        :label="lastLoadedLabel"
        color="success"
        variant="soft"
      />
    </div>

    <ClientOnly>
      <div
        v-if="isLoading"
        class="chart-fallback"
      >
        Preparing market signals...
      </div>
      <VChart
        v-else
        class="market-signal-hero__chart"
        :option="chartOption"
        autoresize
      />

      <template #fallback>
        <div class="chart-fallback">
          Preparing market signals...
        </div>
      </template>
    </ClientOnly>
  </div>
</template>
