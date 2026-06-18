<script setup lang="ts">
defineProps<{
  message: string
  title?: string
  isLoading?: boolean
  dismissible?: boolean
}>()

const emit = defineEmits<{
  dismiss: []
}>()
</script>

<template>
  <section class="workspace-alert">
    <div>
      <p class="workspace-alert__title">
        {{ title ?? 'Control surface issue' }}
      </p>
      <p class="workspace-alert__copy">
        {{ message }}
      </p>
      <UProgress
        v-if="isLoading"
        class="workspace-alert__progress"
        animation="carousel"
        color="warning"
      />
    </div>

    <UButton
      v-if="dismissible !== false"
      class="control-button control-button-secondary"
      label="Dismiss"
      color="error"
      variant="solid"
      @click="emit('dismiss')"
    />
  </section>
</template>
