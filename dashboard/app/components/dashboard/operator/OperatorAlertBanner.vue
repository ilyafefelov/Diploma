<script setup lang="ts">
defineProps<{
  message: string
}>()

const emit = defineEmits<{
  dismiss: []
}>()
</script>

<template>
  <section class="workspace-alert">
    <div>
      <p class="workspace-alert__title">
        Control surface issue
      </p>
      <p class="workspace-alert__copy">
        {{ message }}
      </p>
    </div>

    <UButton
      class="control-button control-button-secondary"
      label="Dismiss"
      color="error"
      variant="solid"
      @click="emit('dismiss')"
    />
  </section>
</template>
