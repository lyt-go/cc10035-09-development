<template>
  <section class="buffer-flow-page" data-testid="buffer-flow-page">
    <header class="page-head">
      <div>
        <h2>浏览器受理缓冲流程</h2>
        <p class="page-desc">
          面向数据流管道的真实席位受理、幂等认领、容量冲突续办，以及边缘缓存顺序转投和联单销回。
        </p>
      </div>
      <div class="page-actions">
        <RouterLink class="btn" to="/data_stream">返回数据流管道</RouterLink>
        <RouterLink class="btn" to="/edge_cache">前往边缘缓存</RouterLink>
      </div>
    </header>
    <BufferFlowPanel module-key="data_stream" />
  </section>
</template>

<script setup lang="ts">
import BufferFlowPanel from '@/components/BufferFlowPanel.vue'
</script>
