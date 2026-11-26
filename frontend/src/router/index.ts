import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import BufferFlowView from '@/views/BufferFlowView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/buffer-flow', name: 'buffer-flow', component: BufferFlowView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
