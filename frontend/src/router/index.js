import { createRouter, createWebHistory } from 'vue-router'
import InboundLoadMatchingPage from '../pages/InboundLoadMatchingPage.vue'
import InboundLoadMatchingLogsPage from '../pages/InboundLoadMatchingLogsPage.vue'

export default createRouter({
    history: createWebHistory(),
    routes: [
        { path: '/', name: 'home', component: InboundLoadMatchingPage },

        {
            path: '/inbound-loads/logs',
            name: 'InboundLoadMatchingLogsPage',
            component: InboundLoadMatchingLogsPage,
        },

        { path: '/:pathMatch(.*)*', redirect: '/' },
    ],
})