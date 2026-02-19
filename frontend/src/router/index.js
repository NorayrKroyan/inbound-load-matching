import { createRouter, createWebHistory } from 'vue-router'
import InboundLoadMatchingPage from '../pages/InboundLoadMatchingPage.vue'


export default createRouter({
    history: createWebHistory(),
    routes: [
        { path: '/', name: 'home', component: InboundLoadMatchingPage },

        { path: '/:pathMatch(.*)*', redirect: '/' },

    ],
})
