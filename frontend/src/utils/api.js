// src/utils/api.js
import axios from 'axios'

const base = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000'

export function API(path = '') {
    if (!path) return base
    if (String(path).startsWith('http')) return path
    return `${base}${path}`
}

export const api = axios.create({
    baseURL: base,
    headers: {
        'Content-Type': 'application/json',
    },
})

export async function fetchJson(url, options = {}) {
    const method = (options.method || 'GET').toUpperCase()

    const config = {
        url,
        method,
        params: options.params || undefined,
        data: options.data || undefined,
        headers: options.headers || undefined,
    }

    const res = await api.request(config)
    return res.data
}
