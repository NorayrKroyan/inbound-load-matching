import { ref, computed, watch } from 'vue'
// ✅ Preferred (per doc): move your api helper to resources/js/api/http and import from there.
// import { fetchJson } from '@/api/http'
import { fetchJson } from '../utils/api'

const LIMIT_STORAGE_KEY = 'inbound-load-matching-limit'
const ALLOWED_LIMITS = [25, 50, 100]

function normalizeLimit(value) {
    const num = Number(value)
    return ALLOWED_LIMITS.includes(num) ? num : 25
}

function readStoredLimit() {
    if (typeof window === 'undefined') return 25
    return normalizeLimit(window.localStorage.getItem(LIMIT_STORAGE_KEY))
}

function storeLimit(value) {
    if (typeof window === 'undefined') return
    window.localStorage.setItem(LIMIT_STORAGE_KEY, String(normalizeLimit(value)))
}

function normalizeStatus(value) {
    return String(value || '')
        .toUpperCase()
        .replace(/[\s\-_]/g, '')
}

function queueStatusKey(row) {
    const key = normalizeStatus(row?.status_label || row?.stage || row?.state)

    if (key === 'ATTERMINAL') return 'ATTERMINAL'
    if (key === 'INTRANSIT') return 'INTRANSIT'
    if (key === 'DELIVEREDPENDING') return 'DELIVEREDPENDING'
    if (key === 'DELIVEREDCONFIRMED') return 'DELIVEREDCONFIRMED'

    return 'OTHER'
}

export function useInboundLoadMatching() {
    const rows = ref([])
    const loading = ref(false)
    const err = ref('')

    const q = ref('')
    const match = ref('GREEN')
    const only = ref('all')
    const limit = ref(readStoredLimit())

    const processingId = ref(null)

    // selection
    const selected = ref(new Set())

    // bulk
    const bulkProcessing = ref(false)
    const bulkTotal = ref(0)
    const bulkDone = ref(0)
    const bulkMsg = ref('')

    watch(
        limit,
        (value) => {
            const next = normalizeLimit(value)

            if (Number(value) !== next) {
                limit.value = next
                return
            }

            storeLimit(next)
        },
        { immediate: true }
    )

    function canProcess(r) {
        if (!r) return false
        if (r.is_inserted || r.is_processed) return false

        const conf = r?.match?.confidence
        const journeyStatus = r?.match?.journey?.status

        const resolved = r?.match?.driver?.resolved
        const hasDriver = !!resolved?.id_driver
        const hasCarrier = !!resolved?.id_carrier
        const hasJoin = !!r?.match?.journey?.join_id

        return (
            conf === 'GREEN' &&
            journeyStatus === 'READY' &&
            hasDriver &&
            hasCarrier &&
            hasJoin
        )
    }

    function isSelectable(r) {
        return canProcess(r)
    }

    function isSelected(id) {
        return selected.value.has(Number(id))
    }

    function toggleRow(r) {
        const id = Number(r.import_id)
        if (!isSelectable(r)) return

        const next = new Set(selected.value)
        next.has(id) ? next.delete(id) : next.add(id)
        selected.value = next
    }

    const eligibleIds = computed(() =>
        rows.value.filter(isSelectable).map((r) => Number(r.import_id))
    )

    const selectedCount = computed(() => selected.value.size)

    const eligibleSelectedCount = computed(() => {
        let n = 0
        for (const id of eligibleIds.value) {
            if (selected.value.has(id)) n++
        }
        return n
    })

    const allEligibleSelected = computed(() => {
        const total = eligibleIds.value.length
        return total > 0 && eligibleSelectedCount.value === total
    })

    const someEligibleSelected = computed(() => {
        return (
            eligibleSelectedCount.value > 0 &&
            eligibleSelectedCount.value < eligibleIds.value.length
        )
    })

    const queueCount = computed(() => rows.value.length)

    const statusCounts = computed(() => {
        const counts = {
            ATTERMINAL: 0,
            INTRANSIT: 0,
            DELIVEREDPENDING: 0,
            DELIVEREDCONFIRMED: 0,
            OTHER: 0,
        }

        for (const row of rows.value) {
            counts[queueStatusKey(row)]++
        }

        return counts
    })

    const statusSummary = computed(() => [
        { key: 'ATTERMINAL', label: 'AT TERMINAL', count: statusCounts.value.ATTERMINAL },
        { key: 'INTRANSIT', label: 'IN TRANSIT', count: statusCounts.value.INTRANSIT },
        { key: 'DELIVEREDPENDING', label: 'DELIVERED-PENDING', count: statusCounts.value.DELIVEREDPENDING },
        { key: 'DELIVEREDCONFIRMED', label: 'DELIVERED-CONFIRMED', count: statusCounts.value.DELIVEREDCONFIRMED },
        { key: 'OTHER', label: 'OTHER', count: statusCounts.value.OTHER },
    ])

    function toggleSelectAllEligible(evt) {
        const checked = !!evt?.target?.checked
        const next = new Set(selected.value)

        if (checked) {
            for (const id of eligibleIds.value) next.add(id)
        } else {
            for (const id of eligibleIds.value) next.delete(id)
        }

        selected.value = next
    }

    async function load() {
        err.value = ''
        loading.value = true
        bulkMsg.value = ''

        try {
            const safeLimit = normalizeLimit(limit.value)

            if (limit.value !== safeLimit) {
                limit.value = safeLimit
            }

            const params = new URLSearchParams()
            params.set('limit', String(safeLimit))
            params.set('only', only.value || 'all')

            if (q.value.trim()) params.set('q', q.value.trim())
            if (match.value) params.set('match', match.value)

            const res = await fetchJson(`/api/inbound-loads/queue?${params.toString()}`)

            rows.value = Array.isArray(res?.rows) ? res.rows : []

            if (res?.limit !== undefined && res?.limit !== null && res?.limit !== '') {
                limit.value = normalizeLimit(res.limit)
            } else {
                limit.value = safeLimit
            }

            // keep only currently eligible selected IDs
            const allowed = new Set(eligibleIds.value)
            const next = new Set()

            for (const id of selected.value) {
                if (allowed.has(id)) next.add(id)
            }

            selected.value = next
        } catch (e) {
            err.value = e?.message || String(e)
            rows.value = []
            selected.value = new Set()
        } finally {
            loading.value = false
        }
    }

    async function processRow(r) {
        if (!canProcess(r)) return

        err.value = ''
        processingId.value = r.import_id

        try {
            const res = await fetchJson('/api/inbound-loads/process', {
                method: 'POST',
                data: { import_id: r.import_id },
            })

            if (!res?.ok) {
                throw new Error(res?.error || 'Process failed')
            }

            await load()
        } catch (e) {
            err.value = e?.message || String(e)
        } finally {
            processingId.value = null
        }
    }

    async function processSelected() {
        err.value = ''
        bulkMsg.value = ''

        const ids = Array.from(selected.value).map(Number)
        if (ids.length === 0) return

        bulkProcessing.value = true
        bulkTotal.value = ids.length
        bulkDone.value = 0

        try {
            const res = await fetchJson('/api/inbound-loads/process-batch', {
                method: 'POST',
                data: { import_ids: ids },
            })

            if (!res?.ok) {
                throw new Error(res?.error || 'Batch failed')
            }

            bulkDone.value = bulkTotal.value

            const okCount = Number(res?.ok_count ?? res?.ok_groups ?? 0)
            const failCount = Number(res?.fail_count ?? res?.fail_groups ?? 0)
            const alreadyCount = Number(res?.already_processed_count ?? 0)

            bulkMsg.value = `Done. OK=${okCount}, Failed=${failCount}, Already=${alreadyCount}`

            selected.value = new Set()
            await load()
        } catch (e) {
            err.value = e?.message || String(e)
        } finally {
            bulkProcessing.value = false
        }
    }

    function confidenceClasses(c) {
        if (c === 'GREEN') return 'border-emerald-200 bg-emerald-50 text-emerald-800'
        if (c === 'YELLOW') return 'border-amber-200 bg-amber-50 text-amber-800'
        return 'border-red-200 bg-red-50 text-red-800'
    }

    function journeyClasses(status) {
        if (status === 'READY') return 'border-emerald-200 bg-emerald-50 text-emerald-800'
        if (status === 'PARTIAL') return 'border-amber-200 bg-amber-50 text-amber-800'
        return 'border-red-200 bg-red-50 text-red-800'
    }

    return {
        rows,
        loading,
        err,
        q,
        match,
        only,
        limit,
        processingId,

        // selection / bulk
        selected,
        bulkProcessing,
        bulkTotal,
        bulkDone,
        bulkMsg,
        eligibleIds,
        selectedCount,
        eligibleSelectedCount,
        allEligibleSelected,
        someEligibleSelected,

        // queue summary
        queueCount,
        statusCounts,
        statusSummary,

        // behavior
        canProcess,
        isSelectable,
        isSelected,
        toggleRow,
        toggleSelectAllEligible,
        load,
        processRow,
        processSelected,

        // UI class helpers
        journeyClasses,
        confidenceClasses,
    }
}