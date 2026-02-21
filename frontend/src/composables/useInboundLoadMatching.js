import { ref, computed } from 'vue'
// ✅ Preferred (per doc): move your api helper to resources/js/api/http and import from there.
// import { fetchJson } from '@/api/http'
import { fetchJson } from '../utils/api'

export function useInboundLoadMatching() {
    const rows = ref([])
    const loading = ref(false)
    const err = ref('')

    const q = ref('')
    const match = ref('GREEN')
    const only = ref('all')

    const processingId = ref(null)

    // selection
    const selected = ref(new Set())

    // bulk
    const bulkProcessing = ref(false)
    const bulkTotal = ref(0)
    const bulkDone = ref(0)
    const bulkMsg = ref('')

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
        rows.value.filter(isSelectable).map(r => Number(r.import_id))
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

    const someEligibleSelected = computed(
        () =>
            eligibleSelectedCount.value > 0 &&
            eligibleSelectedCount.value < eligibleIds.value.length
    )

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
            const params = new URLSearchParams()
            params.set('limit', '200')
            params.set('only', only.value || 'unprocessed')
            if (q.value.trim()) params.set('q', q.value.trim())
            if (match.value) params.set('match', match.value)

            const res = await fetchJson(`/api/inbound-loads/queue?${params.toString()}`)
            rows.value = res?.rows || []

            // clean selection: keep only still-eligible IDs
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
                data: { import_id: r.import_id }, // ✅ data (Axios), not body
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
        bulkProcessing.value = true

        const ids = Array.from(selected.value).map(Number)
        bulkTotal.value = ids.length
        bulkDone.value = 0

        try {
            const res = await fetchJson('/api/inbound-loads/process-batch', {
                method: 'POST',
                data: { import_ids: ids }, // ✅ data (Axios), not body
            })

            if (!res?.ok) {
                throw new Error(res?.error || 'Batch failed')
            }

            bulkMsg.value = `Done. OK=${res.ok_count}, Failed=${res.fail_count}, Already=${res.already_processed_count}`

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