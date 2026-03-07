<template>
  <div class="mx-auto w-full max-w-[1800px] p-4">
    <div class="mb-4 flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
      <div>
        <h1 class="text-2xl font-semibold text-slate-900">Inbound Load Process Logs</h1>
        <div class="mt-1 text-sm text-slate-500">
          Newest entries are shown first. Warning and error rows are highlighted by review status.
        </div>
      </div>

      <div class="flex flex-wrap items-end gap-2">
        <div>
          <label class="mb-1 block text-xs font-medium text-slate-600">Search</label>
          <input
              v-model="filters.q"
              type="text"
              class="h-10 w-[220px] rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none focus:border-slate-400"
              placeholder="message / event / ids"
              @keyup.enter="loadRows"
          />
        </div>

        <div>
          <label class="mb-1 block text-xs font-medium text-slate-600">Run Type</label>
          <select
              v-model="filters.run_type"
              class="h-10 w-[170px] rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none focus:border-slate-400"
          >
            <option value="">All</option>
            <option value="single_process">Single Process</option>
            <option value="batch_process">Batch Process</option>
            <option value="auto_process">Auto Process</option>
          </select>
        </div>

        <div>
          <label class="mb-1 block text-xs font-medium text-slate-600">Severity</label>
          <select
              v-model="filters.severity"
              class="h-10 w-[130px] rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none focus:border-slate-400"
          >
            <option value="">All</option>
            <option value="info">Info</option>
            <option value="warning">Warning</option>
            <option value="error">Error</option>
          </select>
        </div>

        <div>
          <label class="mb-1 block text-xs font-medium text-slate-600">Status</label>
          <select
              v-model="filters.status"
              class="h-10 w-[130px] rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none focus:border-slate-400"
          >
            <option value="">All</option>
            <option value="started">Started</option>
            <option value="success">Success</option>
            <option value="failed">Failed</option>
            <option value="skipped">Skipped</option>
            <option value="exception">Exception</option>
          </select>
        </div>

        <div>
          <label class="mb-1 block text-xs font-medium text-slate-600">Limit</label>
          <select
              v-model.number="filters.limit"
              class="h-10 w-[100px] rounded-lg border border-slate-300 bg-white px-3 text-sm outline-none focus:border-slate-400"
          >
            <option :value="100">100</option>
            <option :value="200">200</option>
            <option :value="300">300</option>
            <option :value="500">500</option>
          </select>
        </div>

        <button
            type="button"
            class="h-10 rounded-lg bg-slate-900 px-4 text-sm font-medium text-white hover:bg-slate-800"
            @click="loadRows"
        >
          Refresh
        </button>

        <button
            type="button"
            class="h-10 rounded-lg border border-slate-300 px-4 text-sm font-medium text-slate-700 hover:bg-slate-50"
            @click="resetFilters"
        >
          Reset
        </button>
      </div>
    </div>

    <div class="mb-4">
      <router-link
          to="/"
          class="inline-flex h-10 items-center rounded-lg border border-slate-300 px-4 text-sm font-medium text-slate-700 hover:bg-slate-50"
      >
        Back to Queue
      </router-link>
    </div>

    <div v-if="err" class="mb-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
      {{ err }}
    </div>

    <div class="rounded-2xl border border-slate-200 bg-white p-3 shadow-sm">
      <DataTable
          :value="rows"
          :loading="loading"
          dataKey="id"
          paginator
          :rows="25"
          :rowsPerPageOptions="[25, 50, 100]"
          responsiveLayout="scroll"
          class="p-datatable-sm"
      >
        <Column header="Review" style="min-width: 120px">
          <template #body="{ data }">
            <span
                :class="reviewClass(data)"
                class="inline-flex rounded-full px-2.5 py-1 text-xs font-semibold"
            >
              {{ reviewLabel(data) }}
            </span>
          </template>
        </Column>

        <Column field="id" header="ID" sortable style="min-width: 80px" />
        <Column field="created_at" header="Created" sortable style="min-width: 170px" />
        <Column field="run_type_label" header="Run Type" sortable style="min-width: 150px" />
        <Column field="session_id" header="Session" style="min-width: 240px" />
        <Column field="loadimport_id" header="Import" sortable style="min-width: 90px" />
        <Column field="id_load" header="Load" sortable style="min-width: 90px" />
        <Column field="id_load_detail" header="Load Detail" sortable style="min-width: 110px" />
        <Column field="stage_detected" header="Stage" sortable style="min-width: 160px" />
        <Column field="event" header="Event" sortable style="min-width: 170px" />

        <Column header="Severity" style="min-width: 120px">
          <template #body="{ data }">
            <span :class="severityClass(data.severity)" class="inline-flex rounded-full px-2.5 py-1 text-xs font-semibold">
              {{ data.severity }}
            </span>
          </template>
        </Column>

        <Column header="Status" style="min-width: 120px">
          <template #body="{ data }">
            <span :class="statusClass(data.status)" class="inline-flex rounded-full px-2.5 py-1 text-xs font-semibold">
              {{ data.status }}
            </span>
          </template>
        </Column>

        <Column field="message" header="Message" style="min-width: 360px">
          <template #body="{ data }">
            <div
                :class="messageBoxClass(data)"
                class="rounded-lg px-3 py-2 whitespace-normal break-words text-sm"
            >
              {{ data.message }}
            </div>
          </template>
        </Column>

        <Column header="Context" style="min-width: 240px">
          <template #body="{ data }">
            <details v-if="data.context_json" class="text-xs text-slate-700">
              <summary class="cursor-pointer font-medium text-slate-600">View</summary>
              <pre class="mt-2 max-h-56 overflow-auto rounded-lg bg-slate-50 p-2 text-xs text-slate-700">{{ prettyContext(data.context_json) }}</pre>
            </details>
            <span v-else class="text-xs text-slate-400">—</span>
          </template>
        </Column>
      </DataTable>
    </div>
  </div>
</template>

<script setup>
import { onMounted, reactive, ref } from 'vue'
import DataTable from 'primevue/datatable'
import Column from 'primevue/column'
import { fetchInboundLoadProcessLogs } from '../utils/api'

const rows = ref([])
const loading = ref(false)
const err = ref('')

const filters = reactive({
  q: '',
  run_type: '',
  severity: '',
  status: '',
  limit: 200,
})

async function loadRows() {
  loading.value = true
  err.value = ''

  try {
    const res = await fetchInboundLoadProcessLogs({
      q: filters.q || undefined,
      run_type: filters.run_type || undefined,
      severity: filters.severity || undefined,
      status: filters.status || undefined,
      limit: filters.limit || 200,
    })

    rows.value = Array.isArray(res?.rows) ? [...res.rows] : []
  } catch (e) {
    err.value = e?.response?.data?.error || e?.message || 'Failed to load logs'
    rows.value = []
  } finally {
    loading.value = false
  }
}

function resetFilters() {
  filters.q = ''
  filters.run_type = ''
  filters.severity = ''
  filters.status = ''
  filters.limit = 200
  loadRows()
}

function severityClass(severity) {
  if (severity === 'error') return 'bg-red-100 text-red-700'
  if (severity === 'warning') return 'bg-amber-100 text-amber-700'
  return 'bg-slate-100 text-slate-700'
}

function statusClass(status) {
  if (status === 'success') return 'bg-emerald-100 text-emerald-700'
  if (status === 'failed' || status === 'exception') return 'bg-red-100 text-red-700'
  if (status === 'started') return 'bg-blue-100 text-blue-700'
  if (status === 'skipped') return 'bg-amber-100 text-amber-700'
  return 'bg-slate-100 text-slate-700'
}

function reviewLabel(row) {
  if (row.severity === 'error' || row.status === 'failed' || row.status === 'exception') return 'Error'
  if (row.severity === 'warning') return 'Warning'
  return 'Normal'
}

function reviewClass(row) {
  if (row.severity === 'error' || row.status === 'failed' || row.status === 'exception') {
    return 'bg-red-100 text-red-700'
  }

  if (row.severity === 'warning') {
    return 'bg-amber-100 text-amber-700'
  }

  return 'bg-emerald-100 text-emerald-700'
}

function messageBoxClass(row) {
  if (row.severity === 'error' || row.status === 'failed' || row.status === 'exception') {
    return 'bg-red-50 text-red-800'
  }

  if (row.severity === 'warning') {
    return 'bg-amber-50 text-amber-800'
  }

  return 'bg-slate-50 text-slate-800'
}

function prettyContext(value) {
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return String(value)
  }
}

onMounted(() => {
  loadRows()
})
</script>