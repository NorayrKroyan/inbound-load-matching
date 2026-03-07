<template>
  <div class="mx-auto w-full max-w-[1600px] p-4">
    <!-- Top bar -->
    <div class="mb-1 flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
      <div class="min-w-0 xl:pr-6">
        <h2 class="m-0 text-xl font-semibold text-slate-900">
          Inbound Load Matching (Queue)
        </h2>
      </div>

      <!-- Compact Controls -->
      <div class="w-full xl:max-w-[980px]">
        <div class="grid grid-cols-1 gap-x-2 gap-y-1 sm:grid-cols-2 lg:grid-cols-6">
          <!-- Search -->
          <div class="lg:col-span-2">
            <label class="mb-1 block text-[11px] font-semibold uppercase tracking-wide text-slate-500">
              Search
            </label>
            <input
                v-model="q"
                class="h-10 w-full rounded-xl border border-slate-300 bg-white px-3 text-sm outline-none ring-0 placeholder:text-slate-400 focus:border-slate-400"
                placeholder="Driver / truck / job / terminal / load #"
                @keydown.enter="load"
            />
          </div>

          <!-- Confidence -->
          <div>
            <label class="mb-1 block text-[11px] font-semibold uppercase tracking-wide text-slate-500">
              Confidence
            </label>
            <select
                v-model="match"
                class="h-10 w-full rounded-xl border border-slate-300 bg-white px-3 text-sm outline-none focus:border-slate-400"
                @change="load"
            >
              <option value="">All</option>
              <option value="GREEN">GREEN</option>
              <option value="YELLOW">YELLOW</option>
              <option value="RED">RED</option>
            </select>
          </div>

          <!-- Queue -->
          <div>
            <label class="mb-1 block text-[11px] font-semibold uppercase tracking-wide text-slate-500">
              Queue
            </label>
            <select
                v-model="only"
                class="h-10 w-full rounded-xl border border-slate-300 bg-white px-3 text-sm outline-none focus:border-slate-400"
                @change="load"
            >
              <option value="unprocessed">Unprocessed</option>
              <option value="processed">Processed</option>
              <option value="all">All</option>
            </select>
          </div>

          <!-- Status -->
          <div>
            <label class="mb-1 block text-[11px] font-semibold uppercase tracking-wide text-slate-500">
              Status
            </label>
            <select
                v-model="statusFilter"
                class="h-10 w-full rounded-xl border border-slate-300 bg-white px-3 text-sm outline-none focus:border-slate-400"
            >
              <option value="ALL">ALL</option>
              <option value="DELIVEREDCONFIRMED">DELIVERED-CONFIRMED</option>
              <option value="DELIVEREDPENDING">DELIVERED-PENDING</option>
              <option value="INTRANSIT">IN TRANSIT</option>
              <option value="ATTERMINAL">ATTERMINAL</option>
            </select>
          </div>

          <!-- Show -->
          <div>
            <label class="mb-1 block text-[11px] font-semibold uppercase tracking-wide text-slate-500">
              Show
            </label>
            <select
                v-model.number="limit"
                class="h-10 w-full rounded-xl border border-slate-300 bg-white px-3 text-sm outline-none focus:border-slate-400"
                @change="load"
            >
              <option :value="25">25 loads</option>
              <option :value="50">50 loads</option>
              <option :value="100">100 loads</option>
            </select>
          </div>

          <!-- Row 2: buttons -->
          <div class="lg:col-span-2 lg:col-start-5 self-end">
            <div class="grid grid-cols-2 gap-2">
              <button
                  class="h-10 rounded-xl border border-slate-900 bg-slate-950 px-3 text-sm font-medium text-white hover:bg-slate-900 disabled:cursor-not-allowed disabled:opacity-60"
                  :disabled="loading"
                  @click="load"
              >
                Refresh
              </button>

              <button
                  class="h-10 rounded-xl border border-slate-900 bg-slate-900 px-3 text-sm font-medium text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                  :disabled="loading || bulkProcessing || selectedCount === 0"
                  @click="processSelected"
                  title="Processes selected eligible rows one-by-one"
              >
                {{ bulkProcessing ? `Processing ${bulkDone}/${bulkTotal}` : `Process (${selectedCount})` }}
              </button>
            </div>
          </div>        </div>

        <div
            v-if="bulkMsg"
            class="mt-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700"
        >
          {{ bulkMsg }}
        </div>
      </div>
    </div>

    <!-- Error -->
    <div
        v-if="err"
        class="mb-3 whitespace-pre-wrap rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800"
    >
      {{ err }}
    </div>

    <!-- Table Card -->
    <div class="overflow-auto rounded-xl border border-slate-200 bg-white">
      <table class="w-full border-collapse text-xs">
        <thead>
        <tr class="text-left">
          <th
              class="sticky top-0 z-10 w-[34px] border-b border-slate-200 bg-slate-50 px-3 py-2"
              title="Select all eligible visible rows"
          >
            <input
                type="checkbox"
                :checked="allVisibleEligibleSelected"
                :indeterminate.prop="someVisibleEligibleSelected && !allVisibleEligibleSelected"
                :disabled="visibleEligibleRows.length === 0 || loading || bulkProcessing"
                @change="toggleSelectAllVisible($event)"
            />
          </th>

          <th class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50 px-3 py-2">ID</th>
          <th class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50 px-3 py-2">Driver (import)</th>
          <th class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50 px-3 py-2">Truck</th>
          <th class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50 px-3 py-2">Jobname</th>
          <th class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50 px-3 py-2">Terminal</th>
          <th class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50 px-3 py-2">Load</th>
          <th class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50 px-3 py-2">Status</th>
          <th class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50 px-3 py-2">Driver Match</th>
          <th class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50 px-3 py-2">Confidence</th>
          <th class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50 px-3 py-2">Pull Point</th>
          <th class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50 px-3 py-2">Pad Location</th>
          <th class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50 px-3 py-2">Journey</th>
          <th class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50 px-3 py-2">Actions</th>
        </tr>
        </thead>

        <tbody>
        <tr v-for="r in filteredRows" :key="r.import_id" class="align-top">
          <td class="w-[34px] border-b border-slate-100 px-3 py-2">
            <div class="flex h-full items-center justify-center">
              <input
                  type="checkbox"
                  :disabled="!isSelectable(r)"
                  :checked="isSelected(r.import_id)"
                  @change="toggleRow(r)"
                  :title="isSelectable(r)
                    ? 'Select for bulk processing'
                    : 'Not eligible (must be GREEN + READY + resolved driver + carrier + join)'"
              />
            </div>
          </td>

          <td class="border-b border-slate-100 px-3 py-2 font-mono">
            {{ r.import_id }}

            <div
                v-if="r.is_processed"
                class="mt-1 inline-flex items-center rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-bold text-slate-700"
            >
              PROCESSED
            </div>

            <div
                v-if="r.is_inserted"
                class="mt-1 inline-flex items-center rounded-full border border-emerald-200 bg-emerald-50 px-2 py-0.5 text-[11px] font-bold text-emerald-800"
            >
              INSERTED
            </div>

            <div v-if="r.import_load_id" class="mt-1 text-xs font-mono text-slate-700">
              load={{ r.import_load_id }}
            </div>
          </td>

          <td class="border-b border-slate-100 px-3 py-2" :title="r.raw_carrier || r.raw_original || ''">
            {{ r.driver_name || '—' }}
          </td>

          <td class="border-b border-slate-100 px-3 py-2 font-mono" :title="r.raw_truck || r.raw_original || ''">
            {{ r.truck_number || '—' }}
          </td>

          <td class="border-b border-slate-100 px-3 py-2">{{ r.jobname || '—' }}</td>
          <td class="border-b border-slate-100 px-3 py-2">{{ r.terminal || '—' }}</td>
          <td class="border-b border-slate-100 px-3 py-2 font-mono">{{ r.load_number || '—' }}</td>

          <td class="border-b border-slate-100 px-3 py-2 font-mono" :title="r.state || ''">
            {{ r.status_label || '—' }}
          </td>

          <td class="border-b border-slate-100 px-3 py-2">
            <div class="text-xs">
              <div><span class="font-semibold">Status:</span> {{ r.match?.driver?.status || '—' }}</div>

              <div v-if="r.match?.driver?.resolved" class="mt-1">
                <span class="font-semibold">Resolved:</span>
                <span class="font-mono">id_driver={{ r.match.driver.resolved.id_driver }}</span>,
                <span class="font-mono">id_contact={{ r.match.driver.resolved.id_contact }}</span>,
                <span class="font-mono">id_vehicle={{ r.match.driver.resolved.id_vehicle ?? 'null' }}</span>,
                <span class="font-mono">id_carrier={{ r.match.driver.resolved.id_carrier ?? 'null' }}</span>

                <span
                    class="ml-1 inline-flex items-center rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-bold text-slate-700"
                >
                    {{ r.match.driver.resolved.method }}
                  </span>
              </div>

              <div v-if="r.match?.driver?.notes" class="mt-1 whitespace-pre-wrap text-amber-700">
                {{ r.match.driver.notes }}
              </div>
            </div>
          </td>

          <td class="border-b border-slate-100 px-3 py-2">
              <span
                  class="inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-bold"
                  :class="confidenceClasses(r.match?.confidence)"
              >
                {{ r.match?.confidence || 'RED' }}
              </span>
          </td>

          <td class="border-b border-slate-100 px-3 py-2">
            <div class="text-xs">
              <div><span class="font-semibold">Status:</span> {{ r.match?.pull_point?.status || '—' }}</div>
              <div v-if="r.match?.pull_point?.resolved" class="mt-1">
                <span class="font-mono">id={{ r.match.pull_point.resolved.id_pull_point }}</span>
                <div>{{ r.match.pull_point.resolved.pp_job }}</div>
                <span
                    class="mt-1 inline-flex items-center rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-bold text-slate-700"
                >
                    {{ r.match.pull_point.resolved.method }}
                  </span>
              </div>
              <div v-else-if="r.match?.pull_point?.notes" class="mt-1 whitespace-pre-wrap text-amber-700">
                {{ r.match.pull_point.notes }}
              </div>
            </div>
          </td>

          <td class="border-b border-slate-100 px-3 py-2">
            <div class="text-xs">
              <div><span class="font-semibold">Status:</span> {{ r.match?.pad_location?.status || '—' }}</div>
              <div v-if="r.match?.pad_location?.resolved" class="mt-1">
                <span class="font-mono">id={{ r.match.pad_location.resolved.id_pad_location }}</span>
                <div>{{ r.match.pad_location.resolved.pl_job }}</div>
                <span
                    class="mt-1 inline-flex items-center rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-bold text-slate-700"
                >
                    {{ r.match.pad_location.resolved.method }}
                  </span>
              </div>
              <div v-else-if="r.match?.pad_location?.notes" class="mt-1 whitespace-pre-wrap text-amber-700">
                {{ r.match.pad_location.notes }}
              </div>
            </div>
          </td>

          <td class="border-b border-slate-100 px-3 py-2">
            <div class="text-xs">
                <span
                    class="inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-bold"
                    :class="journeyClasses(r.match?.journey?.status)"
                >
                  {{ r.match?.journey?.status || 'NONE' }}
                </span>

              <div v-if="r.match?.journey?.pull_point_id || r.match?.journey?.pad_location_id" class="mt-1 font-mono">
                pp={{ r.match.journey.pull_point_id ?? 'null' }},
                pl={{ r.match.journey.pad_location_id ?? 'null' }}
              </div>

              <div v-if="r.match?.journey?.join_id" class="mt-1 font-mono">
                join={{ r.match.journey.join_id }}
              </div>

              <div
                  v-if="r.match?.journey?.miles !== null && r.match?.journey?.miles !== undefined"
                  class="mt-1 font-mono"
              >
                miles={{ r.match.journey.miles }}
              </div>

              <div v-if="r.match?.journey?.status === 'MISSING_JOIN'" class="mt-1 whitespace-pre-wrap text-amber-700">
                Join not found for pp/pl.
              </div>
            </div>
          </td>

          <td class="border-b border-slate-100 px-3 py-2">
            <button
                class="rounded-xl border border-slate-300 bg-white px-3 py-2 text-xs font-medium text-slate-900 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
                :disabled="!canProcess(r) || processingId === r.import_id || bulkProcessing"
                @click="processRow(r)"
            >
              {{ processingId === r.import_id ? 'Processing...' : 'Process' }}
            </button>

            <div v-if="r.is_processed" class="mt-1 text-xs text-slate-700">
              <div class="font-mono">load={{ r.processed_load_id ?? '—' }}</div>
              <div class="font-mono">detail={{ r.processed_load_detail_id ?? '—' }}</div>
            </div>

            <div v-if="r.is_inserted && !r.is_processed" class="mt-1 text-xs text-slate-700">
              <div class="font-mono">load={{ r.import_load_id ?? '—' }}</div>
            </div>
          </td>
        </tr>

        <tr v-if="filteredRows.length === 0">
          <td colspan="14" class="px-3 py-5 text-center text-sm text-slate-500">
            No rows found.
          </td>
        </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup>
import { computed, ref } from 'vue'
import { useInboundLoadMatching } from '../composables/useInboundLoadMatching'

const {
  rows,
  loading,
  err,
  q,
  match,
  only,
  limit,
  processingId,
  bulkProcessing,
  bulkTotal,
  bulkDone,
  bulkMsg,
  canProcess,
  isSelectable,
  isSelected,
  toggleRow,
  selectedCount,
  load,
  processRow,
  processSelected,
  confidenceClasses,
  journeyClasses
} = useInboundLoadMatching()

const statusFilter = ref('ALL')

function normalizeStatus(value) {
  return String(value || '')
      .toUpperCase()
      .replace(/[\s\-_]/g, '')
}

const filteredRows = computed(() => {
  return (rows.value || []).filter((r) => {
    if (statusFilter.value === 'ALL') return true
    return normalizeStatus(r.status_label || r.stage) === statusFilter.value
  })
})

const visibleEligibleRows = computed(() => {
  return filteredRows.value.filter((r) => isSelectable(r))
})

const allVisibleEligibleSelected = computed(() => {
  return visibleEligibleRows.value.length > 0 &&
      visibleEligibleRows.value.every((r) => isSelected(r.import_id))
})

const someVisibleEligibleSelected = computed(() => {
  return visibleEligibleRows.value.some((r) => isSelected(r.import_id))
})

function toggleSelectAllVisible(event) {
  const checked = !!event.target.checked

  visibleEligibleRows.value.forEach((r) => {
    const selected = isSelected(r.import_id)
    if (checked && !selected) toggleRow(r)
    if (!checked && selected) toggleRow(r)
  })
}

load()
</script>