<template>
  <div class="page">
    <div class="topbar">
      <div>
        <h2 class="h2">Inbound Load Matching (Queue)</h2>
        <div class="sub">
          Step 1 — match driver by Name and/or Truck. GREEN only if both confirm same driver.
        </div>
      </div>

      <div class="controls">
        <input
            class="input"
            v-model="q"
            placeholder="Search driver / truck / job / terminal / load #"
            @keydown.enter="load"
        />

        <select class="input" v-model="match" @change="load">
          <option value="">All</option>
          <option value="GREEN">GREEN</option>
          <option value="YELLOW">YELLOW</option>
          <option value="RED">RED</option>
        </select>

        <select class="input" v-model="only" @change="load">
          <option value="unprocessed">Unprocessed</option>
          <option value="processed">Processed</option>
          <option value="all">All</option>
        </select>

        <button class="btn" :disabled="loading" @click="load">Refresh</button>

        <!-- ✅ Bulk actions -->
        <button
            class="btn btnBulk"
            :disabled="loading || bulkProcessing || selectedCount === 0"
            @click="processSelected"
            title="Processes selected eligible rows one-by-one"
        >
          {{ bulkProcessing ? `Processing (${bulkDone}/${bulkTotal})...` : `Process Selected (${selectedCount})` }}
        </button>

        <div v-if="bulkMsg" class="bulkMsg">{{ bulkMsg }}</div>
      </div>
    </div>

    <div v-if="err" class="err">{{ err }}</div>

    <div class="card">
      <table class="table">
        <thead>
        <tr>
          <!-- ✅ Checkbox header -->
          <th class="thChk">
            <input
                type="checkbox"
                :checked="allEligibleSelected"
                :indeterminate.prop="someEligibleSelected && !allEligibleSelected"
                :disabled="eligibleIds.length === 0 || loading || bulkProcessing"
                @change="toggleSelectAllEligible($event)"
                title="Select all eligible rows"
            />
          </th>

          <th>ID</th>
          <th>Driver (import)</th>
          <th>Truck</th>
          <th>Jobname</th>
          <th>Terminal</th>
          <th>Load #</th>
          <th>Status</th>
          <th>Driver Match</th>
          <th>Confidence</th>
          <th>Pull Point</th>
          <th>Pad Location</th>
          <th>Journey</th>
          <th>Actions</th>
        </tr>
        </thead>

        <tbody>
        <tr v-for="r in rows" :key="r.import_id">
          <!-- ✅ Checkbox cell -->
          <td class="tdChk">
            <input
                type="checkbox"
                :disabled="!isSelectable(r)"
                :checked="isSelected(r.import_id)"
                @change="toggleRow(r)"
                :title="isSelectable(r)
                ? 'Select for bulk processing'
                : 'Not eligible (must be GREEN + READY + resolved driver + carrier + join)'"
            />
          </td>

          <td class="mono">
            {{ r.import_id }}

            <div v-if="r.is_processed" class="pill pill-gray mt6">
              PROCESSED
            </div>

            <div v-if="r.is_inserted" class="pill pill-green mt6">
              INSERTED
            </div>

            <div v-if="r.import_load_id" class="small mt6 mono">
              load={{ r.import_load_id }}
            </div>
          </td>

          <td :title="r.raw_carrier || r.raw_original || ''">
            {{ r.driver_name || '—' }}
          </td>

          <td class="mono" :title="r.raw_truck || r.raw_original || ''">
            {{ r.truck_number || '—' }}
          </td>

          <td>{{ r.jobname || '—' }}</td>
          <td>{{ r.terminal || '—' }}</td>
          <td class="mono">{{ r.load_number || '—' }}</td>
          <td class="mono">{{ r.state || '—' }}</td>

          <td>
            <div class="small">
              <div><b>Status:</b> {{ r.match?.driver?.status || '—' }}</div>

              <div v-if="r.match?.driver?.resolved">
                <b>Resolved:</b>
                <span class="mono">id_driver={{ r.match.driver.resolved.id_driver }}</span>,
                <span class="mono">id_contact={{ r.match.driver.resolved.id_contact }}</span>,
                <span class="mono">id_vehicle={{ r.match.driver.resolved.id_vehicle ?? 'null' }}</span>,
                <span class="mono">id_carrier={{ r.match.driver.resolved.id_carrier ?? 'null' }}</span>
                <span class="pill pill-gray">{{ r.match.driver.resolved.method }}</span>
              </div>

              <div v-if="r.match?.driver?.notes" class="warn">{{ r.match.driver.notes }}</div>
            </div>
          </td>

          <td>
              <span :class="['pill', pillClass(r.match?.confidence)]">
                {{ r.match?.confidence || 'RED' }}
              </span>
          </td>

          <td class="small">
            <div><b>Status:</b> {{ r.match?.pull_point?.status || '—' }}</div>
            <div v-if="r.match?.pull_point?.resolved">
              <span class="mono">id={{ r.match.pull_point.resolved.id_pull_point }}</span>
              <div>{{ r.match.pull_point.resolved.pp_job }}</div>
              <span class="pill pill-gray">{{ r.match.pull_point.resolved.method }}</span>
            </div>
            <div v-else-if="r.match?.pull_point?.notes" class="warn">{{ r.match.pull_point.notes }}</div>
          </td>

          <td class="small">
            <div><b>Status:</b> {{ r.match?.pad_location?.status || '—' }}</div>
            <div v-if="r.match?.pad_location?.resolved">
              <span class="mono">id={{ r.match.pad_location.resolved.id_pad_location }}</span>
              <div>{{ r.match.pad_location.resolved.pl_job }}</div>
              <span class="pill pill-gray">{{ r.match.pad_location.resolved.method }}</span>
            </div>
            <div v-else-if="r.match?.pad_location?.notes" class="warn">{{ r.match.pad_location.notes }}</div>
          </td>

          <td class="small">
              <span
                  class="pill"
                  :class="r.match?.journey?.status === 'READY'
                  ? 'pill-green'
                  : (r.match?.journey?.status === 'PARTIAL' ? 'pill-yellow' : 'pill-red')"
              >
                {{ r.match?.journey?.status || 'NONE' }}
              </span>

            <div class="mono" v-if="r.match?.journey?.pull_point_id || r.match?.journey?.pad_location_id">
              pp={{ r.match.journey.pull_point_id ?? 'null' }},
              pl={{ r.match.journey.pad_location_id ?? 'null' }}
            </div>

            <div class="mono" v-if="r.match?.journey?.join_id">
              join={{ r.match.journey.join_id }}
            </div>

            <div class="mono" v-if="r.match?.journey?.miles !== null && r.match?.journey?.miles !== undefined">
              miles={{ r.match.journey.miles }}
            </div>

            <div v-if="r.match?.journey?.status === 'MISSING_JOIN'" class="warn">
              Join not found for pp/pl.
            </div>
          </td>

          <td>
            <button
                class="btn2"
                :disabled="!canProcess(r) || processingId === r.import_id || bulkProcessing"
                @click="processRow(r)"
            >
              {{ processingId === r.import_id ? 'Processing...' : 'Process' }}
            </button>

            <div v-if="r.is_processed" class="small mt6">
              <div class="mono">load={{ r.processed_load_id ?? '—' }}</div>
              <div class="mono">detail={{ r.processed_load_detail_id ?? '—' }}</div>
            </div>

            <div v-if="r.is_inserted && !r.is_processed" class="small mt6">
              <div class="mono">load={{ r.import_load_id ?? '—' }}</div>
            </div>
          </td>
        </tr>

        <tr v-if="rows.length === 0">
          <td colspan="14" class="empty">No rows found.</td>
        </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup>
import '../styles/inbound-load-matching.css'
import { useInboundLoadMatching } from '../composables/useInboundLoadMatching'

const {
  rows,
  loading,
  err,
  q,
  match,
  only,
  processingId,
  bulkProcessing,
  bulkTotal,
  bulkDone,
  bulkMsg,
  pillClass,
  canProcess,
  isSelectable,
  isSelected,
  toggleRow,
  eligibleIds,
  selectedCount,
  allEligibleSelected,
  someEligibleSelected,
  toggleSelectAllEligible,
  load,
  processRow,
  processSelected,
} = useInboundLoadMatching()

load()
</script>


