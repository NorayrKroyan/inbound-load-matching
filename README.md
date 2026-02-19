# Inbound Load Matching (Queue)

A small Laravel + Vue tool to review inbound load imports, automatically match Driver / Pull Point / Pad Location, verify Journey (Join), and then **process** eligible imports into the production tables (`load` + `load_detail`).

This project is built as a monorepo:

- `backend/` — Laravel API
- `frontend/` — Vue 3 (Vite) UI

---

## Features

### Queue (Step 1)
Reads from `loadimports` and builds a queue view:

- Parses import fields (driver, truck, trailer, jobname, terminal, load#, ticket#, status, delivery time)
- Detects whether an import is already processed by checking:
  - `load_detail` with `(input_method='IMPORT', input_id=import_id)`
  - OR tracking columns in `loadimports` if present: `is_inserted`, `id_load`, `id_load_detail`, `inserted_at`
- Matches:
  - **Driver** by name and/or truck
  - **Pull Point** by terminal
  - **Pad Location** by jobname
  - **Journey** by join lookup (`join` table) using `(id_pull_point, id_pad_location)` and returns miles

### Confidence
Driver match confidence is computed:

- **GREEN** = both name and truck match the SAME driver (`CONFIRMED`)
- **YELLOW** = partial or conflicting match (`NAME_ONLY`, `TRUCK_ONLY`, `CONFLICT`)
- **RED** = no match

### Eligibility (UI Checkbox + Batch)
Rows are selectable/processable only if:

- `confidence === GREEN`
- `journey.status === READY`
- Driver resolved with `id_driver`
- Driver resolved has `id_carrier`
- Journey has `join_id`
- Row is not already inserted/processed

The UI supports:

- Search + filters
- Individual Process
- **Process Selected** (batch)

### Process Import (Step 2)
When processing an import, the backend:

1. Prevents duplicates using `load_detail(input_method='IMPORT', input_id=import_id)`
2. Inserts into `load` (carrier, join, contact, vehicle, load_date, delivery_time when delivered)
3. Inserts into `load_detail` (load_number, ticket_number, truck/trailer, miles, weights)
4. Updates `loadimports` tracking columns if they exist

### BOL / Ticket file extraction
The service attempts to detect a BOL/ticket file from the import row (payload or columns), and exposes:

- `bol_path`
- `bol_type` (`image` or `pdf`)

**Important:** Current setup supports returning filename/path. If you only want filename, adjust extraction to return basename only.

---

## API Endpoints

Defined in `backend/routes/api.php`:

- `GET  /api/health`
- `GET  /api/inbound-loads/queue`
- `POST /api/inbound-loads/process`
- `POST /api/inbound-loads/process-batch`

### GET /api/inbound-loads/queue

Query params:

- `limit` (default recommended: 200)
- `only` = `unprocessed | processed | all`
- `q` = free text search
- `match` = `GREEN | YELLOW | RED | (empty for All)`

Response shape:

```json
{
  "ok": true,
  "count": 123,
  "rows": [
    {
      "import_id": 1,
      "driver_name": "John Doe",
      "truck_number": "2512",
      "jobname": "...",
      "terminal": "...",
      "load_number": "...",
      "ticket_number": "...",
      "state": "DELIVERED",
      "delivery_time": "02/12/2026 08:23 PM",
      "net_lbs": 43240,
      "tons": 21.62,
      "miles": 72,
      "bol_path": "tickets__xxxx.jpg",
      "bol_type": "image",
      "is_processed": false,
      "match": {
        "confidence": "GREEN",
        "driver": { "status": "CONFIRMED", "resolved": { "...": "..." } },
        "pull_point": { "status": "ONE", "resolved": { "...": "..." } },
        "pad_location": { "status": "ONE", "resolved": { "...": "..." } },
        "journey": { "status": "READY", "join_id": 5, "miles": 72 }
      }
    }
  ]
}
```

### POST /api/inbound-loads/process

Body:

```json
{ "import_id": 123 }
```

Response:

- If already processed (duplicate prevention):

```json
{
  "ok": true,
  "already_processed": true,
  "id_load": 10,
  "id_load_detail": 55
}
```

- If newly inserted:

```json
{
  "ok": true,
  "already_processed": false,
  "id_load": 10,
  "id_load_detail": 55,
  "bol_path": "tickets__xxxx.jpg",
  "bol_type": "image"
}
```

### POST /api/inbound-loads/process-batch

Body:

```json
{ "import_ids": [1,2,3] }
```

Typical response:

```json
{
  "ok": true,
  "ok_count": 2,
  "fail_count": 1,
  "already_processed_count": 0,
  "results": [
    { "import_id": 1, "ok": true, "id_load": 10, "id_load_detail": 55 },
    { "import_id": 2, "ok": false, "error": "Journey is not READY. Cannot process." }
  ]
}
```

---

## Database Expectations

This tool expects (at minimum) these tables to exist on the **default connection**:

### Input
- `loadimports`
  - required: `id`, `payload_json`, `payload_original`, `jobname`, `created_at`, `updated_at`
  - optional (if present, service uses them):  
    `carrier`, `truck`, `terminal`, `state`, `delivery_time`, `load_number`, `ticket_number`  
    tracking columns (optional): `is_inserted`, `id_load`, `id_load_detail`, `inserted_at`

### Production
- `load`
- `load_detail`  
  Required columns used:
  - `id_load`
  - `input_method` (expects `'IMPORT'`)
  - `input_id` (import id)
  - `load_number`, `ticket_number`, `truck_number`, `trailer_number`, `miles`
  - weights: `net_lbs`, `tons` (optional)
  Optional columns if you want ticket writing:
  - `bol_path`
  - `bol_type`

### Lookup tables
- `driver` (expects: `id_driver`, `id_contact`, `id_vehicle`, `id_carrier`)
- `contact` (expects: `id_contact`, `first_name`, `last_name`)
- `vehicle` (expects: `id_vehicle`, `vehicle_number`, `vehicle_name`)
- Pull point table:
  - either `pull_points` or `pull_point` (service auto-detects)
  - expects: `id_pull_point`, `pp_job`, `is_deleted`
- `pad_location` (expects: `id_pad_location`, `pl_job`, `is_deleted`)
- `join` (expects: `id_join`, `id_pull_point`, `id_pad_location`, `miles`, `is_deleted`)

---

## Setup

### 1) Backend (Laravel)

From project root:

```bash
cd backend
composer install
cp .env.example .env
php artisan key:generate
```

Configure DB in `backend/.env`.

Then run:

```bash
php artisan serve
```

This will default to: `http://127.0.0.1:8000`

> Your frontend should proxy `/api/*` to this backend (or you run both behind same host).

---

### 2) Frontend (Vue 3 + Vite)

From project root:

```bash
cd frontend
npm install
npm run dev
```

Defaults to: `http://localhost:5173`

---

## Frontend Page

The queue UI is implemented in a Vue SFC (example name: `InboundLoadMatchingPage.vue`) and uses a composable:

- `frontend/src/composables/useInboundLoadMatching.js` (or `.ts`)
- API helper: `frontend/src/utils/api.js`

The page supports:

- search input
- confidence filter
- processed/unprocessed filter
- refresh
- select eligible rows
- process single row
- process selected batch

---

## How eligibility works (checkbox + processing)

Row is eligible if:

- Not already processed/inserted
- `match.confidence === "GREEN"`
- `match.journey.status === "READY"`
- Resolved driver includes:
  - `id_driver`
  - `id_carrier`
- Journey includes:
  - `join_id`

---

## Notes / Improvements

- Current matching uses DB lookups and fuzzy name logic. With ~3000 rows per table this is OK, but can be optimized further by:
  - caching lookup maps (contacts/drivers/vehicles) in memory per request
  - reducing per-row queries (bulk preload)
  - adding indexes:
    - `load_detail(input_method, input_id)`
    - `vehicle(vehicle_number)`, `vehicle(vehicle_name)`
    - `contact(first_name, last_name)` (or persisted normalized column)
    - `join(id_pull_point, id_pad_location, is_deleted)`
- If you want `filename only` for tickets/BOL, change extraction to return `basename($path)`.

---

## License

Private/internal tool. Add a license if you plan to open-source.
