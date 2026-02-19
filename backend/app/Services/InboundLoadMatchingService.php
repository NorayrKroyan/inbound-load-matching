<?php

namespace App\Services;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Carbon\Carbon;

class InboundLoadMatchingService
{
    /**
     * Queue = read loadimports (DEFAULT connection)
     * "processed" = found in load_detail via (input_method='IMPORT', input_id=import_id)
     *
     * IMPORTANT: No new columns required in loadimports.
     * If columns exist (is_inserted/id_load/id_load_detail/inserted_at), we update them.
     */
    public function buildQueue(int $limit, string $only, string $q, string $matchFilter): array
    {
        $select = ['id', 'jobname', 'payload_json', 'payload_original', 'created_at', 'updated_at'];

        foreach (['carrier', 'truck', 'terminal', 'state', 'delivery_time', 'load_number', 'ticket_number'] as $col) {
            if ($this->columnExists('loadimports', $col)) $select[] = $col;
        }

        foreach (['is_inserted', 'id_load', 'id_load_detail', 'inserted_at'] as $col) {
            if ($this->columnExists('loadimports', $col)) $select[] = $col;
        }

        $imports = $this->db()->table('loadimports')
            ->select(array_values(array_unique($select)))
            ->orderByDesc('id')
            ->limit($limit)
            ->get();

        $out = [];
        foreach ($imports as $r) {
            $parsed = $this->parseImportRow($r);

            $processedDetail = $this->db()
                ->table('load_detail')
                ->select(['id_load_detail', 'id_load'])
                ->where('input_method', 'IMPORT')
                ->where('input_id', (int)$r->id)
                ->first();

            // if already processed, backfill loadimports columns if missing
            if ($processedDetail) {
                $this->backfillLoadimportsTrackingIfMissing((int)$r->id, (int)$processedDetail->id_load, (int)$processedDetail->id_load_detail);
            }

            $liIsInserted = property_exists($r, 'is_inserted') ? (int)($r->is_inserted ?? 0) : 0;
            $liIdLoad = property_exists($r, 'id_load') ? ($r->id_load !== null ? (int)$r->id_load : null) : null;

            $isProcessed = ($processedDetail ? true : false) || ($liIsInserted === 1) || ($liIdLoad !== null);

            // filtering
            if ($only === 'unprocessed' && $isProcessed) continue;
            if ($only === 'processed' && !$isProcessed) continue;

            if ($q !== '') {
                $hay = strtolower(
                    ($parsed['driver_name'] ?? '') . ' ' .
                    ($parsed['truck_number'] ?? '') . ' ' .
                    ($parsed['trailer_number'] ?? '') . ' ' .
                    ($parsed['jobname'] ?? '') . ' ' .
                    ($parsed['terminal'] ?? '') . ' ' .
                    ($parsed['load_number'] ?? '') . ' ' .
                    ($parsed['ticket_number'] ?? '') . ' ' .
                    ($parsed['raw_carrier'] ?? '') . ' ' .
                    ($parsed['raw_truck'] ?? '') . ' ' .
                    ($parsed['raw_original'] ?? '')
                );
                if (strpos($hay, strtolower($q)) === false) continue;
            }

            $driverMatch = $this->matchDriver($parsed['driver_name'], $parsed['truck_number']);
            $confidence = $this->computeConfidence($driverMatch);
            if ($matchFilter !== '' && $confidence !== $matchFilter) continue;

            $pullPointMatch = $this->matchPullPointByTerminal($parsed['terminal']);
            $padLocationMatch = $this->matchPadLocationByJobname($parsed['jobname']);
            $journey = $this->buildJourney($pullPointMatch, $padLocationMatch);

            $weights = $this->extractWeightsFromPayload($r->payload_json ?? null);
            $netLbs = $weights['net_lbs'];
            $tons = ($netLbs !== null) ? round(((float)$netLbs) / 2000.0, 2) : null;

            // ✅ NEW: attempt to detect BOL file (image/pdf) from payload/row
            $bol = $this->extractBolFromImportRow($r);

            $out[] = [
                'import_id' => (int)$r->id,
                'created_at' => $r->created_at ?? null,
                'updated_at' => $r->updated_at ?? null,

                'driver_name' => $parsed['driver_name'],
                'truck_number' => $parsed['truck_number'],
                'trailer_number' => $parsed['trailer_number'],
                'jobname' => $parsed['jobname'],
                'terminal' => $parsed['terminal'],
                'load_number' => $parsed['load_number'],
                'ticket_number' => $parsed['ticket_number'],
                'state' => $parsed['state'],
                'delivery_time' => $parsed['delivery_time'],

                'net_lbs' => ($netLbs !== null) ? (int)round($netLbs) : null,
                'tons' => $tons,
                'miles' => ($journey['miles'] ?? null),

                // ✅ NEW: show what will be written
                'bol_path' => $bol['bol_path'],
                'bol_type' => $bol['bol_type'],

                'raw_carrier' => $parsed['raw_carrier'],
                'raw_truck' => $parsed['raw_truck'],
                'raw_original' => $parsed['raw_original'],

                'is_processed' => $isProcessed,
                'processed_load_id' => $processedDetail?->id_load ? (int)$processedDetail->id_load : (property_exists($r, 'id_load') ? ($r->id_load !== null ? (int)$r->id_load : null) : null),
                'processed_load_detail_id' => $processedDetail?->id_load_detail ? (int)$processedDetail->id_load_detail : (property_exists($r, 'id_load_detail') ? ($r->id_load_detail !== null ? (int)$r->id_load_detail : null) : null),

                'match' => [
                    'confidence' => $confidence,
                    'driver' => $driverMatch,
                    'pull_point' => $pullPointMatch,
                    'pad_location' => $padLocationMatch,
                    'journey' => $journey,
                ],
            ];
        }

        return $out;
    }

    /**
     * Push one import into `load` + `load_detail`.
     * Duplicate prevention via load_detail(input_method='IMPORT', input_id=import_id).
     * Also updates loadimports tracking columns IF they exist.
     */
    public function processImport(int $importId): array
    {
        $row = $this->db()->table('loadimports')->where('id', $importId)->first();
        if (!$row) {
            return ['ok' => false, 'error' => "Import id={$importId} not found."];
        }

        $existing = $this->db()
            ->table('load_detail')
            ->select(['id_load_detail', 'id_load'])
            ->where('input_method', 'IMPORT')
            ->where('input_id', $importId)
            ->first();

        if ($existing) {
            $this->backfillLoadimportsTrackingIfMissing($importId, (int)$existing->id_load, (int)$existing->id_load_detail);
            return [
                'ok' => true,
                'already_processed' => true,
                'id_load' => (int)$existing->id_load,
                'id_load_detail' => (int)$existing->id_load_detail,
            ];
        }

        $parsed = $this->parseImportRow($row);

        $driverMatch = $this->matchDriver($parsed['driver_name'], $parsed['truck_number']);
        $resolvedDriver = $driverMatch['resolved'] ?? null;
        if (!$resolvedDriver) return ['ok' => false, 'error' => 'No driver resolved. Cannot process.'];

        $idCarrier = $resolvedDriver['id_carrier'] ? (int)$resolvedDriver['id_carrier'] : null;
        if (!$idCarrier) return ['ok' => false, 'error' => 'Resolved driver has no id_carrier. Cannot process.'];

        $pullPointMatch = $this->matchPullPointByTerminal($parsed['terminal']);
        $padLocationMatch = $this->matchPadLocationByJobname($parsed['jobname']);
        $journey = $this->buildJourney($pullPointMatch, $padLocationMatch);

        if (($journey['status'] ?? 'NONE') !== 'READY') return ['ok' => false, 'error' => 'Journey is not READY. Cannot process.'];

        $joinId = $journey['join_id'] ? (int)$journey['join_id'] : null;
        if (!$joinId) return ['ok' => false, 'error' => 'Journey READY but join_id missing. Cannot process.'];

        $miles = isset($journey['miles']) && $journey['miles'] !== null ? (int)$journey['miles'] : null;

        $stateRaw = strtoupper(trim((string)($parsed['state'] ?? '')));
        $state = $this->normalizeState($stateRaw);

        $deliveryTime = $this->cleanDeliveryTime($parsed['delivery_time'] ?? null);

        $weights = $this->extractWeightsFromPayload($row->payload_json ?? null);
        $netLbs = $weights['net_lbs'];
        $tons = ($netLbs !== null) ? round(((float)$netLbs) / 2000.0, 2) : null;

        $idContact = (int)$resolvedDriver['id_contact'];
        $idVehicle = $resolvedDriver['id_vehicle'] ? (int)$resolvedDriver['id_vehicle'] : null;

        $loadDate = $this->guessLoadDate($deliveryTime, $row->created_at ?? null);

        if ($state === 'IN_TRANSIT' && $netLbs === null) {
            return ['ok' => false, 'error' => 'IN_TRANSIT but no weight found in payload.'];
        }

        $deliveryMysql = null;
        if ($state === 'DELIVERED') {
            if (!$deliveryTime) return ['ok' => false, 'error' => 'DELIVERED but delivery_time missing in import.'];
            $deliveryMysql = $this->toMysqlDatetime($deliveryTime);
            if (!$deliveryMysql) return ['ok' => false, 'error' => "DELIVERED but delivery_time not parseable: '{$deliveryTime}'."];
        }

        // ✅ NEW: detect BOL file from import payload/row
        $bol = $this->extractBolFromImportRow($row);

        return $this->db()->transaction(function () use (
            $importId,
            $idCarrier,
            $idContact,
            $idVehicle,
            $joinId,
            $miles,
            $state,
            $deliveryMysql,
            $loadDate,
            $netLbs,
            $tons,
            $parsed,
            $bol
        ) {
            $loadInsert = [
                'id_carrier' => $idCarrier,
                'id_join' => $joinId,
                'id_contact' => $idContact,
                'id_vehicle' => $idVehicle,
                'is_deleted' => 0,
                'load_date' => $loadDate,
                'is_finished' => null,
            ];

            if ($state === 'DELIVERED' && $deliveryMysql) {
                $loadInsert['delivery_time'] = $deliveryMysql;
                $loadInsert['is_finished'] = 1;
            }

            $idLoad = (int)$this->db()->table('load')->insertGetId($loadInsert);

            $detailInsert = [
                'id_load' => $idLoad,
                'input_method' => 'IMPORT',
                'input_id' => $importId,
                'load_number' => $parsed['load_number'] ?? null,
                'ticket_number' => $parsed['ticket_number'] ?? null,
                'truck_number' => $parsed['truck_number'] ?? null,
                'trailer_number' => $parsed['trailer_number'] ?? null,
                'miles' => $miles,
            ];

            if ($netLbs !== null) {
                $detailInsert['net_lbs'] = (int)round($netLbs);
                $detailInsert['tons'] = $tons;
            }

            // ✅ NEW: write bol_path/bol_type if columns exist
            if ($this->columnExists('load_detail', 'bol_path') && ($bol['bol_path'] ?? null)) {
                $detailInsert['bol_path'] = $bol['bol_path'];
            }
            if ($this->columnExists('load_detail', 'bol_type') && ($bol['bol_type'] ?? null)) {
                $detailInsert['bol_type'] = $bol['bol_type']; // 'image' or 'pdf'
            }

            $idLoadDetail = (int)$this->db()->table('load_detail')->insertGetId($detailInsert);

            // ✅ THIS WILL NOW ACTUALLY RUN because Schema::hasColumn works
            $this->updateLoadimportsTracking($importId, $idLoad, $idLoadDetail);

            return [
                'ok' => true,
                'already_processed' => false,
                'id_load' => $idLoad,
                'id_load_detail' => $idLoadDetail,
                'bol_path' => $bol['bol_path'],
                'bol_type' => $bol['bol_type'],
            ];
        });
    }

    // ------------------ NEW: BOL extraction ------------------

    /**
     * Try to find a ticket/BOL file path in the import row.
     * Stores:
     *  - bol_path: string (e.g. "loadimports/.../tickets__...jpg" or URL)
     *  - bol_type: "image" | "pdf"
     *
     * Works without requiring ANY new columns in loadimports by scanning:
     *  - decoded payload_json (attachments/tickets/etc)
     *  - payload_original
     *  - any string fields on the row (if your importer stores file path in some column)
     */
    private function extractBolFromImportRow(object $r): array
    {
        $candidates = [];

        // 1) payload_json scan (common patterns)
        $payloadJson = $r->payload_json ?? null;
        if (is_string($payloadJson) && trim($payloadJson) !== '') {
            $d = json_decode($payloadJson, true);
            if (is_array($d)) {
                $this->collectFileCandidatesFromMixed($d, $candidates);
            }
        }

        // 2) payload_original (sometimes contains a filename/path)
        $po = $this->strOrNull($r->payload_original ?? null);
        if ($po) $candidates[] = $po;

        // 3) scan all string columns on the row (your screenshot shows a jpg name in a column)
        foreach (get_object_vars($r) as $k => $v) {
            if ($v === null) continue;
            if (!is_string($v)) continue;
            $s = trim($v);
            if ($s === '') continue;
            $candidates[] = $s;
        }

        // normalize + pick best file
        $best = $this->pickBestBolCandidate($candidates);

        return [
            'bol_path' => $best['path'],
            'bol_type' => $best['type'],
        ];
    }

    private function collectFileCandidatesFromMixed($mixed, array &$out): void
    {
        if ($mixed === null) return;

        if (is_string($mixed)) {
            $out[] = $mixed;
            return;
        }

        if (is_array($mixed)) {
            foreach ($mixed as $k => $v) {
                // common keys that may directly hold a path/url
                if (is_string($k)) {
                    $kk = strtolower($k);
                    if (in_array($kk, [
                        'bol', 'bol_path', 'bolfile', 'bol_file', 'bolurl', 'bol_url',
                        'ticket', 'ticket_path', 'ticketfile', 'ticket_file', 'ticketurl', 'ticket_url',
                        'ticket_img', 'ticket_image', 'ticket_pdf',
                        'image', 'image_path', 'imageurl',
                        'pdf', 'pdf_path', 'pdfurl',
                        'attachment', 'attachments', 'files', 'documents', 'docs', 'tickets'
                    ], true)) {
                        // still recurse; value may be array/object/string
                    }
                }
                $this->collectFileCandidatesFromMixed($v, $out);
            }
        }
    }

    private function pickBestBolCandidate(array $candidates): array
    {
        // flatten candidates by splitting on whitespace and commas (so "a b c.pdf" still catches c.pdf)
        $flat = [];
        foreach ($candidates as $c) {
            if (!is_string($c)) continue;
            $c = trim($c);
            if ($c === '') continue;

            foreach (preg_split('/[\s,]+/u', $c) as $part) {
                $p = trim($part);
                if ($p !== '') $flat[] = $p;
            }
            $flat[] = $c; // keep original too
        }

        $bestPath = null;
        $bestType = null;
        $bestScore = -999;

        foreach ($flat as $s) {
            $path = $this->cleanCandidatePath($s);
            if (!$path) continue;

            // ignore load.json (your screenshot shows that alongside ticket file)
            if (preg_match('/\/?load\.json$/i', $path)) continue;

            $type = $this->bolTypeFromPath($path);
            if (!$type) continue;

            // scoring:
            // - prefer pdf over image? (you can swap if you want)
            // - prefer strings containing "ticket" or "bol"
            // - prefer longer paths (more likely a real path)
            $score = 0;
            $lower = strtolower($path);
            if (str_contains($lower, 'ticket')) $score += 10;
            if (str_contains($lower, 'bol')) $score += 10;
            if ($type === 'pdf') $score += 3;
            if ($type === 'image') $score += 2;
            $score += min(20, (int)(strlen($path) / 10));

            if ($score > $bestScore) {
                $bestScore = $score;
                $bestPath = $path;
                $bestType = $type;
            }
        }

        return [
            'path' => $bestPath,
            'type' => $bestType,
        ];
    }

    private function cleanCandidatePath(string $s): ?string
    {
        $s = trim($s);
        if ($s === '') return null;

        // strip wrapping quotes/brackets
        $s = trim($s, " \t\n\r\0\x0B\"'[](){}");

        // if it looks like JSON fragment, ignore
        if (str_starts_with($s, '{') || str_starts_with($s, '[')) return null;

        // basic sanity: must contain a dot-extension somewhere
        if (!preg_match('/\.[A-Za-z0-9]{2,5}($|\?)/', $s)) return null;

        return $s;
    }

    private function bolTypeFromPath(string $path): ?string
    {
        $p = strtolower($path);
        // strip querystring
        $p = preg_replace('/\?.*$/', '', $p);

        if (preg_match('/\.pdf$/', $p)) return 'pdf';
        if (preg_match('/\.(jpg|jpeg|png|gif|webp|bmp|tif|tiff)$/', $p)) return 'image';

        return null;
    }

    // ------------------ loadimports tracking ------------------

    private function updateLoadimportsTracking(int $importId, int $idLoad, int $idLoadDetail): void
    {
        $upd = [];

        if ($this->columnExists('loadimports', 'is_inserted')) $upd['is_inserted'] = 1;
        if ($this->columnExists('loadimports', 'id_load')) $upd['id_load'] = $idLoad;
        if ($this->columnExists('loadimports', 'id_load_detail')) $upd['id_load_detail'] = $idLoadDetail;
        if ($this->columnExists('loadimports', 'inserted_at')) $upd['inserted_at'] = DB::raw('NOW()');

        if (!empty($upd)) {
            $this->db()->table('loadimports')->where('id', $importId)->update($upd);
        }
    }

    private function backfillLoadimportsTrackingIfMissing(int $importId, int $idLoad, int $idLoadDetail): void
    {
        $hasAny =
            $this->columnExists('loadimports', 'is_inserted') ||
            $this->columnExists('loadimports', 'id_load') ||
            $this->columnExists('loadimports', 'id_load_detail') ||
            $this->columnExists('loadimports', 'inserted_at');

        if (!$hasAny) return;

        $row = $this->db()->table('loadimports')->where('id', $importId)->first();
        if (!$row) return;

        $upd = [];

        if ($this->columnExists('loadimports', 'is_inserted')) {
            $cur = property_exists($row, 'is_inserted') ? (int)($row->is_inserted ?? 0) : 0;
            if ($cur !== 1) $upd['is_inserted'] = 1;
        }

        if ($this->columnExists('loadimports', 'id_load')) {
            $cur = property_exists($row, 'id_load') ? $row->id_load : null;
            if ($cur === null) $upd['id_load'] = $idLoad;
        }

        if ($this->columnExists('loadimports', 'id_load_detail')) {
            $cur = property_exists($row, 'id_load_detail') ? $row->id_load_detail : null;
            if ($cur === null) $upd['id_load_detail'] = $idLoadDetail;
        }

        if ($this->columnExists('loadimports', 'inserted_at')) {
            $cur = property_exists($row, 'inserted_at') ? $row->inserted_at : null;
            if ($cur === null) $upd['inserted_at'] = DB::raw('NOW()');
        }

        if (!empty($upd)) {
            $this->db()->table('loadimports')->where('id', $importId)->update($upd);
        }
    }

    // ------------------ parsing ------------------

    private function parseImportRow(object $r): array
    {
        $data = [];
        $payloadJson = $r->payload_json ?? null;
        if ($payloadJson) {
            $decoded = json_decode($payloadJson, true);
            if (is_array($decoded)) $data = $decoded;
        }

        $jobname = $this->strOrNull($r->jobname ?? null) ?? $this->strOrNull($data['jobname'] ?? null);

        $terminal =
            $this->strOrNull($r->terminal ?? null)
            ?? $this->strOrNull($data['terminal'] ?? null);

        $state =
            $this->strOrNull($r->state ?? null)
            ?? $this->strOrNull($data['status'] ?? $data['state'] ?? null);

        $deliveryTime =
            $this->strOrNull($r->delivery_time ?? null)
            ?? $this->strOrNull($data['delivery_time'] ?? null)
            ?? $this->strOrNull($data['datetime_delivered'] ?? null)
            ?? $this->strOrNull($data['status_time'] ?? null);

        $loadNumber =
            $this->strOrNull($r->load_number ?? null)
            ?? $this->strOrNull($data['loadnumber'] ?? $data['load_number'] ?? null);

        $ticketNumber =
            $this->strOrNull($r->ticket_number ?? null)
            ?? $this->strOrNull($data['ticket_no'] ?? $data['ticket_number'] ?? null);

        $carrierStr =
            $this->strOrNull($r->carrier ?? null)
            ?? $this->strOrNull($data['carrier'] ?? null);

        $truckStr =
            $this->strOrNull($r->truck ?? null)
            ?? $this->strOrNull($data['truck_trailer'] ?? null)
            ?? $this->strOrNull($data['truck_number'] ?? null)
            ?? $this->strOrNull($data['truck'] ?? null);

        $originalStr = $this->strOrNull($r->payload_original ?? null);

        if (!$carrierStr && $originalStr) $carrierStr = $originalStr;
        if (!$truckStr && $originalStr) $truckStr = $originalStr;

        $driverName = $this->extractDriverName($carrierStr);

        $truckNumber = $this->extractTruckNumber($truckStr);
        $trailerNumber = $this->extractTrailerNumber($truckStr);

        $trailerExplicit =
            $this->strOrNull($data['trailer_number'] ?? null)
            ?? $this->strOrNull($data['trailer'] ?? null)
            ?? $this->strOrNull($data['trailer_no'] ?? null);

        if ($trailerExplicit) {
            $trailerNumber = $trailerExplicit;
        }

        return [
            'driver_name' => $driverName,
            'truck_number' => $truckNumber,
            'trailer_number' => $trailerNumber,

            'jobname' => $this->strOrNull($jobname),
            'terminal' => $this->strOrNull($terminal),
            'load_number' => $loadNumber,
            'ticket_number' => $ticketNumber,
            'state' => $state,
            'delivery_time' => $deliveryTime,

            'raw_carrier' => $carrierStr,
            'raw_truck' => $truckStr,
            'raw_original' => $originalStr,
        ];
    }

    // ------------------ delivery_time cleanup ------------------

    private function cleanDeliveryTime(?string $s): ?string
    {
        $s = $this->strOrNull($s);
        if (!$s) return null;

        $s = preg_replace('/^[A-Za-z_\-]+\s*:\s*/u', '', $s);
        $s = preg_replace('/^[A-Za-z_\-]+\s+/u', '', $s);

        return $this->strOrNull($s);
    }

    // ------------------ weights ------------------

    private function extractWeightsFromPayload(?string $payloadJson): array
    {
        $net = null;
        $box1 = null;
        $box2 = null;

        if ($payloadJson) {
            $d = json_decode($payloadJson, true);

            if (is_array($d)) {
                $net = $this->toFloatOrNull($d['total_weight'] ?? null);

                if ($net === null) {
                    $net = $this->toFloatOrNull(
                        $d['total_lbs'] ?? $d['net_lbs'] ?? $d['netlbs'] ?? $d['net'] ?? null
                    );
                }

                if ($net === null && isset($d['box_numbers'])) {
                    $parts = array_map('trim', explode(',', (string)$d['box_numbers']));
                    $nums = array_map(fn($x) => $this->toFloatOrNull($x), $parts);
                    $nums = array_values(array_filter($nums, fn($x) => $x !== null));
                    if (count($nums) >= 1) {
                        $box1 = $nums[0] ?? null;
                        $box2 = $nums[1] ?? null;
                        $sum = 0.0;
                        foreach ($nums as $n) $sum += $n;
                        $net = $sum > 0 ? $sum : null;
                    }
                }

                if ($net === null && isset($d['weight'])) {
                    preg_match_all('/(^|\n)\s*([0-9,]+)\s*(?=\(|$)/m', (string)$d['weight'], $m);
                    if (!empty($m[2])) {
                        $nums = [];
                        foreach ($m[2] as $raw) {
                            $v = $this->toFloatOrNull($raw);
                            if ($v !== null) $nums[] = $v;
                        }
                        if (count($nums) >= 1) {
                            $box1 = $nums[0] ?? null;
                            $box2 = $nums[1] ?? null;
                            $sum = array_sum($nums);
                            $net = $sum > 0 ? $sum : null;
                        }
                    }
                }
            }
        }

        return [
            'box1' => $box1,
            'box2' => $box2,
            'net_lbs' => ($net !== null && $net > 0) ? $net : null,
        ];
    }

    private function toFloatOrNull($v): ?float
    {
        if ($v === null) return null;
        if (is_numeric($v)) return (float)$v;
        $s = trim((string)$v);
        if ($s === '') return null;
        $s = preg_replace('/[^0-9.\-]/', '', $s);
        return is_numeric($s) ? (float)$s : null;
    }

    // ------------------ date handling ------------------

    private function guessLoadDate(?string $deliveryTime, $createdAt): ?string
    {
        $dt = $deliveryTime ?: ($createdAt ? (string)$createdAt : null);
        if (!$dt) return null;

        if (preg_match('/^\d{2}-\d{2}-\d{4}$/', $dt)) return $dt;

        if (preg_match('/^(\d{4})-(\d{2})-(\d{2})/', $dt, $m)) {
            return "{$m[2]}-{$m[3]}-{$m[1]}";
        }

        if (preg_match('/^(\d{2})\/(\d{2})\/(\d{4})/', $dt, $m)) {
            return "{$m[1]}-{$m[2]}-{$m[3]}";
        }

        return null;
    }

    private function toMysqlDatetime(string $s): ?string
    {
        $s = trim($s);
        if ($s === '') return null;

        if (preg_match('/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$/', $s)) return $s;
        if (preg_match('/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$/', $s)) return $s . ':00';
        if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $s)) return $s . ' 00:00:00';

        $formats = [
            'm/d/Y h:i A',
            'm/d/Y h:i:s A',
            'm-d-Y h:i A',
            'm-d-Y h:i:s A',
            'n/j/Y g:i A',
            'n/j/Y g:i:s A',
        ];

        foreach ($formats as $fmt) {
            try {
                $dt = Carbon::createFromFormat($fmt, $s);
                return $dt->format('Y-m-d H:i:s');
            } catch (\Throwable $e) {}
        }

        try {
            return Carbon::parse($s)->format('Y-m-d H:i:s');
        } catch (\Throwable $e) {
            return null;
        }
    }

    private function normalizeState(string $stateRaw): string
    {
        $s = strtoupper(trim($stateRaw));
        $s = str_replace(' ', '_', $s);
        if ($s === 'INTRANSIT') $s = 'IN_TRANSIT';
        if ($s === 'IN-TRANSIT') $s = 'INTRANSIT';
        if ($s === 'DELIVERED') return 'DELIVERED';
        if ($s === 'IN_TRANSIT') return 'IN_TRANSIT';
        return $s;
    }

    // ------------------ Driver parsing ------------------

    private function extractDriverName(?string $carrier): ?string
    {
        if (!$carrier) return null;

        $lines = preg_split("/\r\n|\n|\r/", $carrier);
        if (is_array($lines) && count($lines) >= 2) {
            $maybe = $this->strOrNull($lines[1]);
            if ($maybe) return $maybe;
        }

        if (preg_match('/\b([A-Z][a-z]+)\s+([A-Z][a-z]+)\b/', $carrier, $m)) {
            return $this->strOrNull($m[1] . ' ' . $m[2]);
        }

        return $this->strOrNull($carrier);
    }

    private function extractTruckNumber(?string $text): ?string
    {
        if (!$text) return null;

        if (preg_match('/Truck\s*#?:?\s*([A-Za-z0-9]+)/i', $text, $m)) {
            return $this->strOrNull($m[1]);
        }

        if (preg_match('/^\s*([A-Za-z0-9]+)\s*[\/\-]\s*([A-Za-z0-9]+)\s*$/', trim($text), $m)) {
            return $this->strOrNull($m[1]);
        }

        $t = trim($text);
        if ($t !== '' && preg_match('/^[A-Za-z0-9]+$/', $t)) {
            return $this->strOrNull($t);
        }

        return null;
    }

    private function extractTrailerNumber(?string $text): ?string
    {
        if (!$text) return null;

        if (preg_match('/Trailer\s*#?:?\s*([A-Za-z0-9]+)/i', $text, $m)) {
            return $this->strOrNull($m[1]);
        }

        if (preg_match('/^\s*([A-Za-z0-9]+)\s*[\/\-]\s*([A-Za-z0-9]+)\s*$/', trim($text), $m)) {
            return $this->strOrNull($m[2]);
        }

        return null;
    }

    // ------------------ Matching ------------------

    private function matchDriver(?string $driverName, ?string $truckNumber): array
    {
        $nameDriver = null;
        $truckDriver = null;
        $notes = [];

        $driverCols = ['id_driver', 'id_contact', 'id_vehicle', 'id_carrier'];

        if ($driverName) {
            [$contact, $method, $note] = $this->findContactForDriverName($driverName);
            if ($note) $notes[] = $note;

            if ($contact) {
                $drv = $this->db()->table('driver')
                    ->select($driverCols)
                    ->where('id_contact', $contact->id_contact)
                    ->first();

                if ($drv) {
                    $nameDriver = [
                        'method' => $method,
                        'id_driver' => (int)$drv->id_driver,
                        'id_contact' => (int)$drv->id_contact,
                        'id_vehicle' => $drv->id_vehicle ? (int)$drv->id_vehicle : null,
                        'id_carrier' => $drv->id_carrier ? (int)$drv->id_carrier : null,
                    ];
                } else {
                    $notes[] = "Contact matched by name but no driver row found for id_contact={$contact->id_contact}.";
                }
            }
        }

        if ($truckNumber) {
            $truckNorm = strtolower($this->normTruck($truckNumber));

            $veh = $this->db()->table('vehicle')
                ->select(['id_vehicle', 'vehicle_number', 'vehicle_name'])
                ->where(function ($q) use ($truckNorm) {
                    $q->whereRaw("LOWER(TRIM(vehicle_number)) = ?", [$truckNorm])
                        ->orWhereRaw("LOWER(TRIM(vehicle_name)) = ?", [$truckNorm]);
                })
                ->first();

            if ($veh) {
                $drv = $this->db()->table('driver')
                    ->select($driverCols)
                    ->where('id_vehicle', $veh->id_vehicle)
                    ->first();

                if ($drv) {
                    $truckDriver = [
                        'method' => 'TRUCK',
                        'id_driver' => (int)$drv->id_driver,
                        'id_contact' => (int)$drv->id_contact,
                        'id_vehicle' => (int)$drv->id_vehicle,
                        'id_carrier' => $drv->id_carrier ? (int)$drv->id_carrier : null,
                    ];
                }
            }
        }

        $resolved = null;
        $status = 'NONE';

        if ($nameDriver && $truckDriver) {
            if ($nameDriver['id_driver'] === $truckDriver['id_driver']) {
                $status = 'CONFIRMED';
                $resolved = $nameDriver;
                $resolved['method'] = $nameDriver['method'] . '+TRUCK';
            } else {
                $status = 'CONFLICT';
                $resolved = $nameDriver;
                $notes[] = "Conflict: name matched driver {$nameDriver['id_driver']} but truck matched driver {$truckDriver['id_driver']}.";
            }
        } elseif ($nameDriver) {
            $status = 'NAME_ONLY';
            $resolved = $nameDriver;
        } elseif ($truckDriver) {
            $status = 'TRUCK_ONLY';
            $resolved = $truckDriver;
        }

        return [
            'status' => $status,
            'resolved' => $resolved,
            'by_name' => $nameDriver,
            'by_truck' => $truckDriver,
            'notes' => implode(' ', array_filter($notes)),
        ];
    }

    private function findContactForDriverName(string $driverName): array
    {
        $normFull = $this->norm($driverName);

        $contact = $this->db()->table('contact')
            ->select(['id_contact', 'first_name', 'last_name'])
            ->whereRaw("LOWER(TRIM(CONCAT(TRIM(first_name), ' ', TRIM(last_name)))) = ?", [$normFull])
            ->first();

        if ($contact) return [$contact, 'NAME_EXACT', ''];

        $likeCands = $this->db()->table('contact')
            ->select(['id_contact', 'first_name', 'last_name'])
            ->whereRaw("LOWER(TRIM(CONCAT(TRIM(first_name), ' ', TRIM(last_name)))) LIKE ?", ['%' . $normFull . '%'])
            ->limit(20)
            ->get();

        if (count($likeCands) === 1) return [$likeCands[0], 'NAME_LIKE_UNIQUE', ''];

        [$first, $last] = $this->splitName($driverName);
        $firstN = $first ? $this->norm($first) : '';
        $lastN  = $last ? $this->norm($last) : '';

        if ($firstN === '' && $lastN === '') return [null, 'NAME_NONE', 'Driver name is empty after normalization.'];

        $qb = $this->db()->table('contact')->select(['id_contact', 'first_name', 'last_name']);
        if ($lastN !== '') $qb->whereRaw("SOUNDEX(TRIM(last_name)) = SOUNDEX(?)", [$lastN]);
        else $qb->whereRaw("SOUNDEX(TRIM(first_name)) = SOUNDEX(?)", [$firstN]);

        $pool = $qb->limit(80)->get();

        if (count($pool) === 0 && $firstN !== '') {
            $pool = $this->db()->table('contact')
                ->select(['id_contact', 'first_name', 'last_name'])
                ->whereRaw("LOWER(TRIM(first_name)) LIKE ?", [substr($firstN, 0, 4) . '%'])
                ->limit(80)
                ->get();
        }

        if (count($pool) === 0) {
            if (count($likeCands) > 1) return [null, 'NAME_NONE', 'Driver name ambiguous (multiple contacts match).'];
            return [null, 'NAME_NONE', 'No contact match found by exact/like/fuzzy.'];
        }

        $best = null;
        $bestScore = 9999;
        $secondScore = 9999;

        foreach ($pool as $c) {
            $full = trim(($c->first_name ?? '') . ' ' . ($c->last_name ?? ''));
            $score = $this->nameDistance($driverName, $full);

            if ($score < $bestScore) {
                $secondScore = $bestScore;
                $bestScore = $score;
                $best = $c;
            } elseif ($score < $secondScore) {
                $secondScore = $score;
            }
        }

        if ($best && $bestScore <= 2 && ($secondScore - $bestScore) >= 1) {
            return [$best, 'NAME_FUZZY', "Fuzzy name match used (distance={$bestScore})."];
        }

        if (count($likeCands) > 1) return [null, 'NAME_NONE', 'Driver name ambiguous (multiple contacts match).'];

        return [null, 'NAME_NONE', 'No contact match found by exact/like/fuzzy.'];
    }

    private function splitName(string $name): array
    {
        $name = trim($name);
        if ($name === '') return [null, null];

        if (str_contains($name, ',')) {
            [$a, $b] = array_map('trim', explode(',', $name, 2));
            return [$b ?: null, $a ?: null];
        }

        $parts = preg_split('/\s+/u', $name);
        $parts = array_values(array_filter($parts, fn($p) => $p !== ''));

        if (count($parts) === 1) return [$parts[0], null];
        return [$parts[0], $parts[count($parts) - 1]];
    }

    private function nameDistance(string $a, string $b): int
    {
        $aN = $this->squashDoubles($this->norm($a));
        $bN = $this->squashDoubles($this->norm($b));
        return levenshtein($aN, $bN);
    }

    private function squashDoubles(string $s): string
    {
        return preg_replace('/(.)\1+/u', '$1', $s);
    }

    private function matchPullPointByTerminal(?string $terminal): array
    {
        $terminal = $this->strOrNull($terminal);
        if (!$terminal) return ['status' => 'NONE', 'resolved' => null, 'candidates' => [], 'notes' => 'No terminal in import.'];

        $table = $this->pickPullPointTable();

        $t = strtolower(trim($terminal));
        $t = preg_replace('/\s+/u', ' ', $t);
        $t = preg_replace('/\s*-\s*/', '-', $t);

        $row = $this->db()->table($table)
            ->select(['id_pull_point', 'pp_job'])
            ->where('is_deleted', 0)
            ->whereRaw("
                LOWER(
                    REPLACE(
                        REPLACE(TRIM(pp_job), ' - ', '-'),
                        ' -','-'
                    )
                ) = ?
            ", [$t])
            ->first();

        if ($row) {
            return [
                'status' => 'ONE',
                'resolved' => [
                    'id_pull_point' => (int)$row->id_pull_point,
                    'pp_job' => $row->pp_job,
                    'method' => 'NORMALIZED_EXACT',
                ],
                'candidates' => [],
                'notes' => '',
            ];
        }

        $cands = $this->db()->table($table)
            ->select(['id_pull_point', 'pp_job'])
            ->where('is_deleted', 0)
            ->whereRaw('LOWER(TRIM(pp_job)) LIKE ?', ['%' . $t . '%'])
            ->limit(10)
            ->get();

        if (count($cands) === 1) {
            return [
                'status' => 'ONE',
                'resolved' => [
                    'id_pull_point' => (int)$cands[0]->id_pull_point,
                    'pp_job' => $cands[0]->pp_job,
                    'method' => 'LIKE_UNIQUE',
                ],
                'candidates' => [],
                'notes' => '',
            ];
        }

        if (count($cands) > 1) {
            return [
                'status' => 'MULTI',
                'resolved' => null,
                'candidates' => $cands->map(fn($x) => [
                    'id_pull_point' => (int)$x->id_pull_point,
                    'pp_job' => $x->pp_job,
                ])->all(),
                'notes' => 'Multiple pull points match.',
            ];
        }

        return ['status' => 'NONE', 'resolved' => null, 'candidates' => [], 'notes' => 'No pull point match found.'];
    }

    private function matchPadLocationByJobname(?string $jobname): array
    {
        $jobname = $this->strOrNull($jobname);
        if (!$jobname) return ['status' => 'NONE', 'resolved' => null, 'candidates' => [], 'notes' => 'No jobname in import.'];

        $norm = strtolower(trim($jobname));
        $norm = preg_replace('/\s+/u', ' ', $norm);
        $normLoose = preg_replace('/\s*\/\s*/u', '/', $norm);

        $exact = $this->db()->table('pad_location')
            ->select(['id_pad_location', 'pl_job'])
            ->where('is_deleted', 0)
            ->whereRaw("LOWER(TRIM(pl_job)) = ?", [$norm])
            ->first();

        if ($exact) {
            return [
                'status' => 'ONE',
                'resolved' => [
                    'id_pad_location' => (int)$exact->id_pad_location,
                    'pl_job' => $exact->pl_job,
                    'method' => 'EXACT',
                ],
                'candidates' => [],
                'notes' => '',
            ];
        }

        $cands = $this->db()->table('pad_location')
            ->select(['id_pad_location', 'pl_job'])
            ->where('is_deleted', 0)
            ->where(function ($q) use ($norm, $normLoose) {
                $q->whereRaw("LOWER(TRIM(pl_job)) LIKE ?", ['%' . $norm . '%'])
                    ->orWhereRaw("LOWER(TRIM(pl_job)) LIKE ?", ['%' . $normLoose . '%'])
                    ->orWhereRaw("? LIKE CONCAT('%', LOWER(TRIM(pl_job)), '%')", [$norm])
                    ->orWhereRaw("? LIKE CONCAT('%', LOWER(TRIM(pl_job)), '%')", [$normLoose]);
            })
            ->limit(10)
            ->get();

        if (count($cands) === 1) {
            return [
                'status' => 'ONE',
                'resolved' => [
                    'id_pad_location' => (int)$cands[0]->id_pad_location,
                    'pl_job' => $cands[0]->pl_job,
                    'method' => 'LIKE_UNIQUE',
                ],
                'candidates' => [],
                'notes' => '',
            ];
        }

        if (count($cands) > 1) {
            return [
                'status' => 'MULTI',
                'resolved' => null,
                'candidates' => $cands->map(fn($x) => [
                    'id_pad_location' => (int)$x->id_pad_location,
                    'pl_job' => $x->pl_job,
                ])->all(),
                'notes' => 'Jobname ambiguous: multiple pad_location matches.',
            ];
        }

        return ['status' => 'NONE', 'resolved' => null, 'candidates' => [], 'notes' => 'No pad_location match found.'];
    }

    private function buildJourney(array $pullPointMatch, array $padLocationMatch): array
    {
        $pp = $pullPointMatch['resolved'] ?? null;
        $pl = $padLocationMatch['resolved'] ?? null;

        if (!$pp || !$pl) {
            return [
                'status' => ($pp || $pl) ? 'PARTIAL' : 'NONE',
                'pull_point_id' => $pp ? (int)$pp['id_pull_point'] : null,
                'pad_location_id' => $pl ? (int)$pl['id_pad_location'] : null,
                'join_id' => null,
                'miles' => null,
                'method' => ($pp || $pl) ? 'PARTIAL' : 'NONE',
            ];
        }

        $ppId = (int)$pp['id_pull_point'];
        $plId = (int)$pl['id_pad_location'];

        $join = $this->db()->table('join')
            ->select(['id_join', 'miles'])
            ->where('is_deleted', 0)
            ->where('id_pull_point', $ppId)
            ->where('id_pad_location', $plId)
            ->first();

        if ($join) {
            return [
                'status' => 'READY',
                'pull_point_id' => $ppId,
                'pad_location_id' => $plId,
                'join_id' => (int)$join->id_join,
                'miles' => $join->miles !== null ? (int)$join->miles : null,
                'method' => 'JOIN_LOOKUP(pp,pl)',
            ];
        }

        return [
            'status' => 'MISSING_JOIN',
            'pull_point_id' => $ppId,
            'pad_location_id' => $plId,
            'join_id' => null,
            'miles' => null,
            'method' => 'JOIN_NOT_FOUND',
        ];
    }

    private function computeConfidence(array $driverMatch): string
    {
        return match ($driverMatch['status'] ?? 'NONE') {
            'CONFIRMED' => 'GREEN',
            'NAME_ONLY', 'TRUCK_ONLY', 'CONFLICT' => 'YELLOW',
            default => 'RED',
        };
    }

    // ------------------ utilities ------------------

    private function pickPullPointTable(): string
    {
        return $this->tableExists('pull_points') ? 'pull_points' : 'pull_point';
    }

    private function tableExists(string $table): bool
    {
        try {
            return Schema::connection($this->db()->getName())->hasTable($table);
        } catch (\Throwable $e) {
            // fallback
            try {
                return Schema::hasTable($table);
            } catch (\Throwable $e2) {
                return false;
            }
        }
    }

    private function columnExists(string $table, string $column): bool
    {
        try {
            return Schema::connection($this->db()->getName())->hasColumn($table, $column);
        } catch (\Throwable $e) {
            // fallback
            try {
                return Schema::hasColumn($table, $column);
            } catch (\Throwable $e2) {
                return false;
            }
        }
    }

    private function norm(string $s): string
    {
        $s = strtolower(trim($s));
        $s = preg_replace('/[^\p{L}\p{N}\s]+/u', ' ', $s);
        $s = preg_replace('/\s+/u', ' ', $s);
        return trim($s);
    }

    private function normTruck(string $s): string
    {
        $s = trim($s);
        return strtolower(preg_replace('/[^a-zA-Z0-9]+/', '', $s));
    }

    private function strOrNull($v): ?string
    {
        if ($v === null) return null;
        $s = trim((string)$v);
        return $s === '' ? null : $s;
    }

    private function db()
    {
        return DB::connection(); // ✅ default connection
    }
}
