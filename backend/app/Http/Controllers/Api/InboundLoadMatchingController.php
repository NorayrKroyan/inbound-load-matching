<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use Illuminate\Http\Request;
use App\Services\InboundLoadMatchingService;

class InboundLoadMatchingController extends Controller
{
    public function queue(Request $request, InboundLoadMatchingService $svc)
    {
        $limit = (int)($request->query('limit', 200));
        $limit = max(1, min($limit, 500));

        $only = strtolower((string)$request->query('only', 'unprocessed')); // unprocessed|processed|all
        $q = (string)$request->query('q', '');
        $match = strtoupper((string)$request->query('match', '')); // GREEN|YELLOW|RED

        $rows = $svc->buildQueue($limit, $only, $q, $match);

        return response()->json([
            'ok' => true,
            'count' => count($rows),
            'rows' => $rows,
        ]);
    }

    public function process(Request $request, InboundLoadMatchingService $svc)
    {
        $importId = (int)($request->input('import_id') ?? 0);
        if ($importId <= 0) {
            return response()->json(['ok' => false, 'error' => 'import_id is required'], 422);
        }

        $res = $svc->processImport($importId);

        $status = ($res['ok'] ?? false) ? 200 : 422;
        return response()->json($res, $status);
    }

    /**
     * ✅ NEW: process many imports in one call
     * Body: { "import_ids": [123,124,125] }
     * Returns per-id results and counts.
     */
    public function processBatch(Request $request, InboundLoadMatchingService $svc)
    {
        $ids = $request->input('import_ids');

        if (!is_array($ids) || count($ids) === 0) {
            return response()->json(['ok' => false, 'error' => 'import_ids must be a non-empty array'], 422);
        }

        // sanitize + unique + limit
        $clean = [];
        foreach ($ids as $v) {
            $id = (int)$v;
            if ($id > 0) $clean[] = $id;
        }
        $clean = array_values(array_unique($clean));

        if (count($clean) === 0) {
            return response()->json(['ok' => false, 'error' => 'No valid import_ids provided'], 422);
        }

        // safety cap
        $clean = array_slice($clean, 0, 500);

        $results = [];
        $okCount = 0;
        $failCount = 0;
        $alreadyCount = 0;

        foreach ($clean as $id) {
            try {
                $res = $svc->processImport($id);

                $results[] = [
                    'import_id' => $id,
                    'ok' => (bool)($res['ok'] ?? false),
                    'already_processed' => (bool)($res['already_processed'] ?? false),
                    'id_load' => $res['id_load'] ?? null,
                    'id_load_detail' => $res['id_load_detail'] ?? null,
                    'bol_path' => $res['bol_path'] ?? null,
                    'bol_type' => $res['bol_type'] ?? null,
                    'error' => $res['ok'] ? null : ($res['error'] ?? 'Unknown error'),
                    'raw' => $res, // keep full for debugging
                ];

                if (($res['ok'] ?? false) === true) {
                    $okCount++;
                    if (($res['already_processed'] ?? false) === true) $alreadyCount++;
                } else {
                    $failCount++;
                }
            } catch (\Throwable $e) {
                $failCount++;
                $results[] = [
                    'import_id' => $id,
                    'ok' => false,
                    'already_processed' => false,
                    'id_load' => null,
                    'id_load_detail' => null,
                    'bol_path' => null,
                    'bol_type' => null,
                    'error' => $e->getMessage(),
                    'raw' => null,
                ];
            }
        }

        return response()->json([
            'ok' => true,
            'requested' => count($ids),
            'processed' => count($clean),
            'ok_count' => $okCount,
            'fail_count' => $failCount,
            'already_processed_count' => $alreadyCount,
            'results' => $results,
        ]);
    }
}
