<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use Illuminate\Http\Request;
use App\Services\InboundLoadMatching\InboundLoadMatchingService;

class InboundLoadMatchingController extends Controller
{
    public function queue(Request $request, InboundLoadMatchingService $svc)
    {
        $limit = (int)($request->query('limit', 200));
        $limit = max(1, min($limit, 500));

        $only  = strtolower((string)$request->query('only', 'unprocessed')); // unprocessed|processed|all
        $q     = (string)$request->query('q', '');
        $match = strtoupper((string)$request->query('match', '')); // GREEN|YELLOW|RED

        $rows = $svc->buildQueue($limit, $only, $q, $match);

        return response()->json([
            'ok' => true,
            'count' => count($rows),
            'rows' => $rows,
        ]);
    }

    /**
     * STRICT single processing:
     * - processes ONLY this import_id
     * - refuses invalid transitions (no auto AT_TERMINAL)
     */
    public function process(Request $request, InboundLoadMatchingService $svc)
    {
        $importId = (int)($request->input('import_id') ?? 0);
        if ($importId <= 0) {
            return response()->json(['ok' => false, 'error' => 'import_id is required'], 422);
        }

        // ✅ IMPORTANT: call STRICT method (must exist in service)
        $res = $svc->processImportStrict($importId);

        return response()->json($res, ($res['ok'] ?? false) ? 200 : 422);
    }

    /**
     * STRICT batch:
     * - processes ONLY selected import_ids
     * - enforces forward-only transitions per (join_id + load_number)
     * - returns per-item results (HTTP 200 so UI can show partial failures)
     *
     * Body: { "import_ids": [123,124,125] }
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
        $clean = array_slice($clean, 0, 500);

        if (count($clean) === 0) {
            return response()->json(['ok' => false, 'error' => 'No valid import_ids provided'], 422);
        }

        // ✅ IMPORTANT: call STRICT method (must exist in service)
        $res = $svc->processBatchStrict($clean);

        // Always 200 so frontend can display per-row errors in response.results
        return response()->json($res, 200);
    }
}
