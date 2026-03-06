<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use App\Services\InboundLoadMatching\InboundLoadMatchingService;
use App\Services\InboundLoadMatching\Support\Str;
use App\Services\InboundLoadMatching\Parsing\StageResolver;

class InboundLoadMatchingController extends Controller
{
    public function queue(Request $request, InboundLoadMatchingService $svc)
    {
        $limit = (int)($request->query('limit', 200));
        $limit = max(1, min($limit, 500));

        $only  = strtolower((string)$request->query('only', 'unprocessed'));
        $q     = (string)$request->query('q', '');
        $match = strtoupper((string)$request->query('match', ''));

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
            return response()->json([
                'ok' => false,
                'error' => 'import_id is required'
            ], 422);
        }

        $row = DB::connection()
            ->table('loadimports')
            ->where('id', $importId)
            ->first();

        if (!$row) {
            return response()->json([
                'ok' => false,
                'error' => 'Import not found'
            ], 404);
        }

        $str = new Str();
        $stageResolver = new StageResolver($str);

        $payload = $row->payload_json ?? null;
        $state   = $row->state ?? null;

        $stage = $stageResolver->determineStage($payload, $state);

        $isInserted = property_exists($row, 'is_inserted')
            ? (int)($row->is_inserted ?? 0)
            : 0;

        if ($stage === StageResolver::STAGE_DELIVERED_CONFIRMED && $isInserted === 0) {
            $res = $svc->processImport($importId);
        } else {
            $res = $svc->processImportStrict($importId);
        }

        return response()->json($res, ($res['ok'] ?? false) ? 200 : 422);
    }

    public function processBatch(Request $request, InboundLoadMatchingService $svc)
    {
        $ids = $request->input('import_ids');

        if (!is_array($ids) || count($ids) === 0) {
            return response()->json([
                'ok' => false,
                'error' => 'import_ids must be a non-empty array'
            ], 422);
        }

        $clean = [];

        foreach ($ids as $v) {
            $id = (int)$v;
            if ($id > 0) $clean[] = $id;
        }

        $clean = array_values(array_unique($clean));
        $clean = array_slice($clean, 0, 500);

        if (count($clean) === 0) {
            return response()->json([
                'ok' => false,
                'error' => 'No valid import_ids provided'
            ], 422);
        }

        $res = $svc->processBatchStrict($clean);

        return response()->json($res, 200);
    }
}
