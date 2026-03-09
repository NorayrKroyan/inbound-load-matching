<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use App\Services\InboundLoadMatching\InboundLoadMatchingService;
use App\Services\InboundLoadMatching\Support\LoadImportProcessRunType;

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

        $res = $svc->processSingleWithLogging($importId);

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
            if ($id > 0) {
                $clean[] = $id;
            }
        }

        $clean = array_values(array_unique($clean));
        $clean = array_slice($clean, 0, 500);

        if (count($clean) === 0) {
            return response()->json([
                'ok' => false,
                'error' => 'No valid import_ids provided'
            ], 422);
        }

        $res = $svc->processBatchWithLogging($clean);

        return response()->json($res, 200);
    }

    public function autoProcessStatus(InboundLoadMatchingService $svc)
    {
        return response()->json($svc->getAutoProcessStatus());
    }

    public function autoProcessStart(Request $request, InboundLoadMatchingService $svc)
    {
        $intervalMinutes = (int)$request->input('interval_minutes', 5);
        $res = $svc->startAutoProcess($intervalMinutes);

        return response()->json($res, 200);
    }

    public function autoProcessStop(InboundLoadMatchingService $svc)
    {
        $res = $svc->stopAutoProcess();

        return response()->json($res, 200);
    }

    public function logs(Request $request)
    {
        $limit = (int)($request->query('limit', 200));
        $limit = max(1, min($limit, 500));

        $q = trim((string)$request->query('q', ''));
        $runType = trim((string)$request->query('run_type', ''));
        $severity = trim((string)$request->query('severity', ''));
        $status = trim((string)$request->query('status', ''));
        $sessionId = trim((string)$request->query('session_id', ''));

        $query = DB::connection()->table('loadimport_process_log');

        if ($runType !== '') {
            $query->where('run_type', $runType);
        }

        if ($severity !== '') {
            $query->where('severity', $severity);
        }

        if ($status !== '') {
            $query->where('status', $status);
        }

        if ($sessionId !== '') {
            $query->where('session_id', $sessionId);
        }

        if ($q !== '') {
            $like = '%' . $q . '%';

            $query->where(function ($sub) use ($like) {
                $sub->where('session_id', 'like', $like)
                    ->orWhere('run_type', 'like', $like)
                    ->orWhere('event', 'like', $like)
                    ->orWhere('severity', 'like', $like)
                    ->orWhere('status', 'like', $like)
                    ->orWhere('stage_detected', 'like', $like)
                    ->orWhere('message', 'like', $like)
                    ->orWhereRaw('CAST(loadimport_id AS CHAR) LIKE ?', [$like])
                    ->orWhereRaw('CAST(id_load AS CHAR) LIKE ?', [$like])
                    ->orWhereRaw('CAST(id_load_detail AS CHAR) LIKE ?', [$like]);
            });
        }

        $rows = $query
            ->orderByDesc('id')
            ->limit($limit)
            ->get()
            ->map(function ($row) {
                $row->run_type_label = LoadImportProcessRunType::label($row->run_type);
                $row->context_json = $row->context_json
                    ? json_decode($row->context_json, true)
                    : null;

                return $row;
            })
            ->values();

        return response()->json([
            'ok' => true,
            'rows' => $rows,
        ]);
    }
}
