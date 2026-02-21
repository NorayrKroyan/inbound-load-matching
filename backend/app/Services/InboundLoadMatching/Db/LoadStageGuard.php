<?php

namespace App\Services\InboundLoadMatching\Db;

use Illuminate\Support\Facades\DB;
use App\Services\InboundLoadMatching\Support\DbSchema;
use App\Services\InboundLoadMatching\Support\Str;
use App\Services\InboundLoadMatching\Parsing\StageResolver;

class LoadStageGuard
{
    public function __construct(
        private readonly DbSchema $schema,
        private readonly Str $str,
        private readonly StageResolver $stageResolver,
    ) {}

    /**
     * Determines current rank from DB for (join_id + load_number).
     * Uses:
     * - review_date => rank 4
     * - delivery_time/load_delivery_date/delivery_date OR is_finished=1 => rank 3
     * - any weight column >0 => rank 2
     * - otherwise rank 1 (exists)
     * - if no row => 0
     */
    public function inferCurrentStageRankFromDb(int $joinId, string $loadNumber): int
    {
        $loadNumber = $this->str->strOrNull($loadNumber);
        if (!$loadNumber) return 0;

        $ld = DB::connection()
            ->table('load_detail as ld')
            ->join('load as l', 'l.id_load', '=', 'ld.id_load')
            ->select(['ld.id_load_detail', 'ld.id_load'])
            ->where('ld.load_number', $loadNumber)
            ->where('l.id_join', $joinId)
            ->where('l.is_deleted', 0)
            ->orderByDesc('ld.id_load_detail')
            ->first();

        if (!$ld) return 0;

        $idLoad = (int)$ld->id_load;
        $idLoadDetail = (int)$ld->id_load_detail;

        if ($this->schema->columnExists('load_detail', 'review_date')) {
            $rv = DB::connection()->table('load_detail')->where('id_load_detail', $idLoadDetail)->value('review_date');
            if ($rv !== null) return 4;
        }

        $hasDelivery = false;
        foreach (['delivery_time', 'load_delivery_date', 'delivery_date'] as $col) {
            if ($this->schema->columnExists('load', $col)) {
                $v = DB::connection()->table('load')->where('id_load', $idLoad)->value($col);
                if ($v !== null) { $hasDelivery = true; break; }
            }
        }
        if ($hasDelivery) return 3;

        if ($this->schema->columnExists('load', 'is_finished')) {
            $isFinished = DB::connection()->table('load')->where('id_load', $idLoad)->value('is_finished');
            if ((int)($isFinished ?? 0) === 1) return 3;
        }

        $weightCols = [];
        foreach (['net_lbs', 'total_lbs', 'weight_lbs', 'tons'] as $c) {
            if ($this->schema->columnExists('load_detail', $c)) $weightCols[] = $c;
        }
        if (!empty($weightCols)) {
            $vals = DB::connection()->table('load_detail')->where('id_load_detail', $idLoadDetail)->first($weightCols);
            if ($vals) {
                foreach ($weightCols as $c) {
                    $v = $vals->$c ?? null;
                    if ($v !== null && is_numeric($v) && (float)$v > 0) return 2;
                }
            }
        }

        return 1;
    }

    public function findExistingLoadForStage(array $parsed, int $joinId): ?object
    {
        $loadNumber = $this->str->strOrNull($parsed['load_number'] ?? null);
        if (!$loadNumber) return null;

        return DB::connection()
            ->table('load_detail as ld')
            ->join('load as l', 'l.id_load', '=', 'ld.id_load')
            ->select(['ld.id_load_detail', 'ld.id_load'])
            ->where('ld.load_number', $loadNumber)
            ->where('l.id_join', $joinId)
            ->where('l.is_deleted', 0)
            ->orderByDesc('ld.id_load_detail')
            ->first();
    }
}
