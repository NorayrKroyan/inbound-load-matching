<?php

namespace App\Services\InboundLoadMatching\Db;

use Illuminate\Support\Facades\DB;
use App\Services\InboundLoadMatching\Support\DbSchema;

class LoadimportsTracker
{
    public function __construct(private readonly DbSchema $schema) {}

    public function updateTracking(int $importId, int $idLoad, int $idLoadDetail): void
    {
        $upd = [];

        if ($this->schema->columnExists('loadimports', 'is_inserted')) $upd['is_inserted'] = 1;
        if ($this->schema->columnExists('loadimports', 'id_load')) $upd['id_load'] = $idLoad;
        if ($this->schema->columnExists('loadimports', 'id_load_detail')) $upd['id_load_detail'] = $idLoadDetail;
        if ($this->schema->columnExists('loadimports', 'inserted_at')) $upd['inserted_at'] = DB::raw('NOW()');

        if (!empty($upd)) {
            DB::connection()->table('loadimports')->where('id', $importId)->update($upd);
        }
    }

    public function backfillIfMissing(int $importId, int $idLoad, int $idLoadDetail): void
    {
        $hasAny =
            $this->schema->columnExists('loadimports', 'is_inserted') ||
            $this->schema->columnExists('loadimports', 'id_load') ||
            $this->schema->columnExists('loadimports', 'id_load_detail') ||
            $this->schema->columnExists('loadimports', 'inserted_at');

        if (!$hasAny) return;

        $row = DB::connection()->table('loadimports')->where('id', $importId)->first();
        if (!$row) return;

        $upd = [];

        if ($this->schema->columnExists('loadimports', 'is_inserted')) {
            $cur = property_exists($row, 'is_inserted') ? (int)($row->is_inserted ?? 0) : 0;
            if ($cur !== 1) $upd['is_inserted'] = 1;
        }

        if ($this->schema->columnExists('loadimports', 'id_load')) {
            $cur = property_exists($row, 'id_load') ? $row->id_load : null;
            if ($cur === null) $upd['id_load'] = $idLoad;
        }

        if ($this->schema->columnExists('loadimports', 'id_load_detail')) {
            $cur = property_exists($row, 'id_load_detail') ? $row->id_load_detail : null;
            if ($cur === null) $upd['id_load_detail'] = $idLoadDetail;
        }

        if ($this->schema->columnExists('loadimports', 'inserted_at')) {
            $cur = property_exists($row, 'inserted_at') ? $row->inserted_at : null;
            if ($cur === null) $upd['inserted_at'] = DB::raw('NOW()');
        }

        if (!empty($upd)) {
            DB::connection()->table('loadimports')->where('id', $importId)->update($upd);
        }
    }
}
