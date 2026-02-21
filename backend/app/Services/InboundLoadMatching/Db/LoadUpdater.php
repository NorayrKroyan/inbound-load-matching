<?php

namespace App\Services\InboundLoadMatching\Db;

use Illuminate\Support\Facades\DB;
use App\Services\InboundLoadMatching\Support\DbSchema;
use App\Services\InboundLoadMatching\Parsing\BolExtractor;

class LoadUpdater
{
    public function __construct(
        private readonly DbSchema $schema,
        private readonly BoxesNoteBuilder $boxesBuilder,
        private readonly BolExtractor $bolExtractor,
    ) {}

    // ------------------ Boxes ------------------

    public function applyBoxesNoteIfMissing(int $idLoadDetail, array &$detailUpd, ?string $boxesNote): void
    {
        if (!$boxesNote) return;
        if (!$this->schema->columnExists('load_detail', 'load_notes')) return;

        $cur = DB::connection()->table('load_detail')
            ->where('id_load_detail', $idLoadDetail)
            ->value('load_notes');

        $merged = $this->boxesBuilder->mergeBoxesIntoNotesIfMissing($cur, $boxesNote);

        if ((string)$merged !== (string)($cur ?? '')) {
            $detailUpd['load_notes'] = $merged;
        }
    }

    // ------------------ Weights ------------------

    public function applyWeightsUpdates(array &$detailUpd, ?float $netLbs, ?float $tons): void
    {
        if ($netLbs !== null && $this->schema->columnExists('load_detail', 'net_lbs')) {
            $detailUpd['net_lbs'] = (int)round($netLbs);
        }
        if ($tons !== null && $this->schema->columnExists('load_detail', 'tons')) {
            $detailUpd['tons'] = $tons;
        }
    }

    // ------------------ Delivery ------------------

    public function applyDeliveryUpdates(array &$loadUpd, ?string $deliveryMysql, bool $finish): void
    {
        if ($deliveryMysql) {
            // doc allows multiple naming schemes
            if ($this->schema->columnExists('load', 'load_delivery_date')) {
                $loadUpd['load_delivery_date'] = $deliveryMysql;
            }
            if ($this->schema->columnExists('load', 'delivery_date')) {
                $loadUpd['delivery_date'] = $deliveryMysql;
            }
            if ($this->schema->columnExists('load', 'delivery_time')) {
                $loadUpd['delivery_time'] = $deliveryMysql;
            }
        }

        if ($finish && $this->schema->columnExists('load', 'is_finished')) {
            $loadUpd['is_finished'] = 1;
        }
    }

    // ------------------ BOL + REPLACEMENT MARKING ------------------

    /**
     * $parsedOrLoadNumber can be:
     *  - FULL parsed array (preferred): ['load_number','jobname','truck_number','terminal',...]
     *  - OR a load_number string (backward compat)
     */
    public function applyBolUpdateIfAllowed(
        int $currentImportId,
            $parsedOrLoadNumber,
        int $idLoadDetail,
        array &$detailUpd,
        array $bol,
        bool $allowed
    ): void {
        if (!$allowed) return;

        $bolPath = $bol['bol_path'] ?? null;
        $bolType = $bol['bol_type'] ?? null;

        if (!is_string($bolPath) || trim($bolPath) === '') return;
        if (!is_string($bolType) || trim($bolType) === '') return;

        if (!$this->schema->columnExists('load_detail', 'bol_path')) return;
        if (!$this->schema->columnExists('load_detail', 'bol_type')) return;

        // 1) set BOL on load_detail
        $detailUpd['bol_path'] = $bolPath;
        $detailUpd['bol_type'] = $bolType;

        // 2) mark OTHER related loadimports rows as REPLACED (current row MUST NOT be modified)
        $parsed = $this->normalizeParsed($parsedOrLoadNumber);
        $this->markOtherImportImagesReplaced($currentImportId, $parsed);
    }

    private function normalizeParsed($parsedOrLoadNumber): array
    {
        if (is_array($parsedOrLoadNumber)) return $parsedOrLoadNumber;

        if (is_string($parsedOrLoadNumber) && trim($parsedOrLoadNumber) !== '') {
            return ['load_number' => trim($parsedOrLoadNumber)];
        }

        return [];
    }

    private function markOtherImportImagesReplaced(int $keepImportId, array $parsed): void
    {
        if (!$this->schema->tableExists('loadimports')) return;

        $hasImagePath = $this->schema->columnExists('loadimports', 'image_path');
        $hasImageOriginal = $this->schema->columnExists('loadimports', 'image_original');
        if (!$hasImagePath && !$hasImageOriginal) return;

        $loadNumber  = $this->s($parsed['load_number'] ?? null);
        if (!$loadNumber) return;

        $jobname     = $this->s($parsed['jobname'] ?? null);
        $truckNumber = $this->s($parsed['truck_number'] ?? null);
        $terminal    = $this->s($parsed['terminal'] ?? null);

        $q = DB::connection()->table('loadimports')
            ->where('id', '<>', $keepImportId);

        // required key (always)
        $this->whereGroupKey($q, 'load_number', '$.loadnumber', $loadNumber);

        // narrowers if present
        if ($jobname)  $this->whereGroupKey($q, 'jobname', '$.jobname', $jobname);

        // ✅ FIX: truck_number can be blank in payload; also match truck_trailer
        if ($truckNumber) $this->whereTruckKey($q, $truckNumber);

        if ($terminal) $this->whereGroupKey($q, 'terminal', '$.terminal', $terminal);

        $select = ['id'];
        if ($hasImagePath) $select[] = 'image_path';
        if ($hasImageOriginal) $select[] = 'image_original';

        $rows = $q->select($select)->get();

        foreach ($rows as $r) {
            $upd = [];

            if ($hasImagePath) {
                $cur = $r->image_path ?? null;
                $new = $this->makeReplacedIfNeeded($cur);
                if ($new !== null && $new !== $cur) $upd['image_path'] = $new;
            }

            if ($hasImageOriginal) {
                $cur = $r->image_original ?? null;
                $new = $this->makeReplacedIfNeeded($cur);
                if ($new !== null && $new !== $cur) $upd['image_original'] = $new;
            }

            if (!empty($upd)) {
                DB::connection()->table('loadimports')
                    ->where('id', (int)$r->id)
                    ->update($upd);
            }
        }
    }

    /**
     * Normal key match:
     * - if column exists: column = value
     * - else: JSON_EXTRACT(payload_json, '$.key') = value
     */
    private function whereGroupKey($query, string $columnName, string $jsonPath, string $value): void
    {
        $value = trim($value);
        if ($value === '') return;

        // If a real column exists, use it.
        if ($this->schema->columnExists('loadimports', $columnName)) {
            $query->where($columnName, $value);
            return;
        }

        // Otherwise group via payload_json.
        if (!$this->schema->columnExists('loadimports', 'payload_json')) return;

        // ✅ SECURITY: only allow known json paths (prevents SQL injection).
        $allowed = ['$.loadnumber', '$.jobname', '$.truck_number', '$.terminal', '$.truck_trailer'];
        if (!in_array($jsonPath, $allowed, true)) return;

        $query->whereRaw(
            "TRIM(JSON_UNQUOTE(JSON_EXTRACT(payload_json, '{$jsonPath}'))) = ?",
            [$value]
        );
    }

    /**
     * ✅ FIX for 741-style rows:
     * - Some payloads have truck_number="" but truck_trailer="Truck #: 2512..."
     * - So we match either:
     *   $.truck_number == 2512
     *   OR $.truck_trailer LIKE %2512%
     */
    private function whereTruckKey($query, string $truckNumber): void
    {
        $truckNumber = trim($truckNumber);
        if ($truckNumber === '') return;

        // If a real column exists, use it.
        if ($this->schema->columnExists('loadimports', 'truck')) {
            $query->where('truck', $truckNumber);
            return;
        }

        if (!$this->schema->columnExists('loadimports', 'payload_json')) return;

        $query->whereRaw(
            "(" .
            "TRIM(JSON_UNQUOTE(JSON_EXTRACT(payload_json, '$.truck_number'))) = ? " .
            "OR " .
            "TRIM(JSON_UNQUOTE(JSON_EXTRACT(payload_json, '$.truck_trailer'))) LIKE ? " .
            ")",
            [$truckNumber, '%' . $truckNumber . '%']
        );
    }

    private function s($v): ?string
    {
        if (!is_string($v)) return null;
        $v = trim($v);
        return $v === '' ? null : $v;
    }

    private function makeReplacedIfNeeded($path): ?string
    {
        if (!is_string($path)) return null;
        $path = trim($path);
        if ($path === '') return null;

        if (preg_match('/_REPLACED\.[A-Za-z0-9]{2,10}(\?.*)?$/', $path)) return $path;
        if (!preg_match('/\.[A-Za-z0-9]{2,10}($|\?)/', $path)) return $path;

        return $this->bolExtractor->buildReplacedName($path);
    }

    // ------------------ Flush ------------------

    public function flushUpdates(int $idLoad, int $idLoadDetail, array $loadUpd, array $detailUpd): void
    {
        if (!empty($loadUpd)) {
            // NOTE: `load` is a reserved keyword in some MariaDB contexts.
            // Laravel will quote it, but if you ever run raw SQL, you must use `load`.
            DB::connection()->table('load')->where('id_load', $idLoad)->update($loadUpd);
        }
        if (!empty($detailUpd)) {
            DB::connection()->table('load_detail')->where('id_load_detail', $idLoadDetail)->update($detailUpd);
        }
    }
}
