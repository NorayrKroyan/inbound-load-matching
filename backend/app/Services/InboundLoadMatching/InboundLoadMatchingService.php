<?php

namespace App\Services\InboundLoadMatching;

use Illuminate\Support\Facades\DB;
use App\Services\InboundLoadMatching\Support\DbSchema;
use App\Services\InboundLoadMatching\Support\Str;

use App\Services\InboundLoadMatching\Parsing\ImportRowParser;
use App\Services\InboundLoadMatching\Parsing\StageResolver;
use App\Services\InboundLoadMatching\Parsing\WeightExtractor;
use App\Services\InboundLoadMatching\Parsing\BolExtractor;
use App\Services\InboundLoadMatching\Parsing\DateResolver;

use App\Services\InboundLoadMatching\Matching\DriverMatcher;
use App\Services\InboundLoadMatching\Matching\LocationMatcher;
use App\Services\InboundLoadMatching\Matching\JourneyResolver;

use App\Services\InboundLoadMatching\Db\BoxesNoteBuilder;
use App\Services\InboundLoadMatching\Db\LoadimportsTracker;
use App\Services\InboundLoadMatching\Db\LoadStageGuard;
use App\Services\InboundLoadMatching\Db\LoadUpdater;

class InboundLoadMatchingService
{
    public function __construct()
    {
        $this->str = new Str();
        $this->schema = new DbSchema();

        $this->stageResolver = new StageResolver($this->str);
        $this->parser = new ImportRowParser($this->str);
        $this->weights = new WeightExtractor();
        $this->bolExtractor = new BolExtractor($this->str);
        $this->dateResolver = new DateResolver($this->str);

        $this->driverMatcher = new DriverMatcher($this->str);
        $this->locationMatcher = new LocationMatcher($this->schema, $this->str);
        $this->journeyResolver = new JourneyResolver();

        $this->boxesBuilder = new BoxesNoteBuilder();
        $this->tracker = new LoadimportsTracker($this->schema);
        $this->stageGuard = new LoadStageGuard($this->schema, $this->str, $this->stageResolver);

        // IMPORTANT: LoadUpdater supports marking old loadimports as *_REPLACED.*
        $this->updater = new LoadUpdater($this->schema, $this->boxesBuilder, $this->bolExtractor);
    }

    private DbSchema $schema;
    private Str $str;

    private StageResolver $stageResolver;
    private ImportRowParser $parser;
    private WeightExtractor $weights;
    private BolExtractor $bolExtractor;
    private DateResolver $dateResolver;

    private DriverMatcher $driverMatcher;
    private LocationMatcher $locationMatcher;
    private JourneyResolver $journeyResolver;

    private BoxesNoteBuilder $boxesBuilder;
    private LoadimportsTracker $tracker;
    private LoadStageGuard $stageGuard;
    private LoadUpdater $updater;

    // ------------------ PUBLIC: queue ------------------

    public function buildQueue(int $limit, string $only, string $q, string $matchFilter): array
    {
        $select = ['id', 'jobname', 'payload_json', 'payload_original', 'created_at', 'updated_at'];

        // IMPORTANT: your DB may not have these columns, so we only include them if they exist
        foreach (['carrier', 'truck', 'terminal', 'state', 'delivery_time', 'load_number', 'ticket_number'] as $col) {
            if ($this->schema->columnExists('loadimports', $col)) $select[] = $col;
        }

        foreach (['is_inserted', 'id_load', 'id_load_detail', 'inserted_at'] as $col) {
            if ($this->schema->columnExists('loadimports', $col)) $select[] = $col;
        }

        $imports = DB::connection()->table('loadimports')
            ->select(array_values(array_unique($select)))
            ->orderByDesc('id')
            ->limit($limit)
            ->get();

        $out = [];

        foreach ($imports as $r) {
            $parsed = $this->parser->parseImportRow($r);

            $processedDetail = DB::connection()
                ->table('load_detail')
                ->select(['id_load_detail', 'id_load'])
                ->where('input_method', 'IMPORT')
                ->where('input_id', (int)$r->id)
                ->first();

            if ($processedDetail) {
                $this->tracker->backfillIfMissing((int)$r->id, (int)$processedDetail->id_load, (int)$processedDetail->id_load_detail);
            }

            $liIsInserted = property_exists($r, 'is_inserted') ? (int)($r->is_inserted ?? 0) : 0;
            $liIdLoad = property_exists($r, 'id_load') ? ($r->id_load !== null ? (int)$r->id_load : null) : null;

            $isProcessed = ($processedDetail ? true : false) || ($liIsInserted === 1) || ($liIdLoad !== null);

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
                    ($parsed['raw_original'] ?? '') . ' ' .
                    ($r->payload_json ?? '')
                );
                if (strpos($hay, strtolower($q)) === false) continue;
            }

            $driverMatch = $this->driverMatcher->matchDriver($parsed['driver_name'], $parsed['truck_number']);
            $confidence = $this->driverMatcher->computeConfidence($driverMatch);
            if ($matchFilter !== '' && $confidence !== $matchFilter) continue;

            $pullPointMatch = $this->locationMatcher->matchPullPointByTerminal($parsed['terminal']);
            $padLocationMatch = $this->locationMatcher->matchPadLocationByJobname($parsed['jobname']);
            $journey = $this->journeyResolver->buildJourney($pullPointMatch, $padLocationMatch);

            $stage = $this->stageResolver->determineStage($r->payload_json ?? null, $parsed['state'] ?? null);

            $weights = $this->weights->extractWeightsFromPayload($r->payload_json ?? null);
            $boxesNote = $this->boxesBuilder->buildBoxesNote($weights['box1'] ?? null, $weights['box2'] ?? null);

            $netLbs = $weights['net_lbs'];
            $tons = ($netLbs !== null) ? round(((float)$netLbs) / 2000.0, 2) : null;

            if ($stage === StageResolver::STAGE_AT_TERMINAL) {
                $netLbs = null;
                $tons = null;
            }

            $bol = $this->bolExtractor->extractBolFromImportRow($r);

            $out[] = [
                'import_id' => (int)$r->id,
                'created_at' => $r->created_at ?? null,
                'updated_at' => $r->updated_at ?? null,
                'stage' => $stage,

                'driver_name' => $parsed['driver_name'],
                'truck_number' => $parsed['truck_number'],
                'trailer_number' => $parsed['trailer_number'],
                'jobname' => $parsed['jobname'],
                'terminal' => $parsed['terminal'],
                'load_number' => $parsed['load_number'],
                'ticket_number' => $parsed['ticket_number'],
                'state' => $parsed['state'],
                'delivery_time' => $parsed['delivery_time'],

                'boxes_note' => $boxesNote,

                'net_lbs' => ($netLbs !== null) ? (int)round($netLbs) : null,
                'tons' => $tons,
                'miles' => ($journey['miles'] ?? null),

                'bol_path' => $bol['bol_path'],
                'bol_type' => $bol['bol_type'],

                'raw_carrier' => $parsed['raw_carrier'],
                'raw_truck' => $parsed['raw_truck'],
                'raw_original' => $parsed['raw_original'],

                'is_processed' => $isProcessed,
                'processed_load_id' => $processedDetail?->id_load
                    ? (int)$processedDetail->id_load
                    : (property_exists($r, 'id_load') ? ($r->id_load !== null ? (int)$r->id_load : null) : null),
                'processed_load_detail_id' => $processedDetail?->id_load_detail
                    ? (int)$processedDetail->id_load_detail
                    : (property_exists($r, 'id_load_detail') ? ($r->id_load_detail !== null ? (int)$r->id_load_detail : null) : null),

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

    // ============================================================
    // STRICT SINGLE PROCESSING (NEXT-STAGE ONLY)
    // ============================================================

    public function processImportStrict(int $importId): array
    {
        $row = DB::connection()->table('loadimports')->where('id', $importId)->first();
        if (!$row) return ['ok' => false, 'error' => "Import id={$importId} not found."];

        $parsed = $this->parser->parseImportRow($row);

        $pp = $this->locationMatcher->matchPullPointByTerminal($parsed['terminal'] ?? null);
        $pl = $this->locationMatcher->matchPadLocationByJobname($parsed['jobname'] ?? null);
        $journey = $this->journeyResolver->buildJourney($pp, $pl);

        $joinId = isset($journey['join_id']) && $journey['join_id'] ? (int)$journey['join_id'] : null;
        if (!$joinId || (($journey['status'] ?? '') !== 'READY')) {
            return ['ok' => false, 'error' => 'Journey is not READY / join_id missing. Cannot process.'];
        }

        $loadNumber = $this->str->strOrNull($parsed['load_number'] ?? null);
        if (!$loadNumber) return ['ok' => false, 'error' => 'Missing load_number. Cannot process.'];

        $detectedStage = $this->stageResolver->determineStage($row->payload_json ?? null, $parsed['state'] ?? null);
        $desiredRank = $this->stageResolver->stageRank($detectedStage);

        $currentRank = $this->stageGuard->inferCurrentStageRankFromDb($joinId, $loadNumber);

        if ($currentRank === 0) {
            if ($desiredRank !== 1) {
                return [
                    'ok' => false,
                    'stage' => $detectedStage,
                    'error' => "No existing load in DB for join_id={$joinId} load_number={$loadNumber}. You must process AT_TERMINAL first.",
                    'current_rank' => $currentRank,
                    'desired_rank' => $desiredRank,
                ];
            }
        } else {
            if ($desiredRank <= $currentRank) {
                return [
                    'ok' => false,
                    'stage' => $detectedStage,
                    'error' => "Selected stage {$detectedStage} is not allowed because current stage is {$this->stageResolver->rankToStage(max(1, $currentRank))}. You can only process the NEXT stage.",
                    'current_rank' => $currentRank,
                    'desired_rank' => $desiredRank,
                ];
            }

            $expectedNext = $currentRank + 1;
            if ($desiredRank !== $expectedNext) {
                return [
                    'ok' => false,
                    'stage' => $detectedStage,
                    'error' => "Invalid jump. Current stage is {$this->stageResolver->rankToStage($currentRank)}. Next allowed is {$this->stageResolver->rankToStage($expectedNext)}. You selected {$detectedStage}.",
                    'current_rank' => $currentRank,
                    'desired_rank' => $desiredRank,
                ];
            }
        }

        return $this->processImport($importId, $detectedStage);
    }

    // ============================================================
    // Non-strict processing (but follows doc write-rules)
    // ============================================================

    public function processImport(int $importId, ?string $forceStage = null): array
    {
        $row = DB::connection()->table('loadimports')->where('id', $importId)->first();
        if (!$row) return ['ok' => false, 'error' => "Import id={$importId} not found."];

        $parsed = $this->parser->parseImportRow($row);

        $driverMatch = $this->driverMatcher->matchDriver($parsed['driver_name'], $parsed['truck_number']);
        $resolvedDriver = $driverMatch['resolved'] ?? null;
        if (!$resolvedDriver) return ['ok' => false, 'error' => 'No driver resolved. Cannot process.'];

        $idCarrier = $resolvedDriver['id_carrier'] ? (int)$resolvedDriver['id_carrier'] : null;
        if (!$idCarrier) return ['ok' => false, 'error' => 'Resolved driver has no id_carrier. Cannot process.'];

        $pullPointMatch = $this->locationMatcher->matchPullPointByTerminal($parsed['terminal']);
        $padLocationMatch = $this->locationMatcher->matchPadLocationByJobname($parsed['jobname']);
        $journey = $this->journeyResolver->buildJourney($pullPointMatch, $padLocationMatch);

        if (($journey['status'] ?? 'NONE') !== 'READY') return ['ok' => false, 'error' => 'Journey is not READY. Cannot process.'];

        $joinId = $journey['join_id'] ? (int)$journey['join_id'] : null;
        if (!$joinId) return ['ok' => false, 'error' => 'Journey READY but join_id missing. Cannot process.'];

        $miles = isset($journey['miles']) && $journey['miles'] !== null ? (int)$journey['miles'] : null;

        $loadDate = $this->dateResolver->guessLoadDateFromAtTerminal($row->payload_json ?? null, $row->created_at ?? null);

        $weights = $this->weights->extractWeightsFromPayload($row->payload_json ?? null);
        $netLbs  = $weights['net_lbs'];
        $tons    = ($netLbs !== null) ? round(((float)$netLbs) / 2000.0, 2) : null;

        $boxesNote = $this->boxesBuilder->buildBoxesNote($weights['box1'] ?? null, $weights['box2'] ?? null);

        $bol = $this->bolExtractor->extractBolFromImportRow($row);

        $stage = $forceStage ?: $this->stageResolver->determineStage($row->payload_json ?? null, $parsed['state'] ?? null);

        // ✅ FIX: delivery datetime must prefer payload datetime_delivered/datetime_at_destination for delivered stages
        $deliveryMysql = $this->resolveDeliveryMysqlForStage($stage, $row->payload_json ?? null, $parsed['delivery_time'] ?? null);

        $existing = DB::connection()
            ->table('load_detail')
            ->select(['id_load_detail', 'id_load'])
            ->where('input_method', 'IMPORT')
            ->where('input_id', $importId)
            ->first();

        $existingTarget = null;
        if ($stage !== StageResolver::STAGE_AT_TERMINAL) {
            $existingTarget = $this->stageGuard->findExistingLoadForStage($parsed, $joinId);
        }

        $idContact = (int)$resolvedDriver['id_contact'];
        $idVehicle = $resolvedDriver['id_vehicle'] ? (int)$resolvedDriver['id_vehicle'] : null;

        return DB::connection()->transaction(function () use (
            $importId,
            $parsed,
            $stage,
            $existing,
            $existingTarget,
            $idCarrier,
            $idContact,
            $idVehicle,
            $joinId,
            $miles,
            $loadDate,
            $netLbs,
            $tons,
            $bol,
            $boxesNote,
            $deliveryMysql,
            $row
        ) {
            // STAGE 1: AT_TERMINAL
            if ($stage === StageResolver::STAGE_AT_TERMINAL) {
                if ($existing) {
                    $this->tracker->backfillIfMissing($importId, (int)$existing->id_load, (int)$existing->id_load_detail);
                    return [
                        'ok' => true,
                        'stage' => $stage,
                        'already_exists' => true,
                        'id_load' => (int)$existing->id_load,
                        'id_load_detail' => (int)$existing->id_load_detail,
                    ];
                }

                $idLoad = (int)DB::connection()->table('load')->insertGetId([
                    'id_carrier' => $idCarrier,
                    'id_join' => $joinId,
                    'id_contact' => $idContact,
                    'id_vehicle' => $idVehicle,
                    'is_deleted' => 0,
                    'load_date' => $loadDate,
                    'is_finished' => null,
                ]);

                $detailInsert = [
                    'id_load' => $idLoad,
                    'input_method' => 'IMPORT',
                    'input_id' => $importId,
                    'load_number' => $parsed['load_number'] ?? null,
                    'truck_number' => $parsed['truck_number'] ?? null,
                    'trailer_number' => $parsed['trailer_number'] ?? null,
                    'miles' => $miles,
                ];

                if ($boxesNote && $this->schema->columnExists('load_detail', 'load_notes')) {
                    $detailInsert['load_notes'] = $boxesNote;
                }

                $idLoadDetail = (int)DB::connection()->table('load_detail')->insertGetId($detailInsert);

                $this->tracker->updateTracking($importId, $idLoad, $idLoadDetail);

                return [
                    'ok' => true,
                    'stage' => $stage,
                    'already_exists' => false,
                    'id_load' => $idLoad,
                    'id_load_detail' => $idLoadDetail,
                ];
            }

            // stages 2-4 require base load
            if (!$existingTarget) {
                return [
                    'ok' => false,
                    'stage' => $stage,
                    'error' => "Stage {$stage} requires an existing load for load_number=" . ($parsed['load_number'] ?? '(null)') . " and join_id={$joinId}. Process AT TERMINAL first.",
                ];
            }

            $idLoad = (int)$existingTarget->id_load;
            $idLoadDetail = (int)$existingTarget->id_load_detail;

            $detailUpd = [];
            $loadUpd = [];

            $this->updater->applyBoxesNoteIfMissing($idLoadDetail, $detailUpd, $boxesNote);

            if ($this->schema->columnExists('load_detail', 'ticket_number') && ($parsed['ticket_number'] ?? null)) {
                $detailUpd['ticket_number'] = $parsed['ticket_number'];
            }

            $this->updater->applyWeightsUpdates($detailUpd, $netLbs, $tons);

            if ($stage === StageResolver::STAGE_IN_TRANSIT) {
                // ✅ BOL allowed here
                $this->updater->applyBolUpdateIfAllowed(
                    $importId,
                    $parsed,
                    $idLoadDetail,
                    $detailUpd,
                    $bol,
                    true
                );

                $this->updater->flushUpdates($idLoad, $idLoadDetail, $loadUpd, $detailUpd);
                $this->tracker->updateTracking($importId, $idLoad, $idLoadDetail);

                return [
                    'ok' => true,
                    'stage' => $stage,
                    'id_load' => $idLoad,
                    'id_load_detail' => $idLoadDetail,
                    'updated' => [
                        'load' => array_keys($loadUpd),
                        'load_detail' => array_keys($detailUpd),
                    ],
                ];
            }

            if ($stage === StageResolver::STAGE_DELIVERED_PENDING) {
                $this->updater->applyDeliveryUpdates($loadUpd, $deliveryMysql, true);

                // ❌ BOL NOT allowed here
                $this->updater->applyBolUpdateIfAllowed(
                    $importId,
                    $parsed,
                    $idLoadDetail,
                    $detailUpd,
                    $bol,
                    false
                );

                $this->updater->flushUpdates($idLoad, $idLoadDetail, $loadUpd, $detailUpd);
                $this->tracker->updateTracking($importId, $idLoad, $idLoadDetail);

                return [
                    'ok' => true,
                    'stage' => $stage,
                    'id_load' => $idLoad,
                    'id_load_detail' => $idLoadDetail,
                    'updated' => [
                        'load' => array_keys($loadUpd),
                        'load_detail' => array_keys($detailUpd),
                    ],
                ];
            }

            if ($stage === StageResolver::STAGE_DELIVERED_CONFIRMED) {
                $this->updater->applyDeliveryUpdates($loadUpd, $deliveryMysql, true);

                // ✅ BOL allowed here (and triggers REPLACED on prior imports)
                $this->updater->applyBolUpdateIfAllowed(
                    $importId,
                    $parsed,
                    $idLoadDetail,
                    $detailUpd,
                    $bol,
                    true
                );

                $reviewMysql = $this->extractReviewDate($row->payload_json ?? null);
                if ($reviewMysql && $this->schema->columnExists('load_detail', 'review_date')) {
                    $detailUpd['review_date'] = $reviewMysql;
                }

                $this->updater->flushUpdates($idLoad, $idLoadDetail, $loadUpd, $detailUpd);
                $this->tracker->updateTracking($importId, $idLoad, $idLoadDetail);

                return [
                    'ok' => true,
                    'stage' => $stage,
                    'id_load' => $idLoad,
                    'id_load_detail' => $idLoadDetail,
                    'updated' => [
                        'load' => array_keys($loadUpd),
                        'load_detail' => array_keys($detailUpd),
                    ],
                ];
            }

            return ['ok' => false, 'stage' => $stage, 'error' => "Unknown stage: {$stage}"];
        });
    }

    // ============================================================
    // STRICT batch (RESTORED: group + forward-only)
    // ============================================================

    public function processBatchStrict(array $importIds): array
    {
        $ids = [];
        foreach ($importIds as $v) {
            $id = (int)$v;
            if ($id > 0) $ids[] = $id;
        }
        $ids = array_values(array_unique($ids));
        if (count($ids) === 0) return ['ok' => false, 'error' => 'import_ids must contain at least one valid id'];
        $ids = array_slice($ids, 0, 500);

        $rows = DB::connection()->table('loadimports')->whereIn('id', $ids)->get();

        $groups = [];
        $orphans = [];

        foreach ($rows as $r) {
            $parsed = $this->parser->parseImportRow($r);

            $pp = $this->locationMatcher->matchPullPointByTerminal($parsed['terminal'] ?? null);
            $pl = $this->locationMatcher->matchPadLocationByJobname($parsed['jobname'] ?? null);
            $journey = $this->journeyResolver->buildJourney($pp, $pl);

            $joinId = isset($journey['join_id']) && $journey['join_id'] ? (int)$journey['join_id'] : null;
            if (!$joinId) {
                $orphans[] = ['import_id' => (int)$r->id, 'ok' => false, 'error' => 'Journey not READY / join_id missing'];
                continue;
            }

            $loadNumber = $this->str->strOrNull($parsed['load_number'] ?? null);
            if (!$loadNumber) {
                $orphans[] = ['import_id' => (int)$r->id, 'ok' => false, 'error' => 'Missing load_number; cannot group/sequence'];
                continue;
            }

            $stage = $this->stageResolver->determineStage($r->payload_json ?? null, $parsed['state'] ?? null);
            $rank = $this->stageResolver->stageRank($stage);

            $groupKey = $joinId . '|' . $loadNumber;

            if (!isset($groups[$groupKey])) {
                $groups[$groupKey] = [
                    'join_id' => $joinId,
                    'load_number' => $loadNumber,
                    'selected' => [],
                ];
            }

            // pick earliest id for each rank
            if (!isset($groups[$groupKey]['selected'][$rank])) {
                $groups[$groupKey]['selected'][$rank] = (int)$r->id;
            } else {
                $cur = (int)$groups[$groupKey]['selected'][$rank];
                if ((int)$r->id < $cur) $groups[$groupKey]['selected'][$rank] = (int)$r->id;
            }
        }

        $results = [];
        $okGroups = 0;
        $failGroups = 0;

        foreach ($groups as $gk => $g) {
            $joinId = (int)$g['join_id'];
            $loadNumber = (string)$g['load_number'];
            $selected = $g['selected'];

            $currentRank = $this->stageGuard->inferCurrentStageRankFromDb($joinId, $loadNumber);

            $selectedRanks = array_keys($selected);
            sort($selectedRanks);

            $trace = [
                'group_key' => $gk,
                'join_id' => $joinId,
                'load_number' => $loadNumber,
                'current_rank' => $currentRank,
                'selected_ranks' => $selectedRanks,
                'steps' => [],
            ];

            if ($currentRank === 0) {
                if (!in_array(1, $selectedRanks, true)) {
                    $trace['ok'] = false;
                    $trace['steps'][] = [
                        'ok' => false,
                        'error' => 'No existing AT_TERMINAL in DB. You must select an AT_TERMINAL import first.',
                    ];
                    $results[] = $trace;
                    $failGroups++;
                    continue;
                }
            }

            $groupOk = true;

            foreach ($selectedRanks as $rank) {
                if ($rank <= $currentRank) {
                    $groupOk = false;
                    $trace['steps'][] = [
                        'ok' => false,
                        'rank' => $rank,
                        'stage' => $this->stageResolver->rankToStage($rank),
                        'error' => "Selected stage {$this->stageResolver->rankToStage($rank)} is not allowed because current stage is {$this->stageResolver->rankToStage(max(1, $currentRank))} (rank={$currentRank}). You can only process the NEXT stage.",
                    ];
                    break;
                }

                $expectedNext = $currentRank + 1;
                if ($rank !== $expectedNext) {
                    $groupOk = false;
                    $trace['steps'][] = [
                        'ok' => false,
                        'rank' => $rank,
                        'stage' => $this->stageResolver->rankToStage($rank),
                        'error' => "Invalid jump. Current rank={$currentRank}. Next allowed is {$this->stageResolver->rankToStage($expectedNext)} (rank={$expectedNext}). You selected {$this->stageResolver->rankToStage($rank)} (rank={$rank}).",
                    ];
                    break;
                }

                $importId = (int)($selected[$rank] ?? 0);
                if ($importId <= 0) {
                    $groupOk = false;
                    $trace['steps'][] = [
                        'ok' => false,
                        'rank' => $rank,
                        'stage' => $this->stageResolver->rankToStage($rank),
                        'error' => "Missing import for required stage {$this->stageResolver->rankToStage($rank)}.",
                    ];
                    break;
                }

                $stage = $this->stageResolver->rankToStage($rank);

                $res = $this->processImport($importId, $stage);

                $trace['steps'][] = [
                    'ok' => (bool)($res['ok'] ?? false),
                    'rank' => $rank,
                    'stage' => $stage,
                    'import_id_used' => $importId,
                    'result' => $res,
                ];

                if (!($res['ok'] ?? false)) {
                    $groupOk = false;
                    break;
                }

                $currentRank = $rank;
            }

            $trace['ok'] = $groupOk;
            if ($groupOk) $okGroups++; else $failGroups++;
            $results[] = $trace;
        }

        foreach ($orphans as $o) {
            $failGroups++;
            $results[] = ['group_key' => null, 'ok' => false, 'orphan' => $o];
        }

        return [
            'ok' => ($failGroups === 0),
            'groups' => count($groups),
            'ok_groups' => $okGroups,
            'fail_groups' => $failGroups,
            'results' => $results,
        ];
    }

    public function processBatch(array $importIds): array
    {
        return $this->processBatchStrict($importIds);
    }

    private function extractReviewDate(?string $payloadJson): ?string
    {
        if (!$payloadJson || !is_string($payloadJson)) return null;

        $d = json_decode($payloadJson, true);
        if (!is_array($d)) return null;

        $review = $this->str->strOrNull($d['review_date'] ?? null);
        if (!$review) return null;

        return $this->dateResolver->toMysqlDatetime($review);
    }

    /**
     * ✅ FIX: delivered datetime must be based on payload datetime_delivered/datetime_at_destination
     * so load.load_delivery_date is actually saved.
     */
    private function resolveDeliveryMysqlForStage(string $stage, ?string $payloadJson, ?string $fallbackDeliveryTime): ?string
    {
        // Delivered stages: prefer payload timestamps
        if (in_array($stage, [StageResolver::STAGE_DELIVERED_PENDING, StageResolver::STAGE_DELIVERED_CONFIRMED], true)) {
            $best = $this->extractFirstNonEmptyFromPayload($payloadJson, [
                'datetime_delivered',
                'datetime_at_destination',
                'delivery_time',
            ]);

            if ($best) return $this->dateResolver->toMysqlDatetime($best);
        }

        // fallback: original resolver behavior
        $deliveryStr = $this->dateResolver->resolveDeliveryTimeStringFromPayload($payloadJson, $fallbackDeliveryTime);
        return $deliveryStr ? $this->dateResolver->toMysqlDatetime($deliveryStr) : null;
    }

    private function extractFirstNonEmptyFromPayload(?string $payloadJson, array $keys): ?string
    {
        if (!$payloadJson || !is_string($payloadJson)) return null;
        $d = json_decode($payloadJson, true);
        if (!is_array($d)) return null;

        foreach ($keys as $k) {
            $v = $this->str->strOrNull($d[$k] ?? null);
            if ($v) return $v;
        }
        return null;
    }
}
