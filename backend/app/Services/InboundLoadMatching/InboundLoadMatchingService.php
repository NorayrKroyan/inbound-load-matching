<?php

namespace App\Services\InboundLoadMatching;

use Throwable;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use App\Services\InboundLoadMatching\Support\DbSchema;
use App\Services\InboundLoadMatching\Support\Str;
use App\Services\InboundLoadMatching\Support\LoadImportProcessLogger;
use App\Services\InboundLoadMatching\Support\LoadImportProcessRunType;

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

        $this->updater = new LoadUpdater($this->schema, $this->boxesBuilder, $this->bolExtractor);
        $this->processLogger = new LoadImportProcessLogger();
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
    private LoadImportProcessLogger $processLogger;

    public function getAutoProcessStatus(): array
    {
        $row = $this->ensureAutoProcessSettingsRow();

        $lastResult = null;
        if (!empty($row->last_result_json)) {
            $decoded = json_decode($row->last_result_json, true);
            $lastResult = is_array($decoded) ? $decoded : null;
        }

        return [
            'ok' => true,
            'enabled' => (bool)($row->is_enabled ?? 0),
            'interval_minutes' => (int)($row->interval_minutes ?? 5),
            'is_running' => (bool)($row->is_running ?? 0),
            'last_started_at' => $row->last_started_at ?? null,
            'last_finished_at' => $row->last_finished_at ?? null,
            'last_result' => $lastResult,
        ];
    }

    public function startAutoProcess(int $intervalMinutes = 5): array
    {
        $intervalMinutes = max(1, min(60, $intervalMinutes));
        $this->ensureAutoProcessSettingsRow();

        DB::connection()->table('loadimport_autoprocess_settings')
            ->where('id', 1)
            ->update([
                'is_enabled' => 1,
                'interval_minutes' => $intervalMinutes,
                'updated_at' => now(),
            ]);

        return $this->getAutoProcessStatus();
    }

    public function stopAutoProcess(): array
    {
        $this->ensureAutoProcessSettingsRow();

        DB::connection()->table('loadimport_autoprocess_settings')
            ->where('id', 1)
            ->update([
                'is_enabled' => 0,
                'updated_at' => now(),
            ]);

        return $this->getAutoProcessStatus();
    }

    public function runAutoProcessTick(): array
    {
        $gate = DB::connection()->transaction(function () {
            $row = DB::connection()
                ->table('loadimport_autoprocess_settings')
                ->where('id', 1)
                ->lockForUpdate()
                ->first();

            if (!$row) {
                DB::connection()->table('loadimport_autoprocess_settings')->insert([
                    'id' => 1,
                    'is_enabled' => 0,
                    'interval_minutes' => 5,
                    'is_running' => 0,
                    'last_started_at' => null,
                    'last_finished_at' => null,
                    'last_result_json' => null,
                    'created_at' => now(),
                    'updated_at' => now(),
                ]);

                $row = DB::connection()
                    ->table('loadimport_autoprocess_settings')
                    ->where('id', 1)
                    ->lockForUpdate()
                    ->first();
            }

            if (!(int)$row->is_enabled) {
                return [
                    'should_run' => false,
                    'reason' => 'disabled',
                ];
            }

            if ((int)$row->is_running === 1) {
                return [
                    'should_run' => false,
                    'reason' => 'already_running',
                ];
            }

            $intervalMinutes = max(1, (int)($row->interval_minutes ?? 5));
            $lastStartedAt = !empty($row->last_started_at)
                ? Carbon::parse($row->last_started_at)
                : null;

            if ($lastStartedAt) {
                $nextDueAt = $lastStartedAt->copy()->addMinutes($intervalMinutes);

                if (now()->lt($nextDueAt)) {
                    return [
                        'should_run' => false,
                        'reason' => 'not_due',
                        'next_due_at' => $nextDueAt->toDateTimeString(),
                    ];
                }
            }

            DB::connection()->table('loadimport_autoprocess_settings')
                ->where('id', 1)
                ->update([
                    'is_running' => 1,
                    'last_started_at' => now(),
                    'updated_at' => now(),
                ]);

            return [
                'should_run' => true,
                'interval_minutes' => $intervalMinutes,
            ];
        });

        if (!($gate['should_run'] ?? false)) {
            return [
                'ok' => true,
                'skipped' => true,
                'reason' => $gate['reason'] ?? 'unknown',
                'next_due_at' => $gate['next_due_at'] ?? null,
            ];
        }

        $candidateIds = [];
        $result = null;

        try {
            $candidateIds = $this->findAutoProcessCandidateIds();

            if (count($candidateIds) === 0) {
                $result = [
                    'ok' => true,
                    'skipped' => true,
                    'reason' => 'no_candidates',
                    'candidate_ids' => [],
                ];
            } else {
                $result = $this->processAutoWithLogging($candidateIds);
                $result['candidate_ids'] = $candidateIds;
            }
        } catch (Throwable $e) {
            $result = [
                'ok' => false,
                'error' => $e->getMessage(),
                'candidate_ids' => $candidateIds,
            ];
        } finally {
            DB::connection()->table('loadimport_autoprocess_settings')
                ->where('id', 1)
                ->update([
                    'is_running' => 0,
                    'last_finished_at' => now(),
                    'last_result_json' => json_encode($result, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
                    'updated_at' => now(),
                ]);
        }

        return $result;
    }

    public function buildQueue(int $limit, string $only, string $q, string $matchFilter): array
    {
        $select = ['id', 'jobname', 'payload_json', 'payload_original', 'created_at', 'updated_at'];

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
            $liIdLoadDetail = property_exists($r, 'id_load_detail') ? ($r->id_load_detail !== null ? (int)$r->id_load_detail : null) : null;
            $liInsertedAt = property_exists($r, 'inserted_at') ? ($r->inserted_at ?? null) : null;

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
            $statusLabel = $this->formatQueueStatusFromStage($stage);

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
                'status_label' => $statusLabel,

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

                'is_inserted' => ($liIsInserted === 1),
                'import_load_id' => $liIdLoad,
                'import_load_detail_id' => $liIdLoadDetail,
                'inserted_at' => $liInsertedAt,

                'is_processed' => $isProcessed,
                'processed_load_id' => $processedDetail?->id_load
                    ? (int)$processedDetail->id_load
                    : $liIdLoad,
                'processed_load_detail_id' => $processedDetail?->id_load_detail
                    ? (int)$processedDetail->id_load_detail
                    : $liIdLoadDetail,

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

    public function processSingleWithLogging(int $importId): array
    {
        $sessionId = $this->processLogger->newSessionId();

        $row = DB::connection()->table('loadimports')->where('id', $importId)->first();

        if (!$row) {
            $this->processLogger->warning(
                sessionId: $sessionId,
                loadimportId: $importId,
                runType: LoadImportProcessRunType::SINGLE_PROCESS,
                event: 'review_not_found',
                status: 'failed',
                message: 'Import not found',
                stageDetected: null,
                context: ['import_id' => $importId]
            );

            return ['ok' => false, 'error' => 'Import not found'];
        }

        $payload = $row->payload_json ?? null;
        $state = $row->state ?? null;
        $stage = $this->stageResolver->determineStage($payload, $state);
        $isInserted = property_exists($row, 'is_inserted') ? (int)($row->is_inserted ?? 0) : 0;

        $this->logStarted(
            sessionId: $sessionId,
            importId: $importId,
            runType: LoadImportProcessRunType::SINGLE_PROCESS,
            stageDetected: $stage,
            context: [
                'mode' => ($stage === StageResolver::STAGE_DELIVERED_CONFIRMED && $isInserted === 0) ? 'bootstrap_or_final' : 'strict',
            ]
        );

        try {
            if ($stage === StageResolver::STAGE_DELIVERED_CONFIRMED && $isInserted === 0) {
                $res = $this->processImport($importId);
            } else {
                $res = $this->processImportStrict($importId);
            }

            $this->logCompleted(
                sessionId: $sessionId,
                importId: $importId,
                runType: LoadImportProcessRunType::SINGLE_PROCESS,
                stageDetected: $stage,
                result: $res
            );

            $res['session_id'] = $sessionId;

            return $res;
        } catch (Throwable $e) {
            $this->processLogger->exception(
                sessionId: $sessionId,
                loadimportId: $importId,
                runType: LoadImportProcessRunType::SINGLE_PROCESS,
                event: 'review_exception',
                e: $e,
                stageDetected: $stage
            );

            return [
                'ok' => false,
                'error' => $e->getMessage(),
                'session_id' => $sessionId,
            ];
        }
    }

    public function processImportStrict(int $importId): array
    {
        $row = DB::connection()->table('loadimports')->where('id', $importId)->first();
        if (!$row) return ['ok' => false, 'error' => "Import id={$importId} not found."];

        $parsed = $this->parser->parseImportRow($row);

        $journey = $this->resolveJourneyForParsed($parsed);
        $joinId = isset($journey['join_id']) && $journey['join_id'] ? (int)$journey['join_id'] : null;

        if (!$joinId || (($journey['status'] ?? '') !== 'READY')) {
            return ['ok' => false, 'error' => 'Journey is not READY / join_id missing. Cannot process.'];
        }

        $loadNumber = $this->str->strOrNull($parsed['load_number'] ?? null);
        if (!$loadNumber) return ['ok' => false, 'error' => 'Missing load_number. Cannot process.'];

        $detectedStage = $this->stageResolver->determineStage($row->payload_json ?? null, $parsed['state'] ?? null);
        $desiredRank = $this->stageResolver->stageRank($detectedStage);

        $currentRank = $this->stageGuard->inferCurrentStageRankFromDb($joinId, $loadNumber);

        $liIsInserted = property_exists($row, 'is_inserted') ? (int)($row->is_inserted ?? 0) : 0;

        $allowDeliveredConfirmedBootstrap =
            $currentRank === 0
            && $detectedStage === StageResolver::STAGE_DELIVERED_CONFIRMED
            && $liIsInserted === 0;

        if ($currentRank === 0) {
            if (!$allowDeliveredConfirmedBootstrap && $desiredRank !== 1) {
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
            $isInserted = property_exists($row, 'is_inserted') ? (int)($row->is_inserted ?? 0) : 0;

            if ($stage === StageResolver::STAGE_DELIVERED_CONFIRMED && $isInserted === 0) {
                $existingLoad = DB::connection()
                    ->table('load_detail as ld')
                    ->join('load as l', 'l.id_load', '=', 'ld.id_load')
                    ->select(['ld.id_load_detail', 'ld.id_load'])
                    ->where('ld.load_number', $parsed['load_number'])
                    ->where('l.id_join', $joinId)
                    ->where('l.is_deleted', 0)
                    ->orderByDesc('ld.id_load_detail')
                    ->first();

                if ($existingLoad) {
                    $idLoad = (int)$existingLoad->id_load;
                    $idLoadDetail = (int)$existingLoad->id_load_detail;

                    $detailUpd = [];
                    $loadUpd = [];

                    $this->updater->applyBoxesNoteIfMissing($idLoadDetail, $detailUpd, $boxesNote);

                    if ($this->schema->columnExists('load_detail', 'ticket_number') && ($parsed['ticket_number'] ?? null)) {
                        $detailUpd['ticket_number'] = $parsed['ticket_number'];
                    }

                    $this->updater->applyWeightsUpdates($detailUpd, $netLbs, $tons);

                    $this->updater->applyDeliveryUpdates($loadUpd, $deliveryMysql, true);

                    $this->updater->applyBolUpdateIfAllowed(
                        $importId,
                        $parsed,
                        $idLoadDetail,
                        $detailUpd,
                        $bol,
                        true
                    );

                    $imageImport = $this->applyDeliveredConfirmedImageImport($importId, $bol);

                    if (!($imageImport['ok'] ?? false)) {
                        return [
                            'ok' => false,
                            'stage' => $stage,
                            'error' => $imageImport['error'] ?? 'IMAGE_IMPORT_GRABBER failed.',
                            'image_import' => $imageImport,
                        ];
                    }

                    $reviewMysql = $this->extractReviewDate($row->payload_json ?? null);
                    if ($reviewMysql && $this->schema->columnExists('load_detail', 'review_date')) {
                        $detailUpd['review_date'] = $reviewMysql;
                    }

                    $this->updater->flushUpdates($idLoad, $idLoadDetail, $loadUpd, $detailUpd);

                    $this->tracker->updateTracking($importId, $idLoad, $idLoadDetail);

                    $this->syncDeliveredConfirmedChainTracking(
                        $importId,
                        $parsed,
                        $joinId,
                        $idLoad,
                        $idLoadDetail
                    );

                    return [
                        'ok' => true,
                        'stage' => $stage,
                        'attached_to_existing' => true,
                        'id_load' => $idLoad,
                        'id_load_detail' => $idLoadDetail,
                        'updated' => [
                            'load' => array_keys($loadUpd),
                            'load_detail' => array_keys($detailUpd),
                        ],
                        'image_import' => $imageImport,
                    ];
                }

                $loadInsert = [
                    'id_carrier' => $idCarrier,
                    'id_join' => $joinId,
                    'id_contact' => $idContact,
                    'id_vehicle' => $idVehicle,
                    'is_deleted' => 0,
                    'load_date' => $loadDate,
                    'is_finished' => null,
                ];

                $idLoad = (int)DB::connection()
                    ->table('load')
                    ->insertGetId($loadInsert);

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

                $detailUpd = [];
                $loadUpd = [];

                $this->updater->applyBoxesNoteIfMissing($idLoadDetail, $detailUpd, $boxesNote);

                if ($this->schema->columnExists('load_detail', 'ticket_number') && ($parsed['ticket_number'] ?? null)) {
                    $detailUpd['ticket_number'] = $parsed['ticket_number'];
                }

                $this->updater->applyWeightsUpdates($detailUpd, $netLbs, $tons);

                $this->updater->applyDeliveryUpdates($loadUpd, $deliveryMysql, true);

                $this->updater->applyBolUpdateIfAllowed(
                    $importId,
                    $parsed,
                    $idLoadDetail,
                    $detailUpd,
                    $bol,
                    true
                );

                $imageImport = $this->applyDeliveredConfirmedImageImport($importId, $bol);

                if (!($imageImport['ok'] ?? false)) {
                    return [
                        'ok' => false,
                        'stage' => $stage,
                        'error' => $imageImport['error'] ?? 'IMAGE_IMPORT_GRABBER failed.',
                        'image_import' => $imageImport,
                    ];
                }

                $reviewMysql = $this->extractReviewDate($row->payload_json ?? null);
                if ($reviewMysql && $this->schema->columnExists('load_detail', 'review_date')) {
                    $detailUpd['review_date'] = $reviewMysql;
                }

                $this->updater->flushUpdates($idLoad, $idLoadDetail, $loadUpd, $detailUpd);

                $this->tracker->updateTracking($importId, $idLoad, $idLoadDetail);

                $this->syncDeliveredConfirmedChainTracking(
                    $importId,
                    $parsed,
                    $joinId,
                    $idLoad,
                    $idLoadDetail
                );

                return [
                    'ok' => true,
                    'stage' => $stage,
                    'auto_inserted' => true,
                    'id_load' => $idLoad,
                    'id_load_detail' => $idLoadDetail,
                    'updated' => [
                        'load' => array_keys($loadUpd),
                        'load_detail' => array_keys($detailUpd),
                    ],
                    'image_import' => $imageImport,
                ];
            }

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

                $loadInsert = [
                    'id_carrier' => $idCarrier,
                    'id_join' => $joinId,
                    'id_contact' => $idContact,
                    'id_vehicle' => $idVehicle,
                    'is_deleted' => 0,
                    'load_date' => $loadDate,
                    'is_finished' => null,
                ];

                $idLoad = (int)DB::connection()
                    ->table('load')
                    ->insertGetId($loadInsert);

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

            if ($stage === StageResolver::STAGE_DELIVERED_PENDING) {
                $this->updater->applyDeliveryUpdates($loadUpd, $deliveryMysql, true);

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

                $this->updater->applyBolUpdateIfAllowed(
                    $importId,
                    $parsed,
                    $idLoadDetail,
                    $detailUpd,
                    $bol,
                    true
                );

                $imageImport = $this->applyDeliveredConfirmedImageImport($importId, $bol);

                if (!($imageImport['ok'] ?? false)) {
                    return [
                        'ok' => false,
                        'stage' => $stage,
                        'error' => $imageImport['error'] ?? 'IMAGE_IMPORT_GRABBER failed.',
                        'image_import' => $imageImport,
                    ];
                }

                $reviewMysql = $this->extractReviewDate($row->payload_json ?? null);
                if ($reviewMysql && $this->schema->columnExists('load_detail', 'review_date')) {
                    $detailUpd['review_date'] = $reviewMysql;
                }

                $this->updater->flushUpdates($idLoad, $idLoadDetail, $loadUpd, $detailUpd);
                $this->tracker->updateTracking($importId, $idLoad, $idLoadDetail);
                $this->syncDeliveredConfirmedChainTracking($importId, $parsed, $joinId, $idLoad, $idLoadDetail);

                return [
                    'ok' => true,
                    'stage' => $stage,
                    'id_load' => $idLoad,
                    'id_load_detail' => $idLoadDetail,
                    'updated' => [
                        'load' => array_keys($loadUpd),
                        'load_detail' => array_keys($detailUpd),
                    ],
                    'image_import' => $imageImport,
                ];
            }

            return ['ok' => false, 'stage' => $stage, 'error' => "Unknown stage: {$stage}"];
        });
    }

    public function processBatchWithLogging(array $importIds, string $runType = LoadImportProcessRunType::BATCH_PROCESS): array
    {
        $sessionId = $this->processLogger->newSessionId();

        $res = $this->processBatchStrict($importIds, $sessionId, $runType);
        $res['session_id'] = $sessionId;

        return $res;
    }

    public function processAutoWithLogging(array $importIds): array
    {
        return $this->processBatchWithLogging($importIds, LoadImportProcessRunType::AUTO_PROCESS);
    }

    public function processBatchStrict(
        array $importIds,
        ?string $sessionId = null,
        string $runType = LoadImportProcessRunType::BATCH_PROCESS
    ): array {
        $sessionId = $sessionId ?: $this->processLogger->newSessionId();

        $ids = [];

        foreach ($importIds as $v) {
            $id = (int)$v;
            if ($id > 0) $ids[] = $id;
        }

        $ids = array_values(array_unique($ids));
        if (count($ids) === 0) {
            return ['ok' => false, 'error' => 'import_ids must contain at least one valid id'];
        }

        $ids = array_slice($ids, 0, 500);

        $rows = DB::connection()->table('loadimports')->whereIn('id', $ids)->get();

        $groups = [];
        $orphans = [];

        $foundIds = [];
        foreach ($rows as $row) {
            $foundIds[] = (int)$row->id;
        }

        $missingIds = array_values(array_diff($ids, $foundIds));
        foreach ($missingIds as $missingId) {
            $orphans[] = [
                'import_id' => (int)$missingId,
                'ok' => false,
                'error' => 'Import not found',
            ];

            $this->processLogger->warning(
                sessionId: $sessionId,
                loadimportId: (int)$missingId,
                runType: $runType,
                event: 'review_not_found',
                status: 'failed',
                message: 'Import not found',
                stageDetected: null,
                context: [
                    'source' => 'batch_request',
                ]
            );
        }

        foreach ($rows as $r) {
            $parsed = $this->parser->parseImportRow($r);
            $journey = $this->resolveJourneyForParsed($parsed);

            $joinId = isset($journey['join_id']) && $journey['join_id'] ? (int)$journey['join_id'] : null;
            if (!$joinId || (($journey['status'] ?? '') !== 'READY')) {
                $orphans[] = ['import_id' => (int)$r->id, 'ok' => false, 'error' => 'Journey not READY / join_id missing'];

                $stage = $this->stageResolver->determineStage($r->payload_json ?? null, $parsed['state'] ?? null);

                $this->processLogger->warning(
                    sessionId: $sessionId,
                    loadimportId: (int)$r->id,
                    runType: $runType,
                    event: 'review_rejected',
                    status: 'failed',
                    message: 'Journey not READY / join_id missing',
                    stageDetected: $stage,
                    context: [
                        'load_number' => $parsed['load_number'] ?? null,
                    ]
                );

                continue;
            }

            $loadNumber = $this->str->strOrNull($parsed['load_number'] ?? null);
            if (!$loadNumber) {
                $orphans[] = ['import_id' => (int)$r->id, 'ok' => false, 'error' => 'Missing load_number; cannot group/sequence'];

                $stage = $this->stageResolver->determineStage($r->payload_json ?? null, $parsed['state'] ?? null);

                $this->processLogger->warning(
                    sessionId: $sessionId,
                    loadimportId: (int)$r->id,
                    runType: $runType,
                    event: 'review_rejected',
                    status: 'failed',
                    message: 'Missing load_number; cannot group/sequence',
                    stageDetected: $stage
                );

                continue;
            }

            $groupKey = $joinId . '|' . $loadNumber;

            if (!isset($groups[$groupKey])) {
                $groups[$groupKey] = [
                    'join_id' => $joinId,
                    'load_number' => $loadNumber,
                    'selected_ids' => [],
                ];
            }

            $groups[$groupKey]['selected_ids'][] = (int)$r->id;
        }

        $results = [];
        $okGroups = 0;
        $failGroups = 0;

        foreach ($groups as $gk => $g) {
            $joinId = (int)$g['join_id'];
            $loadNumber = (string)$g['load_number'];
            $selectedIds = array_values(array_unique($g['selected_ids']));
            $firstSelectedId = (int)($selectedIds[0] ?? 0);

            $currentRank = $this->stageGuard->inferCurrentStageRankFromDb($joinId, $loadNumber);
            $availableByRank = $this->findBestImportsForGroup($joinId, $loadNumber);
            $availableRanks = array_keys($availableByRank);
            sort($availableRanks);

            $trace = [
                'group_key' => $gk,
                'join_id' => $joinId,
                'load_number' => $loadNumber,
                'selected_ids' => $selectedIds,
                'current_rank' => $currentRank,
                'available_ranks' => $availableRanks,
                'steps' => [],
            ];

            if ($currentRank >= 4) {
                $trace['ok'] = true;
                $trace['skipped_completed'] = true;
                $results[] = $trace;
                $okGroups++;

                if ($firstSelectedId > 0) {
                    $this->processLogger->info(
                        sessionId: $sessionId,
                        loadimportId: $firstSelectedId,
                        runType: $runType,
                        event: 'review_skipped',
                        status: 'skipped',
                        message: 'Group already fully processed in DB',
                        stageDetected: StageResolver::STAGE_DELIVERED_CONFIRMED,
                        context: [
                            'group_key' => $gk,
                            'join_id' => $joinId,
                            'load_number' => $loadNumber,
                            'selected_ids' => $selectedIds,
                        ]
                    );
                }

                continue;
            }

            if (empty($availableByRank)) {
                $trace['ok'] = false;
                $trace['steps'][] = [
                    'ok' => false,
                    'error' => 'No usable import rows found for this join_id + load_number group.',
                ];
                $results[] = $trace;
                $failGroups++;

                if ($firstSelectedId > 0) {
                    $this->processLogger->warning(
                        sessionId: $sessionId,
                        loadimportId: $firstSelectedId,
                        runType: $runType,
                        event: 'review_rejected',
                        status: 'failed',
                        message: 'No usable import rows found for this join_id + load_number group',
                        stageDetected: null,
                        context: [
                            'group_key' => $gk,
                            'join_id' => $joinId,
                            'load_number' => $loadNumber,
                            'selected_ids' => $selectedIds,
                        ]
                    );
                }

                continue;
            }

            $planRanks = [];

            if (isset($availableByRank[4])) {
                $planRanks = [4];
                $trace['mode'] = 'FINAL_ONLY';
            } else {
                $highestAvailableRank = max($availableRanks);
                $trace['mode'] = 'SEQUENTIAL';

                if ($highestAvailableRank <= $currentRank) {
                    $trace['ok'] = true;
                    $trace['nothing_to_do'] = true;
                    $results[] = $trace;
                    $okGroups++;

                    if ($firstSelectedId > 0) {
                        $this->processLogger->info(
                            sessionId: $sessionId,
                            loadimportId: $firstSelectedId,
                            runType: $runType,
                            event: 'review_skipped',
                            status: 'skipped',
                            message: 'Nothing to do for this group',
                            stageDetected: $this->stageResolver->rankToStage($currentRank),
                            context: [
                                'group_key' => $gk,
                                'join_id' => $joinId,
                                'load_number' => $loadNumber,
                                'selected_ids' => $selectedIds,
                                'current_rank' => $currentRank,
                                'highest_available_rank' => $highestAvailableRank,
                            ]
                        );
                    }

                    continue;
                }

                if ($currentRank === 0 && !isset($availableByRank[1])) {
                    $trace['ok'] = false;
                    $trace['steps'][] = [
                        'ok' => false,
                        'error' => 'No AT_TERMINAL import exists for this join_id + load_number group, and no DELIVERED_CONFIRMED bootstrap row was found.',
                    ];
                    $results[] = $trace;
                    $failGroups++;

                    if ($firstSelectedId > 0) {
                        $this->processLogger->warning(
                            sessionId: $sessionId,
                            loadimportId: $firstSelectedId,
                            runType: $runType,
                            event: 'review_rejected',
                            status: 'failed',
                            message: 'No AT_TERMINAL import exists for this group and no DELIVERED_CONFIRMED bootstrap row was found',
                            stageDetected: null,
                            context: [
                                'group_key' => $gk,
                                'join_id' => $joinId,
                                'load_number' => $loadNumber,
                                'selected_ids' => $selectedIds,
                            ]
                        );
                    }

                    continue;
                }

                for ($rank = $currentRank + 1; $rank <= $highestAvailableRank; $rank++) {
                    if (!isset($availableByRank[$rank])) {
                        $missingStage = $this->stageResolver->rankToStage($rank);

                        $trace['ok'] = false;
                        $trace['steps'][] = [
                            'ok' => false,
                            'rank' => $rank,
                            'stage' => $missingStage,
                            'error' => 'Missing import row for required next stage.',
                        ];
                        $results[] = $trace;
                        $failGroups++;

                        if ($firstSelectedId > 0) {
                            $this->processLogger->warning(
                                sessionId: $sessionId,
                                loadimportId: $firstSelectedId,
                                runType: $runType,
                                event: 'review_rejected',
                                status: 'failed',
                                message: 'Missing import row for required next stage',
                                stageDetected: $missingStage,
                                context: [
                                    'group_key' => $gk,
                                    'join_id' => $joinId,
                                    'load_number' => $loadNumber,
                                    'selected_ids' => $selectedIds,
                                    'current_rank' => $currentRank,
                                    'missing_rank' => $rank,
                                ]
                            );
                        }

                        continue 2;
                    }

                    $planRanks[] = $rank;
                }
            }

            $trace['plan_ranks'] = $planRanks;
            $groupOk = true;

            foreach ($planRanks as $rank) {
                $importId = (int)$availableByRank[$rank];
                $stage = $this->stageResolver->rankToStage($rank);

                $this->logStarted(
                    sessionId: $sessionId,
                    importId: $importId,
                    runType: $runType,
                    stageDetected: $stage,
                    context: [
                        'group_key' => $gk,
                        'join_id' => $joinId,
                        'load_number' => $loadNumber,
                        'selected_ids' => $selectedIds,
                        'rank' => $rank,
                        'mode' => $trace['mode'] ?? null,
                    ]
                );

                try {
                    $res = $this->processImport($importId, $stage);

                    $this->logCompleted(
                        sessionId: $sessionId,
                        importId: $importId,
                        runType: $runType,
                        stageDetected: $stage,
                        result: $res
                    );
                } catch (Throwable $e) {
                    $res = [
                        'ok' => false,
                        'stage' => $stage,
                        'error' => $e->getMessage(),
                    ];

                    $this->processLogger->exception(
                        sessionId: $sessionId,
                        loadimportId: $importId,
                        runType: $runType,
                        event: 'review_exception',
                        e: $e,
                        stageDetected: $stage,
                        context: [
                            'group_key' => $gk,
                            'join_id' => $joinId,
                            'load_number' => $loadNumber,
                            'rank' => $rank,
                        ]
                    );
                }

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

                $currentRank = max($currentRank, $rank);
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
        return $this->processBatchWithLogging($importIds);
    }

    private function logStarted(
        string $sessionId,
        int $importId,
        string $runType,
        ?string $stageDetected,
        array $context = []
    ): void {
        $label = $this->runTypeLabel($runType);

        $this->processLogger->info(
            sessionId: $sessionId,
            loadimportId: $importId,
            runType: $runType,
            event: 'review_started',
            status: 'started',
            message: $label . ' started',
            stageDetected: $stageDetected,
            context: $context
        );
    }

    private function logCompleted(
        string $sessionId,
        int $importId,
        string $runType,
        ?string $stageDetected,
        array $result
    ): void {
        $label = $this->runTypeLabel($runType);
        $ok = (bool)($result['ok'] ?? false);
        $message = (string)($result['error'] ?? $result['message'] ?? ($ok ? $label . ' completed successfully' : $label . ' failed'));

        $idLoad = isset($result['id_load']) && $result['id_load'] !== null ? (int)$result['id_load'] : null;
        $idLoadDetail = isset($result['id_load_detail']) && $result['id_load_detail'] !== null ? (int)$result['id_load_detail'] : null;
        $resultStage = isset($result['stage']) && $result['stage'] ? (string)$result['stage'] : $stageDetected;

        if ($ok) {
            $this->processLogger->info(
                sessionId: $sessionId,
                loadimportId: $importId,
                runType: $runType,
                event: 'review_completed',
                status: 'success',
                message: $message,
                stageDetected: $resultStage,
                idLoad: $idLoad,
                idLoadDetail: $idLoadDetail,
                context: $result
            );
            return;
        }

        $this->processLogger->warning(
            sessionId: $sessionId,
            loadimportId: $importId,
            runType: $runType,
            event: 'review_completed',
            status: 'failed',
            message: $message,
            stageDetected: $resultStage,
            idLoad: $idLoad,
            idLoadDetail: $idLoadDetail,
            context: $result
        );
    }

    private function runTypeLabel(string $runType): string
    {
        return match ($runType) {
            LoadImportProcessRunType::SINGLE_PROCESS => 'Single process',
            LoadImportProcessRunType::BATCH_PROCESS => 'Batch process',
            LoadImportProcessRunType::AUTO_PROCESS => 'Auto process',
            default => 'Process',
        };
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

    private function resolveDeliveryMysqlForStage(string $stage, ?string $payloadJson, ?string $fallbackDeliveryTime): ?string
    {
        if (in_array($stage, [StageResolver::STAGE_DELIVERED_PENDING, StageResolver::STAGE_DELIVERED_CONFIRMED], true)) {
            $best = $this->extractFirstNonEmptyFromPayload($payloadJson, [
                'datetime_delivered',
                'datetime_at_destination',
                'delivery_time',
            ]);

            if ($best) return $this->dateResolver->toMysqlDatetime($best);
        }

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

    /**
     * Client test behavior:
     * - IMAGE_IMPORT_GRABBER is a fixed command-line script/value from .env
     * - execute it exactly as-is
     * - do not append source/destination args during the test setup
     * - keep source path handling in existing BOL update flow
     */
    /**
     * IMAGE_IMPORT_GRABBER contract:
     * - .env contains the base command (and may include a fixed first arg for local wrappers)
     * - PHP always appends the runtime source relative path as the final argument
     * - production example:
     *     /var/www/html/OOPS/voldchat/movefromapi.sh <relative-path-under-loadimports>
     * - local example:
     *     cmd /c C:\tmp\image_import_grabber_test.bat "C:\tmp\test destination.jpg" <relative-path-under-loadimports>
     */
    private function applyDeliveredConfirmedImageImport(int $importId, array $bol): array
    {
        $sourceBolPath = $this->str->strOrNull($bol['bol_path'] ?? null);
        $bolType = $this->str->strOrNull($bol['bol_type'] ?? null);

        if (!$sourceBolPath) {
            return [
                'ok' => true,
                'skipped' => true,
                'reason' => 'No BOL source path available for IMAGE_IMPORT_GRABBER.',
                'source_path' => null,
                'bol_type' => $bolType,
            ];
        }

        $command = $this->getImageImportGrabberCommand();
        if (!$command) {
            return [
                'ok' => false,
                'skipped' => false,
                'error' => 'IMAGE_IMPORT_GRABBER is not configured.',
                'source_path' => $sourceBolPath,
                'bol_type' => $bolType,
            ];
        }

        try {
            $normalizedSourcePath = $this->normalizeGrabberRelativePath($sourceBolPath);
        } catch (Throwable $e) {
            return [
                'ok' => false,
                'skipped' => false,
                'error' => 'Invalid IMAGE_IMPORT_GRABBER source path for import_id=' . $importId . '. ' . $e->getMessage(),
                'source_path' => $sourceBolPath,
                'bol_type' => $bolType,
            ];
        }

        $finalCommand = $command . ' ' . escapeshellarg($normalizedSourcePath);
        $exec = $this->executeShellCommand($finalCommand);

        if (!($exec['ok'] ?? false)) {
            return [
                'ok' => false,
                'skipped' => false,
                'error' => 'IMAGE_IMPORT_GRABBER failed for import_id=' . $importId . '. ' . ($exec['error'] ?? 'Unknown shell error.'),
                'command' => $finalCommand,
                'output' => $exec['output'] ?? [],
                'exit_code' => $exec['exit_code'] ?? null,
                'source_path' => $normalizedSourcePath,
                'bol_type' => $bolType,
            ];
        }

        return [
            'ok' => true,
            'skipped' => false,
            'command' => $finalCommand,
            'source_path' => $normalizedSourcePath,
            'output' => $exec['output'] ?? [],
            'bol_type' => $bolType,
        ];
    }

    private function normalizeGrabberRelativePath(?string $path): string
    {
        $path = trim((string)$path);
        $path = str_replace('\\', '/', $path);
        $path = ltrim($path, '/');

        if ($path === '') {
            throw new \RuntimeException('Source path is empty.');
        }

        if (preg_match('/^[A-Za-z]:[\/\\\\]/', $path)) {
            throw new \RuntimeException('Absolute Windows path given; relative path expected.');
        }

        if (str_starts_with($path, '\\')) {
            throw new \RuntimeException('UNC/absolute path given; relative path expected.');
        }

        if (str_contains($path, '..')) {
            throw new \RuntimeException('Parent directory traversal is not allowed.');
        }

        return $path;
    }

    private function getImageImportGrabberCommand(): ?string
    {
        $v = $_ENV['IMAGE_IMPORT_GRABBER'] ?? getenv('IMAGE_IMPORT_GRABBER') ?: env('IMAGE_IMPORT_GRABBER');
        $v = is_string($v) ? trim($v) : null;
        return $v !== '' ? $v : null;
    }

    private function executeShellCommand(string $command): array
    {
        $output = [];
        $exitCode = 0;

        exec($command . ' 2>&1', $output, $exitCode);

        if ($exitCode !== 0) {
            return [
                'ok' => false,
                'exit_code' => $exitCode,
                'output' => $output,
                'error' => implode("\n", $output),
            ];
        }

        return [
            'ok' => true,
            'exit_code' => $exitCode,
            'output' => $output,
        ];
    }

    /**
     * When DELIVERED_CONFIRMED is processed first and becomes the official record,
     * mark the sibling imports for the same join_id + load_number as processed too,
     * so the earlier stages disappear from the queue.
     */
    private function syncDeliveredConfirmedChainTracking(
        int $currentImportId,
        array $targetParsed,
        int $targetJoinId,
        int $idLoad,
        int $idLoadDetail
    ): void {
        $targetLoadNumber = $this->str->strOrNull($targetParsed['load_number'] ?? null);
        if (!$targetLoadNumber) return;

        $candidates = $this->findImportCandidatesByLoadNumber($targetLoadNumber);

        foreach ($candidates as $candidate) {
            $candidateId = (int)($candidate->id ?? 0);
            if ($candidateId <= 0 || $candidateId === $currentImportId) {
                continue;
            }

            $candidateParsed = $this->parser->parseImportRow($candidate);
            $candidateLoadNumber = $this->str->strOrNull($candidateParsed['load_number'] ?? null);
            if (!$candidateLoadNumber || strcasecmp($candidateLoadNumber, $targetLoadNumber) !== 0) {
                continue;
            }

            $candidateJourney = $this->resolveJourneyForParsed($candidateParsed);
            $candidateJoinId = isset($candidateJourney['join_id']) && $candidateJourney['join_id']
                ? (int)$candidateJourney['join_id']
                : null;

            if (!$candidateJoinId || $candidateJoinId !== $targetJoinId) {
                continue;
            }

            $candidateStage = $this->stageResolver->determineStage(
                $candidate->payload_json ?? null,
                $candidateParsed['state'] ?? null
            );

            $candidateRank = $this->stageResolver->stageRank($candidateStage);
            if ($candidateRank < 1 || $candidateRank > 4) {
                continue;
            }

            $this->tracker->updateTracking($candidateId, $idLoad, $idLoadDetail);
        }
    }

    private function resolveJourneyForParsed(array $parsed): array
    {
        $pp = $this->locationMatcher->matchPullPointByTerminal($parsed['terminal'] ?? null);
        $pl = $this->locationMatcher->matchPadLocationByJobname($parsed['jobname'] ?? null);
        return $this->journeyResolver->buildJourney($pp, $pl);
    }

    private function findBestImportsForGroup(int $joinId, string $loadNumber): array
    {
        $best = [];
        $candidates = $this->findImportCandidatesByLoadNumber($loadNumber);

        foreach ($candidates as $candidate) {
            $candidateParsed = $this->parser->parseImportRow($candidate);
            $candidateLoadNumber = $this->str->strOrNull($candidateParsed['load_number'] ?? null);

            if (!$candidateLoadNumber || strcasecmp($candidateLoadNumber, $loadNumber) !== 0) {
                continue;
            }

            $candidateJourney = $this->resolveJourneyForParsed($candidateParsed);
            $candidateJoinId = isset($candidateJourney['join_id']) && $candidateJourney['join_id']
                ? (int)$candidateJourney['join_id']
                : null;

            if (!$candidateJoinId || $candidateJoinId !== $joinId) {
                continue;
            }

            $candidateStage = $this->stageResolver->determineStage(
                $candidate->payload_json ?? null,
                $candidateParsed['state'] ?? null
            );

            $rank = $this->stageResolver->stageRank($candidateStage);
            if ($rank < 1 || $rank > 4) {
                continue;
            }

            if (!isset($best[$rank]) || (int)$candidate->id > (int)$best[$rank]) {
                $best[$rank] = (int)$candidate->id;
            }
        }

        ksort($best);
        return $best;
    }

    private function findAutoProcessCandidateIds(): array
    {
        $queue = $this->buildQueue(3000, 'unprocessed', '', '');
        $bestByGroup = [];

        foreach ($queue as $row) {
            if (($row['stage'] ?? null) !== StageResolver::STAGE_DELIVERED_CONFIRMED) {
                continue;
            }

            if (!empty($row['is_processed']) || !empty($row['is_inserted'])) {
                continue;
            }

            $confidence = $row['match']['confidence'] ?? null;
            if ($confidence !== 'GREEN') {
                continue;
            }

            $resolvedDriver = $row['match']['driver']['resolved'] ?? null;
            if (!$resolvedDriver || empty($resolvedDriver['id_driver']) || empty($resolvedDriver['id_carrier'])) {
                continue;
            }

            $journey = $row['match']['journey'] ?? [];
            if (($journey['status'] ?? '') !== 'READY') {
                continue;
            }

            $joinId = (int)($journey['join_id'] ?? 0);
            $loadNumber = $this->str->strOrNull($row['load_number'] ?? null);
            $importId = (int)($row['import_id'] ?? 0);

            if ($joinId <= 0 || !$loadNumber || $importId <= 0) {
                continue;
            }

            $groupKey = $joinId . '|' . $loadNumber;
            if (!isset($bestByGroup[$groupKey]) || $importId > $bestByGroup[$groupKey]) {
                $bestByGroup[$groupKey] = $importId;
            }
        }

        $ids = array_values($bestByGroup);
        rsort($ids);

        return $ids;
    }

    private function ensureAutoProcessSettingsRow(): object
    {
        $row = DB::connection()
            ->table('loadimport_autoprocess_settings')
            ->where('id', 1)
            ->first();

        if ($row) {
            return $row;
        }

        DB::connection()->table('loadimport_autoprocess_settings')->insert([
            'id' => 1,
            'is_enabled' => 0,
            'interval_minutes' => 5,
            'is_running' => 0,
            'last_started_at' => null,
            'last_finished_at' => null,
            'last_result_json' => null,
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        return DB::connection()
            ->table('loadimport_autoprocess_settings')
            ->where('id', 1)
            ->first();
    }

    private function findImportCandidatesByLoadNumber(string $loadNumber): iterable
    {
        $loadNumber = $this->str->strOrNull($loadNumber);
        if (!$loadNumber) return [];

        $hasLoadNumberCol = $this->schema->columnExists('loadimports', 'load_number');
        $hasPayloadJson   = $this->schema->columnExists('loadimports', 'payload_json');

        if (!$hasLoadNumberCol && !$hasPayloadJson) {
            return [];
        }

        $q = DB::connection()->table('loadimports');

        $q->where(function ($w) use ($loadNumber, $hasLoadNumberCol, $hasPayloadJson) {
            if ($hasLoadNumberCol) {
                $w->where('load_number', $loadNumber);
            }

            if ($hasPayloadJson) {
                if ($hasLoadNumberCol) {
                    $w->orWhereRaw(
                        "TRIM(JSON_UNQUOTE(JSON_EXTRACT(payload_json, '$.loadnumber'))) = ?",
                        [$loadNumber]
                    );
                } else {
                    $w->whereRaw(
                        "TRIM(JSON_UNQUOTE(JSON_EXTRACT(payload_json, '$.loadnumber'))) = ?",
                        [$loadNumber]
                    );
                }
            }
        });

        return $q->orderBy('id')->get();
    }

    private function formatQueueStatusFromStage(string $stage): string
    {
        return match ($stage) {
            StageResolver::STAGE_DELIVERED_CONFIRMED => 'DELIVERED-CONFIRMED',
            StageResolver::STAGE_DELIVERED_PENDING   => 'DELIVERED-PENDING',
            StageResolver::STAGE_IN_TRANSIT          => 'IN TRANSIT',
            StageResolver::STAGE_AT_TERMINAL         => 'AT TERMINAL',
            default                                  => strtoupper(str_replace('_', ' ', $stage)),
        };
    }
}
