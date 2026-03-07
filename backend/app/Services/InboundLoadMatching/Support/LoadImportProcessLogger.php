<?php

namespace App\Services\InboundLoadMatching\Support;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Throwable;

class LoadImportProcessLogger
{
    public function newSessionId(): string
    {
        return (string) Str::uuid();
    }

    public function info(
        string $sessionId,
        int $loadimportId,
        string $runType,
        string $event,
        string $status,
        string $message,
        ?string $stageDetected = null,
        ?int $idLoad = null,
        ?int $idLoadDetail = null,
        array $context = []
    ): void {
        $this->write(
            sessionId: $sessionId,
            loadimportId: $loadimportId,
            idLoad: $idLoad,
            idLoadDetail: $idLoadDetail,
            runType: $runType,
            event: $event,
            severity: 'info',
            status: $status,
            stageDetected: $stageDetected,
            message: $message,
            context: $context
        );
    }

    public function warning(
        string $sessionId,
        int $loadimportId,
        string $runType,
        string $event,
        string $status,
        string $message,
        ?string $stageDetected = null,
        ?int $idLoad = null,
        ?int $idLoadDetail = null,
        array $context = []
    ): void {
        $this->write(
            sessionId: $sessionId,
            loadimportId: $loadimportId,
            idLoad: $idLoad,
            idLoadDetail: $idLoadDetail,
            runType: $runType,
            event: $event,
            severity: 'warning',
            status: $status,
            stageDetected: $stageDetected,
            message: $message,
            context: $context
        );
    }

    public function error(
        string $sessionId,
        int $loadimportId,
        string $runType,
        string $event,
        string $status,
        string $message,
        ?string $stageDetected = null,
        ?int $idLoad = null,
        ?int $idLoadDetail = null,
        array $context = []
    ): void {
        $this->write(
            sessionId: $sessionId,
            loadimportId: $loadimportId,
            idLoad: $idLoad,
            idLoadDetail: $idLoadDetail,
            runType: $runType,
            event: $event,
            severity: 'error',
            status: $status,
            stageDetected: $stageDetected,
            message: $message,
            context: $context
        );
    }

    public function exception(
        string $sessionId,
        int $loadimportId,
        string $runType,
        string $event,
        Throwable $e,
        ?string $stageDetected = null,
        ?int $idLoad = null,
        ?int $idLoadDetail = null,
        array $context = []
    ): void {
        $this->write(
            sessionId: $sessionId,
            loadimportId: $loadimportId,
            idLoad: $idLoad,
            idLoadDetail: $idLoadDetail,
            runType: $runType,
            event: $event,
            severity: 'error',
            status: 'exception',
            stageDetected: $stageDetected,
            message: $e->getMessage(),
            context: array_merge($context, [
                'exception_class' => get_class($e),
                'file' => $e->getFile(),
                'line' => $e->getLine(),
                'trace' => $e->getTraceAsString(),
            ])
        );
    }

    protected function write(
        string $sessionId,
        int $loadimportId,
        ?int $idLoad,
        ?int $idLoadDetail,
        string $runType,
        string $event,
        string $severity,
        string $status,
        ?string $stageDetected,
        string $message,
        array $context = []
    ): void {
        DB::table('loadimport_process_log')->insert([
            'session_id'     => $sessionId,
            'loadimport_id'  => $loadimportId,
            'id_load'        => $idLoad,
            'id_load_detail' => $idLoadDetail,
            'run_type'       => $runType,
            'event'          => $event,
            'severity'       => $severity,
            'status'         => $status,
            'stage_detected' => $stageDetected,
            'message'        => $message,
            'context_json'   => empty($context)
                ? null
                : json_encode($context, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
            'created_at'     => now(),
        ]);
    }
}
