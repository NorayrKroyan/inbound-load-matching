<?php

namespace App\Services\InboundLoadMatching\Parsing;

use App\Services\InboundLoadMatching\Support\Str;

class StageResolver
{
    public const STAGE_AT_TERMINAL         = 'AT_TERMINAL';
    public const STAGE_IN_TRANSIT          = 'IN_TRANSIT';
    public const STAGE_DELIVERED_PENDING   = 'DELIVERED_PENDING';
    public const STAGE_DELIVERED_CONFIRMED = 'DELIVERED_CONFIRMED';

    public const STAGE_RANK = [
        self::STAGE_AT_TERMINAL         => 1,
        self::STAGE_IN_TRANSIT          => 2,
        self::STAGE_DELIVERED_PENDING   => 3,
        self::STAGE_DELIVERED_CONFIRMED => 4,
    ];

    public function __construct(private readonly Str $str) {}

    public function stageRank(string $stage): int
    {
        return self::STAGE_RANK[$stage] ?? 1;
    }

    public function rankToStage(int $rank): string
    {
        return match ($rank) {
            4 => self::STAGE_DELIVERED_CONFIRMED,
            3 => self::STAGE_DELIVERED_PENDING,
            2 => self::STAGE_IN_TRANSIT,
            default => self::STAGE_AT_TERMINAL,
        };
    }

    public function determineStage(?string $payloadJson, ?string $stateRaw): string
    {
        $state = strtoupper(trim((string)($stateRaw ?? '')));
        $state = str_replace(['-', ' '], ['_', '_'], $state);

        if ($state === 'ATTERMINAL') $state = 'AT_TERMINAL';
        if ($state === 'INTRANSIT') $state = 'IN_TRANSIT';

        if (str_contains($state, 'AT_TERMINAL')) return self::STAGE_AT_TERMINAL;
        if (str_contains($state, 'IN_TRANSIT')) return self::STAGE_IN_TRANSIT;

        if (str_contains($state, 'DELIVERED')) {
            $reconcile = $this->extractReconcileStatus($payloadJson);
            if ($reconcile === 'CONFIRMED') return self::STAGE_DELIVERED_CONFIRMED;
            return self::STAGE_DELIVERED_PENDING;
        }

        $reconcile = $this->extractReconcileStatus($payloadJson);
        if ($reconcile === 'CONFIRMED') return self::STAGE_DELIVERED_CONFIRMED;
        if ($reconcile === 'PENDING') return self::STAGE_DELIVERED_PENDING;

        return self::STAGE_AT_TERMINAL;
    }

    public function extractReconcileStatus(?string $payloadJson): ?string
    {
        if (!$payloadJson || !is_string($payloadJson)) return null;
        $d = json_decode($payloadJson, true);
        if (!is_array($d)) return null;

        $v = $d['reconcile_status'] ?? $d['reconcileStatus'] ?? null;
        $v = $this->str->strOrNull($v);
        if (!$v) return null;

        $u = strtoupper(trim($v));
        if ($u === 'CONFIRMED') return 'CONFIRMED';
        if ($u === 'PENDING') return 'PENDING';
        return $u;
    }
}
