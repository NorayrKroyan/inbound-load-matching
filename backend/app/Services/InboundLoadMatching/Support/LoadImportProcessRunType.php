<?php

namespace App\Services\InboundLoadMatching\Support;

final class LoadImportProcessRunType
{
    public const SINGLE_PROCESS = 'single_process';
    public const BATCH_PROCESS = 'batch_process';
    public const AUTO_PROCESS = 'auto_process';

    public static function all(): array
    {
        return [
            self::SINGLE_PROCESS,
            self::BATCH_PROCESS,
            self::AUTO_PROCESS,
        ];
    }

    public static function label(?string $value): string
    {
        return match ($value) {
            self::SINGLE_PROCESS => 'Single Process',
            self::BATCH_PROCESS => 'Batch Process',
            self::AUTO_PROCESS => 'Auto Process',
            default => (string) $value,
        };
    }
}
