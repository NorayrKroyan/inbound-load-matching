<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Services\InboundLoadMatching\InboundLoadMatchingService;

class InboundLoadsAutoProcessTickCommand extends Command
{
    protected $signature = 'inbound-loads:auto-process-tick';
    protected $description = 'Run inbound load matching auto-process tick';

    public function handle(InboundLoadMatchingService $service): int
    {
        $result = $service->runAutoProcessTick();

        $this->line(json_encode($result, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));

        return self::SUCCESS;
    }
}
