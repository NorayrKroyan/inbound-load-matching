<?php

namespace App\Services\InboundLoadMatching\Support;

use Illuminate\Support\Facades\Schema;

class DbSchema
{
    public function tableExists(string $table): bool
    {
        try { return Schema::hasTable($table); } catch (\Throwable $e) { return false; }
    }

    public function columnExists(string $table, string $column): bool
    {
        try { return Schema::hasColumn($table, $column); } catch (\Throwable $e) { return false; }
    }
}
