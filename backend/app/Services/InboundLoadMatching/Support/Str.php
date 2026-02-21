<?php

namespace App\Services\InboundLoadMatching\Support;

class Str
{
    public function strOrNull($v): ?string
    {
        if ($v === null) return null;
        $s = trim((string)$v);
        return $s === '' ? null : $s;
    }

    public function norm(string $s): string
    {
        $s = strtolower(trim($s));
        $s = preg_replace('/[^\p{L}\p{N}\s]+/u', ' ', $s);
        $s = preg_replace('/\s+/u', ' ', $s);
        return trim($s);
    }

    public function normTruck(string $s): string
    {
        $s = trim($s);
        return strtolower((string)preg_replace('/[^a-zA-Z0-9]+/', '', $s));
    }

    public function squashDoubles(string $s): string
    {
        return (string)preg_replace('/(.)\1+/u', '$1', $s);
    }
}
