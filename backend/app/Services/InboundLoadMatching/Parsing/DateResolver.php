<?php

namespace App\Services\InboundLoadMatching\Parsing;

use Carbon\Carbon;
use App\Services\InboundLoadMatching\Support\Str;

class DateResolver
{
    public function __construct(private readonly Str $str) {}

    /**
     * Doc rule:
     * - load_date should come from payload.datetime_at_terminal OR fallback,
     *   and then NEVER updated later.
     */
    public function guessLoadDateFromAtTerminal(?string $payloadJson, ?string $createdAt): ?string
    {
        $atTerm = null;

        if ($payloadJson && is_string($payloadJson)) {
            $d = json_decode($payloadJson, true);
            if (is_array($d)) $atTerm = $this->str->strOrNull($d['datetime_at_terminal'] ?? null);
        }

        if ($atTerm) {
            $mdy = $this->parseMdyToDash($atTerm);
            if ($mdy) return $mdy;

            try {
                return Carbon::parse($atTerm)->format('m-d-Y');
            } catch (\Throwable $e) {}
        }

        if ($createdAt) {
            $mdy = $this->parseMdyToDash((string)$createdAt);
            if ($mdy) return $mdy;
            try {
                return Carbon::parse((string)$createdAt)->format('m-d-Y');
            } catch (\Throwable $e) {}
        }

        return null;
    }

    /**
     * Your requirement:
     * - For delivery time use datetime_delivered (primary)
     */
    public function resolveDeliveryTimeStringFromPayload(?string $payloadJson, ?string $fallbackFromParsed): ?string
    {
        $fallbackFromParsed = $this->str->strOrNull($fallbackFromParsed);

        if ($payloadJson && is_string($payloadJson)) {
            $d = json_decode($payloadJson, true);
            if (is_array($d)) {
                // ✅ primary per your note
                $v =
                    $this->str->strOrNull($d['datetime_delivered'] ?? null)
                    ?? $this->str->strOrNull($d['datetime_at_destination'] ?? null)
                    ?? $this->str->strOrNull($d['delivery_time'] ?? null)
                    ?? $this->str->strOrNull($d['status_time'] ?? null);

                if ($v) return $this->cleanDeliveryTime($v);
            }
        }

        return $fallbackFromParsed ? $this->cleanDeliveryTime($fallbackFromParsed) : null;
    }

    public function cleanDeliveryTime(?string $s): ?string
    {
        $s = $this->str->strOrNull($s);
        if (!$s) return null;

        // strips prefixes like "archive 02/13/2026 10:15 AM"
        $s = preg_replace('/^[A-Za-z_\-]+\s*:\s*/u', '', $s);
        $s = preg_replace('/^[A-Za-z_\-]+\s+/u', '', $s);

        return $this->str->strOrNull($s);
    }

    public function toMysqlDatetime(string $s): ?string
    {
        $s = trim($s);
        if ($s === '') return null;

        if (preg_match('/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$/', $s)) return $s;
        if (preg_match('/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$/', $s)) return $s . ':00';
        if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $s)) return $s . ' 00:00:00';

        $formats = [
            'm/d/Y h:i A',
            'm/d/Y h:i:s A',
            'm-d-Y h:i A',
            'm-d-Y h:i:s A',
            'n/j/Y g:i A',
            'n/j/Y g:i:s A',
        ];

        foreach ($formats as $fmt) {
            try {
                $dt = Carbon::createFromFormat($fmt, $s);
                return $dt->format('Y-m-d H:i:s');
            } catch (\Throwable $e) {}
        }

        try {
            return Carbon::parse($s)->format('Y-m-d H:i:s');
        } catch (\Throwable $e) {
            return null;
        }
    }

    private function parseMdyToDash(string $s): ?string
    {
        if (preg_match('/^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/', $s, $m)) {
            $mm = str_pad($m[1], 2, '0', STR_PAD_LEFT);
            $dd = str_pad($m[2], 2, '0', STR_PAD_LEFT);
            return "{$mm}-{$dd}-{$m[3]}";
        }
        if (preg_match('/^(\d{1,2})-(\d{1,2})-(\d{4})\b/', $s, $m)) {
            $mm = str_pad($m[1], 2, '0', STR_PAD_LEFT);
            $dd = str_pad($m[2], 2, '0', STR_PAD_LEFT);
            return "{$mm}-{$dd}-{$m[3]}";
        }
        return null;
    }
}
