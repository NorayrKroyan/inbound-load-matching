<?php

namespace App\Services\InboundLoadMatching\Parsing;

class WeightExtractor
{
    public function extractWeightsFromPayload(?string $payloadJson): array
    {
        $net = null;
        $box1 = null; // box numbers: 2608, 6835
        $box2 = null;

        if ($payloadJson) {
            $d = json_decode($payloadJson, true);

            if (is_array($d)) {
                // ✅ net weight: prefer total_weight (matches your payload)
                $net = $this->toFloatOrNull($d['total_weight'] ?? null);

                if ($net === null) {
                    $net = $this->toFloatOrNull($d['total_lbs'] ?? $d['net_lbs'] ?? $d['netlbs'] ?? $d['net'] ?? null);
                }

                // ✅ BOX numbers: prefer explicit box_numbers "2608,6835"
                if (isset($d['box_numbers'])) {
                    $parts = array_map('trim', explode(',', (string) $d['box_numbers']));
                    $nums = array_map(fn($x) => $this->toFloatOrNull($x), $parts);
                    $nums = array_values(array_filter($nums, fn($x) => $x !== null));
                    $box1 = $nums[0] ?? $box1;
                    $box2 = $nums[1] ?? $box2;
                }

                // ✅ Fallback: parse from "weight": "2608 (22,060)\n6835 (20,710)"
                if (isset($d['weight'])) {
                    $w = (string) $d['weight'];

                    // Box numbers = before parentheses
                    preg_match_all('/(^|\n)\s*([0-9,]+)\s*\(/m', $w, $mBox);
                    $boxNums = [];
                    foreach (($mBox[2] ?? []) as $raw) {
                        $v = $this->toFloatOrNull($raw);
                        if ($v !== null) $boxNums[] = (int) $v;
                    }

                    if ($box1 === null) $box1 = $boxNums[0] ?? null;
                    if ($box2 === null) $box2 = $boxNums[1] ?? null;

                    // If net_lbs still missing: sum numbers inside parentheses as fallback
                    // (These are typically per-box weights; sum gives net)
                    preg_match_all('/\(\s*([0-9,]+)\s*\)/m', $w, $mW);
                    $weightNums = [];
                    foreach (($mW[1] ?? []) as $raw) {
                        $v = $this->toFloatOrNull($raw);
                        if ($v !== null) $weightNums[] = $v;
                    }

                    if ($net === null && count($weightNums) > 0) {
                        $sum = 0.0;
                        foreach ($weightNums as $n) $sum += $n;
                        $net = ($sum > 0) ? $sum : null;
                    }
                }
            }
        }

        return [
            'box1' => $box1,
            'box2' => $box2,
            'net_lbs' => ($net !== null && $net > 0) ? $net : null,
        ];
    }

    public function toFloatOrNull($v): ?float
    {
        if ($v === null) return null;
        if (is_numeric($v)) return (float) $v;

        $s = trim((string) $v);
        if ($s === '') return null;

        $s = preg_replace('/[^0-9.\-]/', '', $s);
        return is_numeric($s) ? (float) $s : null;
    }
}
