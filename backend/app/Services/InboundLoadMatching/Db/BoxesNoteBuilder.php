<?php

namespace App\Services\InboundLoadMatching\Db;

class BoxesNoteBuilder
{
    public function buildBoxesNote(?float $box1, ?float $box2): ?string
    {
        $a = ($box1 !== null) ? (int)round($box1) : null;
        $b = ($box2 !== null) ? (int)round($box2) : null;

        if ($a === null && $b === null) return null;

        if ($a !== null && $b !== null) return "BOXES:{$a},{$b}";
        if ($a !== null) return "BOXES:{$a}";
        return "BOXES:{$b}";
    }

    /**
     * Doc rule: "box numbers never change", so:
     * - If BOXES already present => keep as-is (do not overwrite)
     * - Else append boxes
     */
    public function mergeBoxesIntoNotesIfMissing(?string $cur, string $boxesNote): string
    {
        $cur = $cur !== null ? trim((string)$cur) : '';
        if ($cur === '') return $boxesNote;

        if (preg_match('/\bBOXES:[0-9,\s]+\b/i', $cur)) {
            return $cur; // ✅ never overwrite existing BOXES
        }

        return $cur . ' | ' . $boxesNote;
    }
}
