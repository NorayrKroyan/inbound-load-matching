<?php

namespace App\Services\InboundLoadMatching\Matching;

use Illuminate\Support\Facades\DB;

class JourneyResolver
{
    public function buildJourney(array $pullPointMatch, array $padLocationMatch): array
    {
        $pp = $pullPointMatch['resolved'] ?? null;
        $pl = $padLocationMatch['resolved'] ?? null;

        if (!$pp || !$pl) {
            return [
                'status' => ($pp || $pl) ? 'PARTIAL' : 'NONE',
                'pull_point_id' => $pp ? (int)$pp['id_pull_point'] : null,
                'pad_location_id' => $pl ? (int)$pl['id_pad_location'] : null,
                'join_id' => null,
                'miles' => null,
                'method' => ($pp || $pl) ? 'PARTIAL' : 'NONE',
            ];
        }

        $ppId = (int)$pp['id_pull_point'];
        $plId = (int)$pl['id_pad_location'];

        $join = DB::connection()->table('join')
            ->select(['id_join', 'miles'])
            ->where('is_deleted', 0)
            ->where('id_pull_point', $ppId)
            ->where('id_pad_location', $plId)
            ->first();

        if ($join) {
            return [
                'status' => 'READY',
                'pull_point_id' => $ppId,
                'pad_location_id' => $plId,
                'join_id' => (int)$join->id_join,
                'miles' => $join->miles !== null ? (int)$join->miles : null,
                'method' => 'JOIN_LOOKUP(pp,pl)',
            ];
        }

        return [
            'status' => 'MISSING_JOIN',
            'pull_point_id' => $ppId,
            'pad_location_id' => $plId,
            'join_id' => null,
            'miles' => null,
            'method' => 'JOIN_NOT_FOUND',
        ];
    }
}
