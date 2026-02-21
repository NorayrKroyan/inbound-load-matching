<?php

namespace App\Services\InboundLoadMatching\Matching;

use Illuminate\Support\Facades\DB;
use App\Services\InboundLoadMatching\Support\DbSchema;
use App\Services\InboundLoadMatching\Support\Str;

class LocationMatcher
{
    public function __construct(
        private readonly DbSchema $schema,
        private readonly Str $str
    ) {}

    public function matchPullPointByTerminal(?string $terminal): array
    {
        $terminal = $this->str->strOrNull($terminal);
        if (!$terminal) return ['status' => 'NONE', 'resolved' => null, 'candidates' => [], 'notes' => 'No terminal in import.'];

        $table = $this->schema->tableExists('pull_points') ? 'pull_points' : 'pull_point';

        $t = strtolower(trim($terminal));
        $t = preg_replace('/\s+/u', ' ', $t);
        $t = preg_replace('/\s*-\s*/', '-', $t);

        $row = DB::connection()->table($table)
            ->select(['id_pull_point', 'pp_job'])
            ->where('is_deleted', 0)
            ->whereRaw("
                LOWER(
                    REPLACE(
                        REPLACE(TRIM(pp_job), ' - ', '-'),
                        ' -','-'
                    )
                ) = ?
            ", [$t])
            ->first();

        if ($row) {
            return [
                'status' => 'ONE',
                'resolved' => [
                    'id_pull_point' => (int)$row->id_pull_point,
                    'pp_job' => $row->pp_job,
                    'method' => 'NORMALIZED_EXACT',
                ],
                'candidates' => [],
                'notes' => '',
            ];
        }

        $cands = DB::connection()->table($table)
            ->select(['id_pull_point', 'pp_job'])
            ->where('is_deleted', 0)
            ->whereRaw('LOWER(TRIM(pp_job)) LIKE ?', ['%' . $t . '%'])
            ->limit(10)
            ->get();

        if (count($cands) === 1) {
            return [
                'status' => 'ONE',
                'resolved' => [
                    'id_pull_point' => (int)$cands[0]->id_pull_point,
                    'pp_job' => $cands[0]->pp_job,
                    'method' => 'LIKE_UNIQUE',
                ],
                'candidates' => [],
                'notes' => '',
            ];
        }

        if (count($cands) > 1) {
            return [
                'status' => 'MULTI',
                'resolved' => null,
                'candidates' => $cands->map(fn($x) => [
                    'id_pull_point' => (int)$x->id_pull_point,
                    'pp_job' => $x->pp_job,
                ])->all(),
                'notes' => 'Multiple pull points match.',
            ];
        }

        return ['status' => 'NONE', 'resolved' => null, 'candidates' => [], 'notes' => 'No pull point match found.'];
    }

    public function matchPadLocationByJobname(?string $jobname): array
    {
        $jobname = $this->str->strOrNull($jobname);
        if (!$jobname) return ['status' => 'NONE', 'resolved' => null, 'candidates' => [], 'notes' => 'No jobname in import.'];

        $norm = strtolower(trim($jobname));
        $norm = preg_replace('/\s+/u', ' ', $norm);
        $normLoose = preg_replace('/\s*\/\s*/u', '/', $norm);

        $exact = DB::connection()->table('pad_location')
            ->select(['id_pad_location', 'pl_job'])
            ->where('is_deleted', 0)
            ->whereRaw("LOWER(TRIM(pl_job)) = ?", [$norm])
            ->first();

        if ($exact) {
            return [
                'status' => 'ONE',
                'resolved' => [
                    'id_pad_location' => (int)$exact->id_pad_location,
                    'pl_job' => $exact->pl_job,
                    'method' => 'EXACT',
                ],
                'candidates' => [],
                'notes' => '',
            ];
        }

        $cands = DB::connection()->table('pad_location')
            ->select(['id_pad_location', 'pl_job'])
            ->where('is_deleted', 0)
            ->where(function ($q) use ($norm, $normLoose) {
                $q->whereRaw("LOWER(TRIM(pl_job)) LIKE ?", ['%' . $norm . '%'])
                    ->orWhereRaw("LOWER(TRIM(pl_job)) LIKE ?", ['%' . $normLoose . '%'])
                    ->orWhereRaw("? LIKE CONCAT('%', LOWER(TRIM(pl_job)), '%')", [$norm])
                    ->orWhereRaw("? LIKE CONCAT('%', LOWER(TRIM(pl_job)), '%')", [$normLoose]);
            })
            ->limit(10)
            ->get();

        if (count($cands) === 1) {
            return [
                'status' => 'ONE',
                'resolved' => [
                    'id_pad_location' => (int)$cands[0]->id_pad_location,
                    'pl_job' => $cands[0]->pl_job,
                    'method' => 'LIKE_UNIQUE',
                ],
                'candidates' => [],
                'notes' => '',
            ];
        }

        if (count($cands) > 1) {
            return [
                'status' => 'MULTI',
                'resolved' => null,
                'candidates' => $cands->map(fn($x) => [
                    'id_pad_location' => (int)$x->id_pad_location,
                    'pl_job' => $x->pl_job,
                ])->all(),
                'notes' => 'Jobname ambiguous: multiple pad_location matches.',
            ];
        }

        return ['status' => 'NONE', 'resolved' => null, 'candidates' => [], 'notes' => 'No pad_location match found.'];
    }
}
