<?php

namespace App\Services\InboundLoadMatching\Matching;

use Illuminate\Support\Facades\DB;
use App\Services\InboundLoadMatching\Support\Str;

class DriverMatcher
{
    public function __construct(private readonly Str $str) {}

    public function matchDriver(?string $driverName, ?string $truckNumber): array
    {
        $nameDriver = null;
        $truckDriver = null;
        $notes = [];

        $driverCols = ['id_driver', 'id_contact', 'id_vehicle', 'id_carrier'];

        if ($driverName) {
            [$contact, $method, $note] = $this->findContactForDriverName($driverName);
            if ($note) $notes[] = $note;

            if ($contact) {
                $drv = DB::connection()->table('driver')
                    ->select($driverCols)
                    ->where('id_contact', $contact->id_contact)
                    ->first();

                if ($drv) {
                    $nameDriver = [
                        'method' => $method,
                        'id_driver' => (int)$drv->id_driver,
                        'id_contact' => (int)$drv->id_contact,
                        'id_vehicle' => $drv->id_vehicle ? (int)$drv->id_vehicle : null,
                        'id_carrier' => $drv->id_carrier ? (int)$drv->id_carrier : null,
                    ];
                } else {
                    $notes[] = "Contact matched by name but no driver row found for id_contact={$contact->id_contact}.";
                }
            }
        }

        if ($truckNumber) {
            $truckNorm = strtolower($this->str->normTruck($truckNumber));

            $veh = DB::connection()->table('vehicle')
                ->select(['id_vehicle', 'vehicle_number', 'vehicle_name'])
                ->where(function ($q) use ($truckNorm) {
                    $q->whereRaw("LOWER(TRIM(vehicle_number)) = ?", [$truckNorm])
                        ->orWhereRaw("LOWER(TRIM(vehicle_name)) = ?", [$truckNorm]);
                })
                ->first();

            if ($veh) {
                $drv = DB::connection()->table('driver')
                    ->select($driverCols)
                    ->where('id_vehicle', $veh->id_vehicle)
                    ->first();

                if ($drv) {
                    $truckDriver = [
                        'method' => 'TRUCK',
                        'id_driver' => (int)$drv->id_driver,
                        'id_contact' => (int)$drv->id_contact,
                        'id_vehicle' => (int)$drv->id_vehicle,
                        'id_carrier' => $drv->id_carrier ? (int)$drv->id_carrier : null,
                    ];
                }
            }
        }

        $resolved = null;
        $status = 'NONE';

        if ($nameDriver && $truckDriver) {
            if ($nameDriver['id_driver'] === $truckDriver['id_driver']) {
                $status = 'CONFIRMED';
                $resolved = $nameDriver;
                $resolved['method'] = $nameDriver['method'] . '+TRUCK';
            } else {
                $status = 'CONFLICT';
                $resolved = $nameDriver;
                $notes[] = "Conflict: name matched driver {$nameDriver['id_driver']} but truck matched driver {$truckDriver['id_driver']}.";
            }
        } elseif ($nameDriver) {
            $status = 'NAME_ONLY';
            $resolved = $nameDriver;
        } elseif ($truckDriver) {
            $status = 'TRUCK_ONLY';
            $resolved = $truckDriver;
        }

        return [
            'status' => $status,
            'resolved' => $resolved,
            'by_name' => $nameDriver,
            'by_truck' => $truckDriver,
            'notes' => implode(' ', array_filter($notes)),
        ];
    }

    public function computeConfidence(array $driverMatch): string
    {
        return match ($driverMatch['status'] ?? 'NONE') {
            'CONFIRMED' => 'GREEN',
            'NAME_ONLY', 'TRUCK_ONLY', 'CONFLICT' => 'YELLOW',
            default => 'RED',
        };
    }

    private function findContactForDriverName(string $driverName): array
    {
        $normFull = $this->str->norm($driverName);

        $contact = DB::connection()->table('contact')
            ->select(['id_contact', 'first_name', 'last_name'])
            ->whereRaw("LOWER(TRIM(CONCAT(TRIM(first_name), ' ', TRIM(last_name)))) = ?", [$normFull])
            ->first();

        if ($contact) return [$contact, 'NAME_EXACT', ''];

        $likeCands = DB::connection()->table('contact')
            ->select(['id_contact', 'first_name', 'last_name'])
            ->whereRaw("LOWER(TRIM(CONCAT(TRIM(first_name), ' ', TRIM(last_name)))) LIKE ?", ['%' . $normFull . '%'])
            ->limit(20)
            ->get();

        if (count($likeCands) === 1) return [$likeCands[0], 'NAME_LIKE_UNIQUE', ''];

        [$first, $last] = $this->splitName($driverName);
        $firstN = $first ? $this->str->norm($first) : '';
        $lastN  = $last ? $this->str->norm($last) : '';

        if ($firstN === '' && $lastN === '') return [null, 'NAME_NONE', 'Driver name is empty after normalization.'];

        $qb = DB::connection()->table('contact')->select(['id_contact', 'first_name', 'last_name']);
        if ($lastN !== '') $qb->whereRaw("SOUNDEX(TRIM(last_name)) = SOUNDEX(?)", [$lastN]);
        else $qb->whereRaw("SOUNDEX(TRIM(first_name)) = SOUNDEX(?)", [$firstN]);

        $pool = $qb->limit(80)->get();

        if (count($pool) === 0 && $firstN !== '') {
            $pool = DB::connection()->table('contact')
                ->select(['id_contact', 'first_name', 'last_name'])
                ->whereRaw("LOWER(TRIM(first_name)) LIKE ?", [substr($firstN, 0, 4) . '%'])
                ->limit(80)
                ->get();
        }

        if (count($pool) === 0) {
            if (count($likeCands) > 1) return [null, 'NAME_NONE', 'Driver name ambiguous (multiple contacts match).'];
            return [null, 'NAME_NONE', 'No contact match found by exact/like/fuzzy.'];
        }

        $best = null;
        $bestScore = 9999;
        $secondScore = 9999;

        foreach ($pool as $c) {
            $full = trim(($c->first_name ?? '') . ' ' . ($c->last_name ?? ''));
            $score = $this->nameDistance($driverName, $full);

            if ($score < $bestScore) {
                $secondScore = $bestScore;
                $bestScore = $score;
                $best = $c;
            } elseif ($score < $secondScore) {
                $secondScore = $score;
            }
        }

        if ($best && $bestScore <= 2 && ($secondScore - $bestScore) >= 1) {
            return [$best, 'NAME_FUZZY', "Fuzzy name match used (distance={$bestScore})."];
        }

        if (count($likeCands) > 1) return [null, 'NAME_NONE', 'Driver name ambiguous (multiple contacts match).'];

        return [null, 'NAME_NONE', 'No contact match found by exact/like/fuzzy.'];
    }

    private function splitName(string $name): array
    {
        $name = trim($name);
        if ($name === '') return [null, null];

        if (str_contains($name, ',')) {
            [$a, $b] = array_map('trim', explode(',', $name, 2));
            return [$b ?: null, $a ?: null];
        }

        $parts = preg_split('/\s+/u', $name);
        $parts = array_values(array_filter($parts, fn($p) => $p !== ''));

        if (count($parts) === 1) return [$parts[0], null];
        return [$parts[0], $parts[count($parts) - 1]];
    }

    private function nameDistance(string $a, string $b): int
    {
        $aN = $this->str->squashDoubles($this->str->norm($a));
        $bN = $this->str->squashDoubles($this->str->norm($b));
        return levenshtein($aN, $bN);
    }
}
