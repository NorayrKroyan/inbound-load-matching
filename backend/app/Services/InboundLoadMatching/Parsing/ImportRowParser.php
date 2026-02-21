<?php

namespace App\Services\InboundLoadMatching\Parsing;

use App\Services\InboundLoadMatching\Support\Str;

class ImportRowParser
{
    public function __construct(private readonly Str $str) {}

    public function parseImportRow(object $r): array
    {
        $data = [];
        $payloadJson = $r->payload_json ?? null;
        if ($payloadJson) {
            $decoded = json_decode($payloadJson, true);
            if (is_array($decoded)) $data = $decoded;
        }

        $jobname = $this->str->strOrNull($r->jobname ?? null) ?? $this->str->strOrNull($data['jobname'] ?? null);
        $terminal = $this->str->strOrNull($r->terminal ?? null) ?? $this->str->strOrNull($data['terminal'] ?? null);
        $state = $this->str->strOrNull($r->state ?? null) ?? $this->str->strOrNull($data['status'] ?? $data['state'] ?? null);

        // delivery_time in parsed is only a fallback; DateResolver will prefer payload.datetime_delivered
        $deliveryTime =
            $this->str->strOrNull($r->delivery_time ?? null)
            ?? $this->str->strOrNull($data['delivery_time'] ?? null)
            ?? $this->str->strOrNull($data['datetime_delivered'] ?? null)
            ?? $this->str->strOrNull($data['datetime_at_destination'] ?? null)
            ?? $this->str->strOrNull($data['status_time'] ?? null);

        $loadNumber = $this->str->strOrNull($r->load_number ?? null)
            ?? $this->str->strOrNull($data['loadnumber'] ?? $data['load_number'] ?? null);

        $ticketNumber = $this->str->strOrNull($r->ticket_number ?? null)
            ?? $this->str->strOrNull($data['ticket_no'] ?? $data['ticket_number'] ?? null);

        $carrierStr = $this->str->strOrNull($r->carrier ?? null) ?? $this->str->strOrNull($data['carrier'] ?? null);

        $truckStr =
            $this->str->strOrNull($r->truck ?? null)
            ?? $this->str->strOrNull($data['truck_trailer'] ?? null)
            ?? $this->str->strOrNull($data['truck_number'] ?? null)
            ?? $this->str->strOrNull($data['truck'] ?? null);

        $originalStr = $this->str->strOrNull($r->payload_original ?? null);

        if (!$carrierStr && $originalStr) $carrierStr = $originalStr;
        if (!$truckStr && $originalStr) $truckStr = $originalStr;

        $driverName = $this->extractDriverName($carrierStr);
        $truckNumber = $this->extractTruckNumber($truckStr);
        $trailerNumber = $this->extractTrailerNumber($truckStr);

        $trailerExplicit =
            $this->str->strOrNull($data['trailer_number'] ?? null)
            ?? $this->str->strOrNull($data['trailer'] ?? null)
            ?? $this->str->strOrNull($data['trailer_no'] ?? null);

        if ($trailerExplicit) $trailerNumber = $trailerExplicit;

        return [
            'driver_name' => $driverName,
            'truck_number' => $truckNumber,
            'trailer_number' => $trailerNumber,

            'jobname' => $this->str->strOrNull($jobname),
            'terminal' => $this->str->strOrNull($terminal),
            'load_number' => $loadNumber,
            'ticket_number' => $ticketNumber,
            'state' => $state,
            'delivery_time' => $deliveryTime,

            'raw_carrier' => $carrierStr,
            'raw_truck' => $truckStr,
            'raw_original' => $originalStr,
        ];
    }

    private function extractDriverName(?string $carrier): ?string
    {
        if (!$carrier) return null;

        $lines = preg_split("/\r\n|\n|\r/", $carrier);
        if (is_array($lines) && count($lines) >= 2) {
            $maybe = $this->str->strOrNull($lines[1]);
            if ($maybe) return $maybe;
        }

        if (preg_match('/\b([A-Z][a-z]+)\s+([A-Z][a-z]+)\b/', $carrier, $m)) {
            return $this->str->strOrNull($m[1] . ' ' . $m[2]);
        }

        return $this->str->strOrNull($carrier);
    }

    private function extractTruckNumber(?string $text): ?string
    {
        if (!$text) return null;

        if (preg_match('/Truck\s*#?:?\s*([A-Za-z0-9]+)/i', $text, $m)) {
            return $this->str->strOrNull($m[1]);
        }

        if (preg_match('/^\s*([A-Za-z0-9]+)\s*[\/\-]\s*([A-Za-z0-9]+)\s*$/', trim($text), $m)) {
            return $this->str->strOrNull($m[1]);
        }

        $t = trim($text);
        if ($t !== '' && preg_match('/^[A-Za-z0-9]+$/', $t)) {
            return $this->str->strOrNull($t);
        }

        return null;
    }

    private function extractTrailerNumber(?string $text): ?string
    {
        if (!$text) return null;

        if (preg_match('/Trailer\s*#?:?\s*([A-Za-z0-9]+)/i', $text, $m)) {
            return $this->str->strOrNull($m[1]);
        }

        if (preg_match('/^\s*([A-Za-z0-9]+)\s*[\/\-]\s*([A-Za-z0-9]+)\s*$/', trim($text), $m)) {
            return $this->str->strOrNull($m[2]);
        }

        return null;
    }
}
