<?php

namespace App\Services\InboundLoadMatching\Parsing;

use App\Services\InboundLoadMatching\Support\Str;

class BolExtractor
{
    public function __construct(private readonly Str $str) {}

    public function extractBolFromImportRow(object $r): array
    {
        // ✅ Priority 1: loadimports.image_path
        $imgPath = $this->str->strOrNull($r->image_path ?? null);
        if ($imgPath) {
            $type = $this->bolTypeFromPath($imgPath);
            return [
                'bol_path' => $imgPath,
                'bol_type' => $type,
            ];
        }

        // ✅ Priority 2: loadimports.payload_path + payload_json.pod_images[0]
        $payloadPath = $this->str->strOrNull($r->payload_path ?? null);
        $payloadJson = $r->payload_json ?? null;

        if ($payloadPath && is_string($payloadJson) && trim($payloadJson) !== '') {
            $d = json_decode($payloadJson, true);
            if (is_array($d)) {
                $first = null;

                // pod_images: ["file.jpg", ...]
                if (isset($d['pod_images']) && is_array($d['pod_images']) && count($d['pod_images']) > 0) {
                    $first = $this->str->strOrNull($d['pod_images'][0] ?? null);
                }

                // fallback keys (if vendor changes key name)
                if (!$first) {
                    $first =
                        $this->str->strOrNull($d['pod_image'] ?? null)
                        ?? $this->str->strOrNull($d['bol_image'] ?? null)
                        ?? $this->str->strOrNull($d['ticket_image'] ?? null);
                }

                if ($first) {
                    $joined = rtrim($payloadPath, "/\\") . '/' . ltrim($first, "/\\");
                    $type = $this->bolTypeFromPath($joined);
                    return [
                        'bol_path' => $joined,
                        'bol_type' => $type,
                    ];
                }
            }
        }

        // No BOL found
        return [
            'bol_path' => null,
            'bol_type' => null,
        ];
    }

    private function bolTypeFromPath(string $path): ?string
    {
        $p = strtolower($path);
        $p = preg_replace('/\?.*$/', '', $p);

        if (preg_match('/\.pdf$/', $p)) return 'pdf';
        if (preg_match('/\.(jpg|jpeg|png|gif|webp|bmp|tif|tiff)$/', $p)) return 'image';

        // Unknown extension => null (do not write)
        return null;
    }

    public function buildReplacedName(string $path): string
    {
        // foo.jpg => foo_REPLACED.jpg
        $q = '';
        if (str_contains($path, '?')) {
            [$path, $q] = explode('?', $path, 2);
            $q = '?' . $q;
        }

        $dot = strrpos($path, '.');
        if ($dot === false) return $path . '_REPLACED' . $q;

        $base = substr($path, 0, $dot);
        $ext = substr($path, $dot);
        return $base . '_REPLACED' . $ext . $q;
    }
}
