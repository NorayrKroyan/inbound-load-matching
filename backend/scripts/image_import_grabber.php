<?php

if ($argc < 3) {
    fwrite(STDERR, "Usage: php image_import_grabber.php <source> <dest>\n");
    exit(2);
}

$source = trim((string)$argv[1]);
$dest   = trim((string)$argv[2]);

if ($source === '') {
    fwrite(STDERR, "SOURCE_EMPTY\n");
    exit(3);
}

if ($dest === '') {
    fwrite(STDERR, "DEST_EMPTY\n");
    exit(4);
}

$appRoot = realpath(__DIR__ . '/..');
if ($appRoot === false) {
    fwrite(STDERR, "APP_ROOT_NOT_FOUND\n");
    exit(5);
}

$sourceNorm = str_replace(['/', '\\'], DIRECTORY_SEPARATOR, $source);

$candidates = [
    $source,
    $appRoot . DIRECTORY_SEPARATOR . 'storage' . DIRECTORY_SEPARATOR . 'app' . DIRECTORY_SEPARATOR . 'public' . DIRECTORY_SEPARATOR . $sourceNorm,
    $appRoot . DIRECTORY_SEPARATOR . 'storage' . DIRECTORY_SEPARATOR . 'app' . DIRECTORY_SEPARATOR . $sourceNorm,
    $appRoot . DIRECTORY_SEPARATOR . 'public' . DIRECTORY_SEPARATOR . 'storage' . DIRECTORY_SEPARATOR . $sourceNorm,
    $appRoot . DIRECTORY_SEPARATOR . 'public' . DIRECTORY_SEPARATOR . $sourceNorm,
    $appRoot . DIRECTORY_SEPARATOR . $sourceNorm,
];

$sourceAbs = null;
foreach ($candidates as $candidate) {
    if (is_file($candidate)) {
        $sourceAbs = $candidate;
        break;
    }
}

if ($sourceAbs === null) {
    fwrite(STDERR, "SOURCE_NOT_FOUND: " . $source . "\n");
    exit(6);
}

$destDir = dirname($dest);
if (!is_dir($destDir) && !mkdir($destDir, 0775, true) && !is_dir($destDir)) {
    fwrite(STDERR, "DEST_DIR_CREATE_FAILED: " . $destDir . "\n");
    exit(7);
}

if (!copy($sourceAbs, $dest)) {
    fwrite(STDERR, "COPY_FAILED: " . $sourceAbs . " => " . $dest . "\n");
    exit(8);
}

if (!is_file($dest)) {
    fwrite(STDERR, "DEST_NOT_CREATED: " . $dest . "\n");
    exit(9);
}

echo $dest . PHP_EOL;
exit(0);
