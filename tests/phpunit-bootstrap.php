<?php

declare(strict_types=1);

$redisPath = __DIR__.'/../src/Database/Adapter/Redis.php';
if (! is_file($redisPath) || ! str_starts_with((string) file_get_contents($redisPath, false, null, 0, 5), '<?php')) {
    $decompress = __DIR__.'/../bin/decompress-pr-1048-blobs';
    if (is_file($decompress)) {
        passthru('bash '.escapeshellarg($decompress), $exitCode);
        if ($exitCode !== 0) {
            fwrite(STDERR, "Failed to decompress PR #1048 adapter sources (exit {$exitCode})\n");
            exit(1);
        }
    }
}

require __DIR__.'/../vendor/autoload.php';
