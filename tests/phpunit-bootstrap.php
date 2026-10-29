<?php

declare(strict_types=1);

$redisPath = __DIR__.'/../src/Database/Adapter/Redis.php';
if (! is_file($redisPath) || ! str_starts_with((string) file_get_contents($redisPath, false, null, 0, 5), '<?php')) {
    $restore = __DIR__.'/../bin/restore-pr-1048-sources';
    if (is_file($restore)) {
        passthru('bash '.escapeshellarg($restore), $exitCode);
        if ($exitCode !== 0) {
            fwrite(STDERR, "Failed to restore PR #1048 adapter sources (exit {$exitCode})\n");
            exit(1);
        }
    }
}

require __DIR__.'/../vendor/autoload.php';
