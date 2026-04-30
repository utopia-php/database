<?php

/**
 * Cross-process worker for RedisCrossProcessTest. Re-attaches to an
 * existing Redis-backed namespace, reads a document, asserts the value
 * the parent wrote, then updates it. Exit 0 on success, 1 with stderr
 * detail on any failure.
 *
 * argv: [$script, $namespace, $documentId, $action]
 *   action: 'read-and-update'
 */

require __DIR__ . '/../../../../vendor/autoload.php';

use Utopia\Cache\Adapter\Redis as RedisCacheAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Redis as RedisDbAdapter;
use Utopia\Database\Database;
use Utopia\Database\Validator\Authorization;

\set_error_handler(static function (int $errno, string $errstr, string $errfile, int $errline): bool {
    \fwrite(\STDERR, "PHP error {$errno}: {$errstr} at {$errfile}:{$errline}\n");
    return false;
});

try {
    /** @var array<int, string> $argv */
    $argv = $_SERVER['argv'] ?? [];
    $argc = \count($argv);

    if ($argc < 4) {
        throw new \RuntimeException('Usage: redis_cross_process_worker.php <namespace> <documentId> <action>');
    }

    [$_script, $namespace, $documentId, $action] = $argv;

    if ($action !== 'read-and-update') {
        throw new \RuntimeException("Unsupported action: {$action}");
    }

    $host = \getenv('REDIS_HOST') ?: 'redis-mirror';
    $port = (int) (\getenv('REDIS_PORT') ?: 6379);

    $redis = new \Redis();
    $redis->connect($host, $port);

    $cacheRedis = new \Redis();
    $cacheRedis->connect(\getenv('CACHE_REDIS_HOST') ?: 'redis', (int) (\getenv('CACHE_REDIS_PORT') ?: 6379));

    $authorization = new Authorization();
    $authorization->addRole('any');

    $cache = new Cache(new RedisCacheAdapter($cacheRedis));
    // @phpstan-ignore class.notFound, argument.type (Redis adapter built in parallel)
    $database = new Database(new RedisDbAdapter($redis), $cache);
    $database
        ->setAuthorization($authorization)
        ->setDatabase('utopiaTests')
        ->setNamespace($namespace);

    $document = $database->getDocument('crossproc', $documentId);
    if ($document->isEmpty()) {
        throw new \RuntimeException("Document '{$documentId}' not visible to child process — cross-process state did not propagate.");
    }

    $value = $document->getAttribute('value');
    if ($value !== 'hello') {
        throw new \RuntimeException("Expected child to read value='hello', got " . \var_export($value, true));
    }

    $database->updateDocument('crossproc', $documentId, $document->setAttribute('value', 'world'));

    \fwrite(\STDOUT, "OK\n");
    exit(0);
} catch (\Throwable $error) {
    \fwrite(\STDERR, $error::class . ': ' . $error->getMessage() . "\n");
    \fwrite(\STDERR, $error->getTraceAsString() . "\n");
    exit(1);
}
