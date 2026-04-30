<?php

/**
 * Cross-process worker for the relationship round-trip test. Re-attaches
 * to an existing Redis-backed namespace, fetches two parent documents:
 * the first is expected to have its relationship key resolved to a
 * specific child id; the second is expected to have a null relationship
 * key (proves null-surfacing across processes).
 *
 * argv: [$script, $namespace, $parentSetId, $parentNullId, $expectedChildId]
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

    if ($argc < 5) {
        throw new \RuntimeException('Usage: redis_relationship_worker.php <namespace> <parentSetId> <parentNullId> <expectedChildId>');
    }

    [$_script, $namespace, $parentSetId, $parentNullId, $expectedChildId] = $argv;

    $host = \getenv('REDIS_HOST') ?: 'redis-mirror';
    $port = (int) (\getenv('REDIS_PORT') ?: 6379);

    $redis = new \Redis();
    $redis->connect($host, $port);

    $cacheRedis = new \Redis();
    $cacheRedis->connect(\getenv('CACHE_REDIS_HOST') ?: 'redis', (int) (\getenv('CACHE_REDIS_PORT') ?: 6379));

    $authorization = new Authorization();
    $authorization->addRole('any');

    $cache = new Cache(new RedisCacheAdapter($cacheRedis));
    $database = new Database(new RedisDbAdapter($redis), $cache);
    $database
        ->setAuthorization($authorization)
        ->setDatabase('utopiaTests')
        ->setNamespace($namespace);

    $parentSet = $database->getDocument('parents', $parentSetId);
    if ($parentSet->isEmpty()) {
        throw new \RuntimeException("Parent '{$parentSetId}' not visible to child process — cross-process state did not propagate.");
    }

    $childRef = $parentSet->getAttribute('child');
    // The orchestrator may either inline the related document (Document)
    // or surface a bare id (string), depending on populate state. Accept
    // both — what we care about is the round-trip identity.
    $observedChildId = match (true) {
        \is_string($childRef) => $childRef,
        \is_array($childRef) => $childRef['$id'] ?? null,
        \is_object($childRef) && \method_exists($childRef, 'getId') => $childRef->getId(),
        default => null,
    };

    if ($observedChildId !== $expectedChildId) {
        throw new \RuntimeException(
            "Expected child id '{$expectedChildId}' on parent '{$parentSetId}', got " . \var_export($observedChildId, true)
        );
    }

    $parentNull = $database->getDocument('parents', $parentNullId);
    if ($parentNull->isEmpty()) {
        throw new \RuntimeException("Parent '{$parentNullId}' not visible to child process.");
    }

    if (! \array_key_exists('child', $parentNull->getArrayCopy())) {
        throw new \RuntimeException(
            "Relationship key 'child' missing on parent '{$parentNullId}' — null-surfacing failed across processes."
        );
    }

    $nullRef = $parentNull->getAttribute('child');
    if ($nullRef !== null) {
        throw new \RuntimeException(
            "Expected null relationship on parent '{$parentNullId}', got " . \var_export($nullRef, true)
        );
    }

    \fwrite(\STDOUT, "OK\n");
    exit(0);
} catch (\Throwable $error) {
    \fwrite(\STDERR, $error::class . ': ' . $error->getMessage() . "\n");
    \fwrite(\STDERR, $error->getTraceAsString() . "\n");
    exit(1);
}
