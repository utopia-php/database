<?php

namespace Tests\Unit\Cache;

use PDO;
use PHPUnit\Framework\TestCase;
use Redis;
use Utopia\Cache\Adapter\CircuitBreaker;
use Utopia\Cache\Adapter\Redis as RedisCache;
use Utopia\Cache\Cache;
use Utopia\Cache\Exception\Unavailable;
use Utopia\CircuitBreaker\CircuitBreaker as Breaker;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\Bridge;
use Utopia\Database\Cache\Mutation;
use Utopia\Database\Cache\Query as QueryCache;
use Utopia\Database\Cache\Scope;
use Utopia\Database\Database;
use Utopia\Database\Document;

/** Actual released v1 process and bridge readers/writers share Redis and SQL. */
final class BridgeTest extends TestCase
{
    private Redis $redis;
    private Cache $cache;
    private string $namespace;
    private string $path;

    #[\Override]
    protected function setUp(): void
    {
        $this->redis = new Redis();
        $this->redis->connect(\getenv('DATABASE_TEST_CACHE_HOST') ?: 'redis', (int) (\getenv('DATABASE_TEST_CACHE_PORT') ?: 6379));
        $this->cache = new Cache(new RedisCache($this->redis));
        $this->namespace = 'bridge_'.\bin2hex(\random_bytes(8));
        $path = \tempnam(\sys_get_temp_dir(), 'cache-bridge-');
        $this->assertNotFalse($path);
        $this->path = $path;
        $this->assertTrue($this->old('init'));
    }

    #[\Override]
    protected function tearDown(): void
    {
        if (isset($this->redis, $this->namespace)) {
            $cursor = null;
            do {
                $keys = $this->redis->scan($cursor, '*'.$this->namespace.'*', 1000);
                if ($keys !== false && $keys !== []) {
                    $this->redis->del($keys);
                }
            } while ($cursor !== 0);
            $this->redis->close();
        }
        if (isset($this->path)) {
            \unlink($this->path);
        }
    }

    public function testFirstPassKeepsOldAndBridgeReadersWarmAndInvalidatesBothDirections(): void
    {
        $database = $this->database('legacy');
        $this->assertSame('original', $this->old('get'));
        $this->assertSame(['original'], $this->old('find'));
        $this->assertSame('original', $database->getDocument('users', 'user')->getAttribute('name'));
        $database->setProfiling(true);
        $database->getDocument('users', 'user');
        $profiler = $database->getProfiler();
        $this->assertNotNull($profiler);
        $profiler->reset();
        $database->getDocument('users', 'user');
        $this->assertSame(0, $profiler->getQueryCount(), 'The legacy pass keeps the hot path cached');
        $database->updateDocument('users', 'user', new Document(['name' => 'bridge']));
        $this->assertSame('bridge', $this->old('get'));
        $this->assertSame(['bridge'], $this->old('find'));
        $this->old('update', ['value' => 'old']);
        $this->assertSame('old', $database->getDocument('users', 'user')->getAttribute('name'));
        $this->assertSame('old', $database->find('users')[0]->getAttribute('name'));
        $key = $database->getCacheBaseKeys('users')[0];
        $this->assertFalse($this->redis->hExists(\strtolower($key.'#owners'), '__utopia_version__'), 'The bridge must not pollute v1 owner hashes with v2 internal fields');
        $this->assertSame([], $this->cache->legacy()->list($key.'#owners'));
    }

    public function testSecondAndThirdPassesInvalidateDocumentsQueriesAndNegativeEntries(): void
    {
        $legacy = $this->database('legacy');
        $dual = $this->database('dual');
        $this->assertSame('original', $legacy->getDocument('users', 'user')->getAttribute('name'));
        $this->assertSame('original', $dual->getDocument('users', 'user')->getAttribute('name'));
        $this->assertSame('original', $dual->find('users')[0]->getAttribute('name'));
        $this->assertTrue($legacy->getDocument('users', 'missing')->isEmpty());
        $this->assertTrue($dual->getDocument('users', 'missing')->isEmpty());
        $legacy->createDocument('users', new Document(['$id' => 'missing', 'name' => 'created']));
        $this->assertSame('created', $dual->getDocument('users', 'missing')->getAttribute('name'));
        $dual->updateDocument('users', 'user', new Document(['name' => 'dual']));
        $this->assertSame('dual', $legacy->getDocument('users', 'user')->getAttribute('name'));
        $this->assertSame('dual', $this->old('get'), 'Rollback readers remain valid during the dual-write window');
        $this->assertContains('dual', \array_map(fn (Document $document) => $document->getAttribute('name'), $legacy->find('users')));
        $v2 = $this->database('v2');
        $v2->updateDocument('users', 'user', new Document(['name' => 'final']));
        $this->assertSame('final', $dual->getDocument('users', 'user')->getAttribute('name'));
        $this->assertSame('final', $v2->getDocument('users', 'user')->getAttribute('name'));
        $this->assertContains('final', \array_map(fn (Document $document) => $document->getAttribute('name'), $dual->find('users')));
    }

    public function testSlotZeroRemainsCacheableAcrossEveryPhase(): void
    {
        foreach (['', 'legacy', 'dual', 'v2'] as $mode) {
            $cache = Bridge::wrap($this->cache, $mode);
            $query = new QueryCache($cache, slots: 1);
            $scope = new Scope(namespace: $this->namespace.$mode);
            $entry = $query->getEntry($scope, 'users', []);
            $this->assertNotNull($entry);
            $this->assertTrue($query->set($entry, [new Document(['$id' => 'warm'])], $query->getLease($entry)));
            $next = $query->getEntry($scope, 'users', []);
            $this->assertNotNull($next);
            $documents = $query->get($next);
            $this->assertNotNull($documents, 'The zero slot must load the same field that the lease filled');
            $this->assertSame('warm', $documents[0]->getId());
        }
    }

    public function testSchemaInvalidationRetiresDefinitionsInBothKeyspaces(): void
    {
        $legacy = $this->database('legacy');
        $dual = $this->database('dual');
        $legacy->getCollection('users');
        $dual->getCollection('users');
        $dual->createAttribute('users', Attribute::string(key: 'extra'));
        $attributes = function (Database $database): array {
            $items = $database->getCollection('users')->getAttribute('attributes');
            $this->assertIsArray($items);

            return \array_map(function (mixed $attribute): string {
                $this->assertInstanceOf(Document::class, $attribute);

                return $attribute->getId();
            }, $items);
        };
        $this->assertContains('extra', $attributes($legacy));
        $this->assertContains('extra', $attributes($dual));
        $oldAttributes = $this->old('attributes');
        $this->assertIsArray($oldAttributes);
        $this->assertContains('extra', $oldAttributes);
    }

    public function testAnOldTransactionFinishesBeforeTheReadSwitch(): void
    {
        [$process, $pipes] = $this->startOld('hold', ['value' => 'committed']);
        try {
            $this->assertSame("ready\n", \fgets($pipes[1]));
            $legacy = $this->database('legacy');
            $this->assertSame('original', $legacy->getDocument('users', 'user')->getAttribute('name'));
            \fwrite($pipes[0], "commit\n");
            \fclose($pipes[0]);
            $this->assertSame('committed', \json_decode((string) \stream_get_contents($pipes[1]), true, flags: JSON_THROW_ON_ERROR));
            $this->assertSame('', \stream_get_contents($pipes[2]));
            $this->assertSame(0, \proc_close($process));
            $process = null;
            $this->assertSame('committed', $legacy->getDocument('users', 'user')->getAttribute('name'));
            $this->assertSame('committed', $this->database('dual')->getDocument('users', 'user')->getAttribute('name'));
        } finally {
            if (\is_resource($process)) {
                \proc_terminate($process);
                \proc_close($process);
            }
        }
    }

    public function testEvictionDuringDualWritesRetiresFillsAndPreservesNewerWriters(): void
    {
        $bridge = new Bridge($this->cache, 'dual');
        $mutation = new Mutation($bridge);
        $key = $this->namespace;
        $first = \time().'.first';
        $second = \time().'.second';
        $mutation->begin($key, $first, 3600);
        $this->redis->del('v2:'.$key.'#state');
        $cold = $mutation->read($key, 3600)->value;
        $this->assertNotNull($cold);
        $mutation->begin($key, $second, 3600);
        $mutation->finish($key, $first, 3600);
        $this->assertNull($mutation->read($key, 3600)->value);
        $mutation->finish($key, $second, 3600);
        $this->assertNotSame($cold, $mutation->read($key, 3600)->value);
    }

    public function testWithCacheInvalidationAndPurgeRecordingsCoverBothReaders(): void
    {
        $legacy = $this->database('legacy');
        $dual = $this->database('dual');
        $key = $legacy->getQueryCacheKey('users');
        $before = \bin2hex(\random_bytes(8));
        $after = \bin2hex(\random_bytes(8));
        $this->assertSame($before, $legacy->withCache($key, fn () => $before));
        $this->assertSame($before, $dual->withCache($key, fn () => $before));
        $this->assertTrue($dual->purgeCachedQueries('users'));
        $this->assertSame($after, $legacy->withCache($key, fn () => $after));
        $this->assertSame($after, $dual->withCache($key, fn () => $after));
        $purges = $dual->getCache()->recordPurges(fn () => $dual->getCache()->purge('MiXeD'));
        $this->assertSame(['mixed' => ['' => true]], $purges, 'Recorded keys must stay logical so replay does not add the namespace twice');
    }

    public function testRollbackRetiresFillsInBothKeyspaces(): void
    {
        $legacy = $this->database('legacy');
        $dual = $this->database('dual');
        $legacy->getDocument('users', 'user');
        $dual->getDocument('users', 'user');
        try {
            $dual->withTransaction(function () use ($dual): void {
                $dual->updateDocument('users', 'user', new Document(['name' => 'rolled-back']));
                throw new \RuntimeException('rollback');
            });
        } catch (\RuntimeException $error) {
            $this->assertSame('rollback', $error->getMessage());
        }
        $this->assertSame('original', $legacy->getDocument('users', 'user')->getAttribute('name'));
        $this->assertSame('original', $dual->getDocument('users', 'user')->getAttribute('name'));
        $this->assertSame('original', $this->old('get'));
    }

    public function testFailedLegacyCompletionStillFinishesV2AndReportsFailure(): void
    {
        $adapter = new FailingLegacyRedis($this->redis);
        $bridge = new Bridge(new Cache($adapter), 'dual');
        $mutation = new Mutation($bridge);
        $key = $this->namespace;
        $token = \time().'.completion';
        $mutation->begin($key, $token, 3600);
        $this->assertNull($mutation->read($key, 3600)->value);
        $adapter->failCompletion = true;
        try {
            $mutation->finish($key, $token, 3600);
            $this->fail('A partial completion must be reported');
        } catch (Unavailable $error) {
            $this->assertSame('Legacy completion unavailable', $error->getMessage());
        }
        $this->assertNotNull($mutation->read($key, 3600)->value, 'V2 completion must run even when legacy completion fails');
        $legacy = $this->cache->load($key.'#epoch', PHP_INT_MAX);
        $this->assertIsString($legacy);
        $this->assertStringStartsWith('blocked:', $legacy);
    }

    public function testOpenBreakerAbortsRequiredCoordinationBeforeSQL(): void
    {
        $breaker = new Breaker(timeout: 3600);
        $cache = new Cache(new CircuitBreaker(new RedisCache($this->redis), $breaker));
        $database = $this->database('dual', $cache);
        $breaker->trip();
        try {
            $database->updateDocument('users', 'user', new Document(['name' => 'must-not-commit']));
            $this->fail('An open breaker must not permit an unfenced SQL write');
        } catch (\Throwable $error) {
            $this->assertInstanceOf(Unavailable::class, $error);
            $this->assertSame('original', $this->old('get'));
        }
    }

    private function database(string $mode, ?Cache $cache = null): Database
    {
        $bridge = new Bridge($cache ?? $this->cache, $mode);
        $database = new Database(new SQLite(new PDO('sqlite:'.$this->path)), $bridge);
        $database->setDatabase('bridge')->setNamespace($this->namespace)->setQueryCache(new QueryCache($bridge));

        return $database;
    }

    /** @param array<string, string> $arguments
     * @return array{resource, array<int, resource>}
     */
    private function startOld(string $action, array $arguments = []): array
    {
        $process = \proc_open([PHP_BINARY, __DIR__.'/../../../compat/runtime.php', \json_encode($arguments + ['action' => $action, 'path' => $this->path, 'namespace' => $this->namespace], JSON_THROW_ON_ERROR)], [['pipe', 'r'], ['pipe', 'w'], ['pipe', 'w']], $pipes);
        $this->assertIsResource($process);
        foreach ($pipes as $pipe) {
            \stream_set_timeout($pipe, 15);
        }

        return [$process, $pipes];
    }

    /** @param array<string, string> $arguments */
    private function old(string $action, array $arguments = []): mixed
    {
        [$process, $pipes] = $this->startOld($action, $arguments);
        \fclose($pipes[0]);
        $output = \stream_get_contents($pipes[1]);
        $error = \stream_get_contents($pipes[2]);
        \fclose($pipes[1]);
        \fclose($pipes[2]);
        $this->assertSame(0, \proc_close($process), (string) $error.' '.(string) $output);

        return \json_decode((string) $output, true, flags: JSON_THROW_ON_ERROR);
    }
}
