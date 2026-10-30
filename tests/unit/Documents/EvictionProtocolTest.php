<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Redis;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter\Redis as RedisCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\Mutation;
use Utopia\Database\Cache\Query as QueryCache;
use Utopia\Database\Cache\Scope;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Permission;
use Utopia\Database\Role;

/** Real Redis plus real SQLite/Memory reads; DEL models loss of an entire cache key. */
final class EvictionProtocolTest extends TestCase
{
    private Redis $redis;
    private Cache $cache;
    private string $namespace;

    #[\Override]
    protected function setUp(): void
    {
        if (! \interface_exists(\Utopia\Cache\Feature\Versioned::class)) {
            $this->markTestSkipped('The older-cache compatibility tier exercises the legacy protocol');
        }
        $this->redis = new Redis();
        $this->redis->connect(\getenv('DATABASE_TEST_CACHE_HOST') ?: 'redis', (int) (\getenv('DATABASE_TEST_CACHE_PORT') ?: 6379));
        $this->cache = new Cache(new RedisCache($this->redis));
        $this->namespace = 'eviction_'.\bin2hex(\random_bytes(8));
    }

    #[\Override]
    protected function tearDown(): void
    {
        if (! isset($this->redis)) {
            return;
        }
        $cursor = null;
        do {
            $keys = $this->redis->scan($cursor, '*'.$this->namespace.'*', 1000);
            if ($keys !== false && $keys !== []) {
                $this->redis->del($keys);
            }
        } while ($cursor !== 0);
        $this->redis->close();
    }

    public function testLostLegacyCountersCannotStrandDocumentCaching(): void
    {
        $adapter = new CountingMemory();
        $database = $this->database($adapter);
        $key = \strtolower($database->getCacheBaseKeys('users')[0]);
        $this->cache->save($key.'#epoch', 'blocked:old@'.\time());
        for ($i = 0; $i < 6; $i++) {
            $this->cache->purge($key.'#started');
        }
        $this->redis->del($key.'#finished');
        $this->cache->purge($database->getCacheBaseKeys(Database::METADATA, 'users')[1]);

        $this->assertSame('original', $database->getDocument('users', 'user')->getAttribute('name'));
        $adapter->reset();
        $this->assertSame('original', $database->getDocument('users', 'user')->getAttribute('name'));
        $this->assertSame(0, $adapter->documentReads);

        $this->redis->del($key.'#state');
        $database->updateDocuments('users', new Document(['name' => 'updated']));
        $this->assertSame('updated', $database->getDocument('users', 'user')->getAttribute('name'));
        $adapter->reset();
        $this->assertSame('updated', $database->getDocument('users', 'user')->getAttribute('name'));
        $this->assertSame(0, $adapter->documentReads, 'Caching resumes after a write whose previous state was evicted');
    }

    public function testFinishingAnEvictedWriterCannotUnblockANewerWriter(): void
    {
        $key = $this->namespace;
        $mutation = new Mutation($this->cache);
        $mutation->begin($key, 'first', 3600);
        $this->redis->del($key.'#state');
        $cold = $mutation->read($key, 3600)->value;
        $this->assertNotNull($cold);
        $mutation->begin($key, 'second', 3600);
        $mutation->finish($key, 'first', 3600);
        $this->assertNull($mutation->read($key, 3600)->value, 'A newer live writer still owns its barrier');
        $mutation->finish($key, 'second', 3600);
        $active = $mutation->read($key, 3600)->value;
        $this->assertNotNull($active);
        $this->assertNotSame($cold, $active, 'Finishing lost writers retires cold fills');
    }

    public function testQueryResultsRefilledAfterEvictionAreRetiredWhenTheWriterFinishes(): void
    {
        $query = new QueryCache($this->cache);
        $scope = new Scope(namespace: $this->namespace);
        $key = $query->getCollectionKey($scope, 'users');
        $token = $query->createToken();
        $query->blockCollection($key, $token);
        $this->redis->del(\strtolower($key.'#state'));
        $entry = $query->getEntry($scope, 'users', []);
        $this->assertNotNull($entry);
        $this->assertTrue($query->set($entry, [new Document(['$id' => 'pre-commit'])], $query->getGeneration($entry)));
        $query->activateCollection($key, $token, 3600);
        $fresh = $query->getEntry($scope, 'users', []);
        $this->assertNotNull($fresh);
        $this->assertNull($query->get($fresh), 'A completed writer retires results filled after its registration disappeared');
    }

    public function testPreCommitDocumentFillAfterEvictionCannotSurviveCommit(): void
    {
        $path = \tempnam(\sys_get_temp_dir(), 'eviction-sqlite-');
        if ($path === false) {
            $this->fail('Unable to create SQLite fixture');
        }
        try {
            $writerConnection = new \PDO('sqlite:'.$path);
            $readerConnection = new \PDO('sqlite:'.$path);
            $writerConnection->exec('PRAGMA journal_mode=WAL');
            $writerAdapter = new EvictionSQLite($writerConnection);
            $writer = $this->database($writerAdapter);
            $reader = new Database(new SQLite($readerConnection), $this->cache);
            $reader->setDatabase('eviction-tests')->setNamespace($this->namespace);
            $key = \strtolower($writer->getCacheBaseKeys('users')[0]);
            $writerAdapter->beforeCommit = function () use ($key, $reader): void {
                $this->redis->del($key.'#state');
                $this->assertSame('original', $reader->getDocument('users', 'user')->getAttribute('name'));
            };
            $writer->updateDocuments('users', new Document(['name' => 'updated']));
            $this->assertSame('updated', $reader->getDocument('users', 'user')->getAttribute('name'));
            $this->assertSame('updated', $reader->getDocument('users', 'user')->getAttribute('name'));
        } finally {
            foreach ([$path, $path.'-wal', $path.'-shm'] as $file) {
                if (\is_file($file)) {
                    \unlink($file);
                }
            }
        }
    }

    private function database(CountingMemory|SQLite $adapter): Database
    {
        $database = new Database($adapter, $this->cache);
        $database->setDatabase('eviction-tests')->setNamespace($this->namespace);
        $database->create();
        $database->createCollection(Collection::create(id: 'users', attributes: [Attribute::string(key: 'name')], permissions: [Permission::read(Role::any()), Permission::create(Role::any()), Permission::update(Role::any())]));
        $database->createDocument('users', new Document(['$id' => 'user', 'name' => 'original']));

        return $database;
    }
}
