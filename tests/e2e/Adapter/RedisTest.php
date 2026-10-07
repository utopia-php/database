<?php

namespace Tests\E2E\Adapter;

use Redis;
use Utopia\Cache\Adapter\None as NoneCacheAdapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Id;
use Utopia\Database\Permission;
use Utopia\Database\Role;

/**
 * Paratest's `--functional` mode invokes `setUpBeforeClass`/`tearDownAfterClass`
 * between every test method, not just at suite boundaries, while inherited
 * fixture statics (`$moviesFixtureInit`, `$documentsFixtureInit`, etc.) stay
 * set across methods within the same worker process. Scrubbing the namespace
 * or recreating the `Database` between tests would leave the cached fixture
 * metadata pointing at collections that no longer exist. The CI Redis
 * container is ephemeral, so leaking keys to process exit is safe.
 */
class RedisTest extends Base
{
    public static ?Database $database = null;
    public static ?Redis $redisClient = null;
    public static string $redisNamespace = '';

    public static function getAdapterName(): string
    {
        return 'redis';
    }

    /**
     * Subclasses may override to flip shared-tables/tenant on. Called once
     * before `create()` so the configured namespace and tenancy mode reach
     * the underlying adapter from the start — patching them after-the-fact
     * leaks keys under the original namespace.
     */
    protected function configureDatabase(Database $database): void
    {
        // Default: per-run unique namespace, no shared tables.
    }

    public function getDatabase(): Database
    {
        if (self::$database !== null) {
            return self::$database;
        }

        if (self::$authorization === null) {
            self::$authorization = new \Utopia\Database\Validator\Authorization();
        }

        $host = \getenv('REDIS_HOST') ?: 'redis';
        $port = (int) (\getenv('REDIS_PORT') ?: 6379);

        $client = new Redis();
        $client->connect($host, $port);
        self::$redisClient = $client;

        // Redis-as-adapter makes the Cache layer redundant — adapter reads
        // and cache reads cost the same Redis round trip, and any
        // invalidation gap between them just becomes a stale-read window.
        // None() short-circuits the cache so reads always hit Redis.
        $cache = new Cache(new NoneCacheAdapter());

        $adapter = new RedisAdapter($client);

        self::$redisNamespace = 'utopia_test_' . \uniqid();
        $database = new Database($adapter, $cache);
        $database
            ->setAuthorization(self::$authorization)
            ->setDatabase($this->testDatabase)
            ->setNamespace(self::$redisNamespace);

        $this->configureDatabase($database);

        if ($database->exists()) {
            $database->delete();
        }

        $database->create();

        return self::$database = $database;
    }

    protected function deleteColumn(string $collection, string $column): bool
    {
        // Redis keeps no out-of-band schema; raw column drops do not apply.
        return true;
    }

    protected function deleteIndex(string $collection, string $index): bool
    {
        return true;
    }

    /**
     * Inherited test exercises the case where an INTEGER column is altered
     * to VARCHAR. Redis stores documents as JSON; type changes do not
     * retroactively recast existing values the way PDO string returns do.
     */
    public function testUpdateAttributeStructure(): void
    {
        $this->markTestSkipped(
            'Redis stores documents as JSON; type changes do not retroactively coerce existing column values the way PDO string returns do.'
        );
    }

    /**
     * Inherited test exercises VARCHAR truncation when shrinking a column
     * that holds oversize data. Redis does not enforce string sizes on disk.
     */
    public function testUpdateAttributeSize(): void
    {
        $this->markTestSkipped(
            'Redis does not enforce string size truncation when an attribute is resized smaller than existing data.'
        );
    }

    public function testReadsDropAStoredNonStringPermission(): void
    {
        $database = $this->getDatabase();
        $collection = 'lenientReads';
        $id = Id::unique();
        $permissions = [Permission::read(Role::any())];

        $database->createCollection(Collection::create(
            id: $collection,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: true,
        ));
        $database->createDocument($collection, new Document([
            '$id' => $id,
            '$permissions' => $permissions,
            'title' => 'stored',
        ]));

        $client = self::$redisClient;
        $this->assertNotNull($client);
        $payloads = $this->findStoredPayloads($client, $id);
        $this->assertNotEmpty($payloads, 'The created document must be stored as a JSON string value.');
        foreach ($payloads as $key => $stored) {
            $stored[Document::PERMISSIONS] = [Permission::read(Role::any()), 42, null];
            $client->set($key, \json_encode($stored, JSON_THROW_ON_ERROR));
        }

        $this->assertSame($permissions, $database->getDocument($collection, $id)->getPermissions());
        $this->assertSame(
            [$permissions],
            \array_map(fn (Document $document): array => $document->getPermissions(), $database->find($collection)),
        );

        $this->assertSame(1, $database->updateDocuments($collection, new Document(['title' => 'bulk'])));
        $this->assertSame('bulk', $database->getDocument($collection, $id)->getAttribute('title'));

        $updated = $database->updateDocument($collection, $id, new Document(['title' => 'single']));
        $this->assertSame('single', $updated->getAttribute('title'));
        $this->assertSame($permissions, $updated->getPermissions());
    }

    /**
     * @return array<string, array<mixed>>
     */
    private function findStoredPayloads(Redis $client, string $id): array
    {
        $payloads = [];
        $iterator = null;
        do {
            $keys = $client->scan($iterator, null, 1000, 'string');
            if (! \is_array($keys) || $keys === []) {
                continue;
            }
            $keys = \array_values(\array_filter($keys, \is_string(...)));
            $values = $client->mGet($keys);
            if (! \is_array($values)) {
                continue;
            }
            foreach (\array_combine($keys, \array_values($values)) as $key => $value) {
                if (! \is_string($value) || ! \str_contains($value, $id)) {
                    continue;
                }
                $decoded = \json_decode($value, true);
                if (\is_array($decoded) && ($decoded[Document::ID] ?? null) === $id) {
                    $payloads[$key] = $decoded;
                }
            }
        } while ($iterator > 0);

        return $payloads;
    }
}
