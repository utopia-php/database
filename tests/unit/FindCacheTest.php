<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

class FindCacheTest extends TestCase
{
    private Database $database;

    protected function setUp(): void
    {
        $this->database = $this->createDatabase(new HashMemoryCache());
    }

    private function createDatabase(Adapter $cache, ?DatabaseMemory $adapter = null): Database
    {
        $database = new Database($adapter ?? new DatabaseMemory(), new Cache($cache));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('find_cache_' . \uniqid());

        $database->create();
        $database->createCollection('projects');
        $database->createAttribute('projects', 'name', Database::VAR_STRING, 255, true);

        return $database;
    }

    private function seedProject(Database $database, string $id, string $name): void
    {
        $database->createDocument('projects', new Document([
            '$id' => $id,
            '$permissions' => [Permission::read(Role::any())],
            'name' => $name,
        ]));
    }

    public function testFindCachedReturnsStaleResultUntilPurged(): void
    {
        $this->seedProject($this->database, 'first', 'First');

        $documents = $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600));
        $this->assertCount(1, $documents);
        $this->assertSame('first', $documents[0]->getId());

        $this->seedProject($this->database, 'second', 'Second');

        $documents = $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600));
        $this->assertCount(1, $documents);
        $this->assertSame('first', $documents[0]->getId());

        $this->database->purgeCachedFinds('projects');

        $documents = $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600));
        $this->assertCount(2, $documents);
        $this->assertSame('first', $documents[0]->getId());
        $this->assertSame('second', $documents[1]->getId());
    }

    public function testFindCachedBypassesCacheWhenTtlIsZero(): void
    {
        $this->seedProject($this->database, 'first', 'First');

        $documents = $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600));
        $this->assertCount(1, $documents);

        $this->seedProject($this->database, 'second', 'Second');

        $documents = $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 0));
        $this->assertCount(2, $documents);
    }

    public function testFindCachedUsesDefaultTtl(): void
    {
        $this->seedProject($this->database, 'first', 'First');

        $documents = $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderAsc('name')]));
        $this->assertCount(1, $documents);

        $this->seedProject($this->database, 'second', 'Second');

        $documents = $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderAsc('name')]));
        $this->assertCount(1, $documents);
    }

    public function testFindCachedStoresEmptyResults(): void
    {
        $documents = $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600));
        $this->assertCount(0, $documents);

        $this->seedProject($this->database, 'first', 'First');

        $documents = $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600));
        $this->assertCount(0, $documents);

        $this->database->purgeCachedFinds('projects');

        $documents = $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600));
        $this->assertCount(1, $documents);
    }

    public function testFindCachedDelegatesToFindWhenAuthorizationIsEnabled(): void
    {
        $this->seedProject($this->database, 'first', 'First');

        $documents = $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600);
        $this->assertCount(1, $documents);

        $this->seedProject($this->database, 'second', 'Second');

        $documents = $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600);
        $this->assertCount(2, $documents);
    }

    public function testFindCachedBypassesCacheForRandomOrder(): void
    {
        $this->seedProject($this->database, 'first', 'First');

        $documents = $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderRandom()], ttl: 3600));
        $this->assertCount(1, $documents);

        $this->seedProject($this->database, 'second', 'Second');

        $documents = $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderRandom()], ttl: 3600));
        $this->assertCount(2, $documents);
    }

    public function testFindCachedDelegatesMalformedQueriesToFind(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Invalid query type');

        /** @var array<Query> $queries Intentionally malformed to exercise runtime validation. */
        $queries = ['not-a-query'];

        $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', $queries, ttl: 3600));
    }

    public function testFindCachedTriggersFindEventOnCacheHit(): void
    {
        $events = [];
        $this->database->on(Database::EVENT_DOCUMENT_FIND, 'test', function (string $event) use (&$events): void {
            $events[] = $event;
        });

        $this->seedProject($this->database, 'first', 'First');

        $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600));
        $this->seedProject($this->database, 'second', 'Second');

        $documents = $this->database->getAuthorization()->skip(fn () => $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600));
        $this->assertCount(1, $documents);
        $this->assertSame('first', $documents[0]->getId());

        $this->assertSame([
            Database::EVENT_DOCUMENT_FIND,
            Database::EVENT_DOCUMENT_FIND,
        ], $events);
    }

    public function testPurgeCachedFindRemovesOnlyOneCallerKey(): void
    {
        $database = $this->createDatabase(new HashMemoryCache());

        $this->seedProject($database, 'first', 'First');

        $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, key: 'a'));
        $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, key: 'b'));

        $this->seedProject($database, 'second', 'Second');

        $database->purgeCachedFind('projects', key: 'a');

        $documents = $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, key: 'a'));
        $this->assertCount(2, $documents);

        $documents = $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, key: 'b'));
        $this->assertCount(1, $documents);
    }

    public function testPurgeCachedFindsRemovesAllCallerKeys(): void
    {
        $database = $this->createDatabase(new HashMemoryCache());

        $this->seedProject($database, 'first', 'First');

        $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, key: 'a'));
        $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, key: 'b'));

        $this->seedProject($database, 'second', 'Second');

        $database->purgeCachedFinds('projects');

        $documents = $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, key: 'a'));
        $this->assertCount(2, $documents);

        $documents = $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, key: 'b'));
        $this->assertCount(2, $documents);
    }

    public function testFindCachedTouchesCacheEntryOnHitWhenEnabled(): void
    {
        $cache = new TouchSpyCache();
        $database = $this->createDatabase($cache);

        $this->seedProject($database, 'first', 'First');

        $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, touchOnHit: true));
        $this->assertSame(0, $cache->touches);

        $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, touchOnHit: true));
        $this->assertSame(1, $cache->touches);
    }

    public function testFindCachedDoesNotTouchCacheEntryByDefault(): void
    {
        $cache = new TouchSpyCache();
        $database = $this->createDatabase($cache);

        $this->seedProject($database, 'first', 'First');

        $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600));
        $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600));

        $this->assertSame(0, $cache->touches);
    }

    public function testFindCachedRefetchesExpiredCachedDocuments(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache, new TtlMemoryAdapter());
        $database->createAttribute('projects', 'expiresAt', Database::VAR_DATETIME, 0, false);
        $database->createIndex('projects', 'expiresAtTtl', Database::INDEX_TTL, ['expiresAt'], ttl: 1);

        $database->createDocument('projects', new Document([
            '$id' => 'first',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'First',
            'expiresAt' => '2999-01-01T00:00:00.000+00:00',
        ]));
        $this->seedProject($database, 'second', 'Second');

        $documents = $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name'), Query::limit(1)], ttl: 3600));
        $this->assertCount(1, $documents);
        $this->assertSame('first', $documents[0]->getId());

        [$findKey, $findField] = $database->getAuthorization()->skip(fn () => $database->getCachedFindKeys(
            'projects',
            [Query::orderAsc('name'), Query::limit(1)],
            collection: $database->getCollection('projects')
        ));
        $cache->setCachedDocumentAttribute($findKey, $findField, 'first', 'expiresAt', '2000-01-01T00:00:00.000+00:00');
        $database->getAuthorization()->skip(fn () => $database->updateDocument('projects', 'first', new Document(['name' => 'Zulu'])));

        $documents = $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name'), Query::limit(1)], ttl: 3600));
        $this->assertCount(1, $documents);
        $this->assertSame('second', $documents[0]->getId());
    }

    public function testFindCachedDoesNotTouchCacheEntryWithExpiredDocuments(): void
    {
        $cache = new TouchSpyCache();
        $database = $this->createDatabase($cache, new TtlMemoryAdapter());
        $database->createAttribute('projects', 'expiresAt', Database::VAR_DATETIME, 0, false);
        $database->createIndex('projects', 'expiresAtTtl', Database::INDEX_TTL, ['expiresAt'], ttl: 1);

        $database->createDocument('projects', new Document([
            '$id' => 'first',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'First',
            'expiresAt' => '2999-01-01T00:00:00.000+00:00',
        ]));

        $documents = $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, touchOnHit: true));
        $this->assertCount(1, $documents);
        $this->assertSame(0, $cache->touches);

        [$findKey, $findField] = $database->getAuthorization()->skip(fn () => $database->getCachedFindKeys(
            'projects',
            [Query::orderAsc('name')],
            collection: $database->getCollection('projects')
        ));
        $cache->setCachedDocumentAttribute($findKey, $findField, 'first', 'expiresAt', '2000-01-01T00:00:00.000+00:00');

        $documents = $database->getAuthorization()->skip(fn () => $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, touchOnHit: true));
        $this->assertCount(1, $documents);
        $this->assertSame(0, $cache->touches);
    }
}

class HashMemoryCache implements Adapter
{
    /**
     * @var array<string, array<string, array{time: int, data: array<int|string, mixed>|string}>>
     */
    private array $store = [];

    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        $hash = $hash === '' ? $key : $hash;
        $saved = $this->store[$key][$hash] ?? null;
        if ($saved === null) {
            return false;
        }

        return ($saved['time'] + $ttl > \time()) ? $saved['data'] : false;
    }

    public function save(string $key, array|string $data, string $hash = ''): bool|string|array
    {
        if ($key === '' || empty($data)) {
            return false;
        }

        $hash = $hash === '' ? $key : $hash;
        $this->store[$key][$hash] = [
            'time' => \time(),
            'data' => $data,
        ];

        return $data;
    }

    public function touch(string $key, string $hash = ''): bool
    {
        $hash = $hash === '' ? $key : $hash;
        if (!isset($this->store[$key][$hash])) {
            return false;
        }

        $this->store[$key][$hash]['time'] = \time();

        return true;
    }

    public function setCachedDocumentAttribute(string $key, string $hash, string $documentId, string $attribute, mixed $value): void
    {
        $usesEntryKey = isset($this->store[$hash][$hash]);
        $cacheKey = $usesEntryKey ? $hash : $key;
        $cacheHash = $hash;
        $payload = $this->store[$cacheKey][$cacheHash]['data'] ?? [];
        $documents = \is_array($payload) && \is_array($payload['documents'] ?? null) ? $payload['documents'] : $payload;
        if (!\is_array($documents)) {
            return;
        }

        foreach ($documents as $index => $document) {
            if (!\is_array($document) || ($document['$id'] ?? '') !== $documentId) {
                continue;
            }

            $documents[$index][$attribute] = $value;
            if (\is_array($payload) && \array_key_exists('documents', $payload)) {
                $payload['documents'] = $documents;
                $this->store[$cacheKey][$cacheHash]['data'] = $payload;
            } else {
                $this->store[$cacheKey][$cacheHash]['data'] = $documents;
            }
            return;
        }
    }

    /**
     * @return array<string>
     */
    public function list(string $key): array
    {
        return \array_keys($this->store[$key] ?? []);
    }

    public function purge(string $key, string $hash = ''): bool
    {
        if ($hash !== '') {
            unset($this->store[$key][$hash]);
            return true;
        }

        unset($this->store[$key]);

        return true;
    }

    public function flush(): bool
    {
        $this->store = [];

        return true;
    }

    public function ping(): bool
    {
        return true;
    }

    public function getSize(): int
    {
        return \count($this->store);
    }

    public function getName(?string $key = null): string
    {
        return 'hash-memory';
    }
}

class TouchSpyCache extends HashMemoryCache
{
    public int $touches = 0;

    public function touch(string $key, string $hash = ''): bool
    {
        $touched = parent::touch($key, $hash);

        if ($touched) {
            $this->touches++;
        }

        return $touched;
    }
}

class TtlMemoryAdapter extends DatabaseMemory
{
    public function getSupportForTTLIndexes(): bool
    {
        return true;
    }
}
