<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter;
use Utopia\Cache\Adapter\Memory as CacheMemory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

class ListCacheTest extends TestCase
{
    private Database $database;

    protected function setUp(): void
    {
        $this->database = $this->createDatabase(new CacheMemory());
    }

    private function createDatabase(Adapter $cache, ?DatabaseMemory $adapter = null): Database
    {
        $database = new Database($adapter ?? new DatabaseMemory(), new Cache($cache));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('list_cache_' . \uniqid());

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

    public function testFindCachedReturnsStaleResultUntilListKeyIsPurged(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache);
        $this->seedProject($database, 'first', 'First');

        $documents = $database->getAuthorization()->skip(fn () => $database->findCached(
            'projects',
            [Query::orderAsc('name')],
            ttl: 3600,
            cacheCollection: 'wafrules',
            namespace: '_39',
            roles: ['waf'],
            payloadKey: 'rules',
        ));
        $this->assertCount(1, $documents);
        $this->assertSame('first', $documents[0]->getId());

        $this->seedProject($database, 'second', 'Second');

        $documents = $database->getAuthorization()->skip(fn () => $database->findCached(
            'projects',
            [Query::orderAsc('name')],
            ttl: 3600,
            cacheCollection: 'wafrules',
            namespace: '_39',
            roles: ['waf'],
            payloadKey: 'rules',
        ));
        $this->assertCount(1, $documents);
        $this->assertSame('first', $documents[0]->getId());

        $cache->purge($database->getFindCacheKey('wafrules', '_39'));

        $documents = $database->getAuthorization()->skip(fn () => $database->findCached(
            'projects',
            [Query::orderAsc('name')],
            ttl: 3600,
            cacheCollection: 'wafrules',
            namespace: '_39',
            roles: ['waf'],
            payloadKey: 'rules',
        ));
        $this->assertCount(2, $documents);
    }

    public function testFindCachedUsesListCacheKeyAndField(): void
    {
        $cache = new HashMemoryCache();
        $database = $this->createDatabase($cache);
        $this->seedProject($database, 'first', 'First');

        $database->getAuthorization()->skip(fn () => $database->findCached(
            'projects',
            [Query::orderAsc('name')],
            ttl: 3600,
            cacheCollection: 'wafrules',
            namespace: '_39',
            roles: ['waf'],
            payloadKey: 'rules',
        ));

        $fields = $cache->list($database->getFindCacheKey('wafrules', '_39'));

        $this->assertCount(1, $fields);
        $this->assertStringEndsWith(':documents:rules', $fields[0]);
        $this->assertSame(4, \substr_count($fields[0], ':'));
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

        $queries = [Query::orderAsc('name'), Query::limit(1)];
        $database->getAuthorization()->skip(fn () => $database->findCached(
            'projects',
            $queries,
            ttl: 3600,
            cacheCollection: 'wafrules',
            namespace: '_39',
            roles: ['waf'],
            payloadKey: 'rules',
        ));

        $collection = $database->getCollection('projects');
        $cache->setCachedPayloadDocumentAttribute(
            $database->getFindCacheKey('wafrules', '_39'),
            $database->getFindCacheField($collection, $queries, ['waf'], 'documents', 'rules'),
            'rules',
            'first',
            'expiresAt',
            '2000-01-01T00:00:00.000+00:00',
        );
        $database->getAuthorization()->skip(fn () => $database->updateDocument('projects', 'first', new Document(['name' => 'Zulu'])));

        $documents = $database->getAuthorization()->skip(fn () => $database->findCached(
            'projects',
            $queries,
            ttl: 3600,
            cacheCollection: 'wafrules',
            namespace: '_39',
            roles: ['waf'],
            payloadKey: 'rules',
        ));
        $this->assertCount(1, $documents);
        $this->assertSame('second', $documents[0]->getId());
    }

    public function testFindCachedValidatesQueryTypesBeforeCaching(): void
    {
        $this->expectException(QueryException::class);

        /** @var array<Query> $queries */
        $queries = ['invalid'];

        $this->database->getAuthorization()->skip(fn () => $this->database->findCached(
            'projects',
            $queries,
            ttl: 3600,
            cacheCollection: 'wafrules',
            namespace: '_39',
        ));
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

    public function setCachedPayloadDocumentAttribute(string $key, string $hash, string $payload, string $documentId, string $attribute, mixed $value): void
    {
        $data = $this->store[$key][$hash]['data'] ?? [];
        if (!\is_array($data)) {
            return;
        }

        $documents = $data[$payload] ?? [];
        if (!\is_array($documents)) {
            return;
        }

        foreach ($documents as $index => $document) {
            if (!\is_array($document) || ($document['$id'] ?? '') !== $documentId) {
                continue;
            }

            $documents[$index][$attribute] = $value;
            $data[$payload] = $documents;
            $this->store[$key][$hash]['data'] = $data;
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

class TtlMemoryAdapter extends DatabaseMemory
{
    public function getSupportForTTLIndexes(): bool
    {
        return true;
    }
}
