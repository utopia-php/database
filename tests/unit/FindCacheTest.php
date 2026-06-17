<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter;
use Utopia\Cache\Adapter\Memory as CacheMemory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

class FindCacheTest extends TestCase
{
    private Database $database;

    protected function setUp(): void
    {
        $this->database = $this->createDatabase(new CacheMemory());
    }

    private function createDatabase(Adapter $cache): Database
    {
        $database = new Database(new DatabaseMemory(), new Cache($cache));
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

        $documents = $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600);
        $this->assertCount(1, $documents);
        $this->assertSame('first', $documents[0]->getId());

        $this->seedProject($this->database, 'second', 'Second');

        $documents = $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600);
        $this->assertCount(1, $documents);
        $this->assertSame('first', $documents[0]->getId());

        $this->database->purgeCachedFindCollection('projects');

        $documents = $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600);
        $this->assertCount(2, $documents);
        $this->assertSame('first', $documents[0]->getId());
        $this->assertSame('second', $documents[1]->getId());
    }

    public function testFindCachedBypassesCacheWhenTtlIsZero(): void
    {
        $this->seedProject($this->database, 'first', 'First');

        $documents = $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600);
        $this->assertCount(1, $documents);

        $this->seedProject($this->database, 'second', 'Second');

        $documents = $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 0);
        $this->assertCount(2, $documents);
    }

    public function testFindCachedTriggersFindEventOnCacheHit(): void
    {
        $events = [];
        $this->database->on(Database::EVENT_DOCUMENT_FIND, 'test', function (string $event) use (&$events): void {
            $events[] = $event;
        });

        $this->seedProject($this->database, 'first', 'First');

        $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600);
        $this->database->findCached('projects', [Query::orderAsc('name')], ttl: 3600);

        $this->assertSame([
            Database::EVENT_DOCUMENT_FIND,
            Database::EVENT_DOCUMENT_FIND,
        ], $events);
    }

    public function testPurgeCachedFindRemovesOnlyOneCallerKey(): void
    {
        $database = $this->createDatabase(new HashMemoryCache());

        $this->seedProject($database, 'first', 'First');

        $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, key: 'a');
        $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, key: 'b');

        $this->seedProject($database, 'second', 'Second');

        $database->purgeCachedFind('projects', key: 'a');

        $documents = $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, key: 'a');
        $this->assertCount(2, $documents);

        $documents = $database->findCached('projects', [Query::orderAsc('name')], ttl: 3600, key: 'b');
        $this->assertCount(1, $documents);
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
