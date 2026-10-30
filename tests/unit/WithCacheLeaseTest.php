<?php

namespace Tests\Unit;

use Closure;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;

class WithCacheLeaseTest extends TestCase
{
    private Cache $cache;

    private DatabaseMemory $adapter;

    private Database $database;

    private string $key;

    #[\Override]
    protected function setUp(): void
    {
        $this->cache = new Cache(new \Utopia\Cache\Adapter\Memory());
        $this->adapter = new DatabaseMemory();
        $this->database = new Database($this->adapter, $this->cache);
        $this->database
            ->setDatabase('utopiaTests')
            ->setNamespace('with_cache_' . \uniqid());

        $this->database->create();
        $this->database->createCollection(Collection::create(id: 'projects'));
        $this->database->createAttribute('projects', Attribute::string(key: 'name'));
        $this->database->createDocument('projects', new Document([
            '$id' => 'project',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'fresh',
        ]));

        $this->key = $this->database->getQueryCacheKey('projects');
    }

    /**
     * Write to the row through the adapter, bypassing Database and therefore the
     * cache purge, so the cache is left holding the previous copy.
     */
    private function staleCache(string $attribute, string $value): void
    {
        $collection = $this->database->getCollection('projects');
        $document = $this->adapter->getDocument($collection, 'project');
        $document->setAttribute($attribute, $value);
        $this->adapter->updateDocument($collection, 'project', $document, true);
    }

    public function testDocumentPurgeRemovesAllVariants(): void
    {
        $plain = fn (): mixed => $this->database->getDocument('projects', 'project')->getAttribute('name');
        $projected = fn (): mixed => $this->database
            ->getDocument('projects', 'project', [Query::select(['name'])])
            ->getAttribute('name');

        $this->assertSame('fresh', $plain());
        $this->assertSame('fresh', $projected());

        $this->staleCache('name', 'changed');

        // Both variants still answer 'fresh' from cache, so the purge has something to invalidate.
        $this->assertSame('fresh', $plain());
        $this->assertSame('fresh', $projected());

        $this->database->purgeCachedDocument('projects', 'project');

        $this->assertSame('changed', $plain());
        $this->assertSame('changed', $projected());
    }

    public function testAReadInFlightDuringAPurgeCannotCacheWhatItRead(): void
    {
        $adapter = new class () extends DatabaseMemory {
            public ?Closure $afterNextDocumentRead = null;

            #[\Override]
            public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                $document = parent::getDocument($collection, $id, $queries, $forUpdate);

                $callback = $collection->getId() === Database::METADATA ? null : $this->afterNextDocumentRead;
                if ($callback !== null) {
                    $this->afterNextDocumentRead = null;
                    $callback();
                }

                return $document;
            }
        };
        $database = new Database($adapter, new Cache(new \Utopia\Cache\Adapter\Memory()));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('with_cache_in_flight_' . \uniqid());
        $database->create();
        $database->createCollection(Collection::create(id: 'projects'));
        $database->createAttribute('projects', Attribute::string(key: 'name'));
        $database->createDocument('projects', new Document([
            '$id' => 'project',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'fresh',
        ]));

        $adapter->afterNextDocumentRead = function () use ($adapter, $database): void {
            $collection = $database->getCollection('projects');
            $row = $adapter->getDocument($collection, 'project')->setAttribute('name', 'changed');
            $adapter->updateDocument($collection, 'project', $row, true);
            $database->purgeCachedDocument('projects', 'project');
        };

        $this->assertSame('fresh', $database->getDocument('projects', 'project')->getAttribute('name'), 'The read returns the row it read before the purge');
        $this->assertSame('changed', $database->getDocument('projects', 'project')->getAttribute('name'), 'The read that started before the purge must not have cached its row');
    }

    public function testStaleListWriteAfterConcurrentPurgeIsRejected(): void
    {
        $hash = 'list-hash';

        $document = $this->database->getDocument('projects', 'project');

        // The callback stands in for the database read of an older request: a
        // concurrent writer purges the query key after the read started but
        // before the result is cached. Without a lease the stale list below
        // would land in the cache after the purge.
        $result = $this->database->withCache($this->key, function () use ($document) {
            $this->database->purgeCachedQueries('projects');

            return [$document];
        }, $hash);

        /** @var mixed $result */
        $this->assertIsArray($result);
        $this->assertCount(1, $result);
        $callbackCalls = 0;
        $fresh = $this->database->withCache($this->key, function () use (&$callbackCalls): array {
            $callbackCalls++;

            return [];
        }, $hash);
        /** @var mixed $fresh */
        $this->assertSame([], $fresh);
        $this->assertSame(1, $callbackCalls);
    }

    public function testListWriteLandsWhenNoConcurrentPurge(): void
    {
        $hash = 'list-hash';

        $document = $this->database->getDocument('projects', 'project');

        $result = $this->database->withCache($this->key, fn () => [$document], $hash);

        /** @var mixed $result */
        $this->assertIsArray($result);
        $this->assertCount(1, $result);
        $callbackCalls = 0;
        $cached = $this->database->withCache($this->key, function () use (&$callbackCalls): array {
            $callbackCalls++;

            return [];
        }, $hash);
        /** @var mixed $cached */
        $this->assertIsArray($cached);
        $this->assertCount(1, $cached);
        $this->assertSame(0, $callbackCalls);
    }
}
