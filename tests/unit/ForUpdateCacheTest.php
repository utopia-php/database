<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as CacheMemory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

class ForUpdateCacheTest extends TestCase
{
    private DatabaseMemory $adapter;

    private Database $database;

    private Cache $cache;

    protected function setUp(): void
    {
        $this->adapter = new DatabaseMemory();
        $this->cache = new Cache(new CacheMemory());
        $this->database = new Database($this->adapter, $this->cache);
        $this->database
            ->setDatabase('utopiaTests')
            ->setNamespace('for_update_' . \uniqid());

        $this->database->create();
        $this->database->createCollection('projects');
        $this->database->createAttribute('projects', 'name', Database::VAR_STRING, 255, false);
        $this->database->createAttribute('projects', 'description', Database::VAR_STRING, 255, false);
        $this->database->createDocument('projects', new Document([
            '$id' => 'project',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'stale',
            'description' => 'stale',
        ]));
    }

    /**
     * Write to the row through the adapter, bypassing Database and therefore
     * the cache purge — the cache now holds a stale copy, exactly as if a
     * concurrent reader had re-cached the old row after a writer's purge.
     */
    private function staleCache(string $attribute, string $value): void
    {
        $collection = $this->database->getCollection('projects');
        $document = $this->adapter->getDocument($collection, 'project');
        $document->setAttribute($attribute, $value);
        $this->adapter->updateDocument($collection, 'project', $document, true);
    }

    public function testForUpdateReadBypassesStaleCache(): void
    {
        $cached = $this->database->getDocument('projects', 'project');
        $this->assertSame('stale', $cached->getAttribute('name'));

        $this->staleCache('name', 'fresh');

        $cached = $this->database->getDocument('projects', 'project');
        $this->assertSame('stale', $cached->getAttribute('name'));

        $fresh = $this->database->getDocument('projects', 'project', forUpdate: true);
        $this->assertSame('fresh', $fresh->getAttribute('name'));
    }

    public function testForUpdateReadDoesNotPopulateCache(): void
    {
        $this->database->getDocument('projects', 'project');
        $this->staleCache('name', 'fresh');

        $this->database->getDocument('projects', 'project', forUpdate: true);

        // The locking read happens inside an open transaction; caching what it
        // saw would publish a pre-commit row. The cached copy must be untouched.
        $cached = $this->database->getDocument('projects', 'project');
        $this->assertSame('stale', $cached->getAttribute('name'));
    }

    public function testUpdateDocumentDoesNotResurrectStaleCachedAttributes(): void
    {
        $this->database->getDocument('projects', 'project');
        $this->staleCache('name', 'fresh');

        // updateDocument merges the changes into its locking read of the current
        // row. Served from the stale cache, that merge would durably revert
        // name to 'stale' — the lost-update behind flaky project config tests.
        $this->database->updateDocument('projects', 'project', new Document([
            'description' => 'updated',
        ]));

        $collection = $this->database->getCollection('projects');
        $row = $this->adapter->getDocument($collection, 'project');
        $this->assertSame('fresh', $row->getAttribute('name'));
        $this->assertSame('updated', $row->getAttribute('description'));

        $document = $this->database->getDocument('projects', 'project');
        $this->assertSame('fresh', $document->getAttribute('name'));
        $this->assertSame('updated', $document->getAttribute('description'));
    }

    public function testReadRejectsStaleCacheSnapshotReCachedAfterUpdate(): void
    {
        // Prime the cache with the original snapshot.
        $original = $this->database->getDocument('projects', 'project');
        $this->assertSame('stale', $original->getAttribute('name'));

        // Commit a newer version through Database: this purges the cache and
        // records the committed $updatedAt as the version marker. usleep guarantees
        // a strictly later $updatedAt than the snapshot captured above.
        \usleep(2000);
        $this->database->updateDocument('projects', 'project', new Document([
            'name' => 'fresh',
        ]));

        // Simulate a concurrent reader whose pre-commit read landed in the cache
        // after the writer's purge — the exact race the version marker defends
        // against. The resurrected snapshot carries the OLD $updatedAt.
        [, $documentKey, $hashKey] = $this->database->getCacheKeys('projects', 'project');
        $this->cache->save($documentKey, $original->getArrayCopy(), $hashKey);

        // A normal (non-locking) read must not serve the resurrected stale snapshot:
        // its $updatedAt is older than the recorded version marker, so it is rejected
        // and the row is reloaded from the adapter.
        $served = $this->database->getDocument('projects', 'project');
        $this->assertSame('fresh', $served->getAttribute('name'));
    }
}
