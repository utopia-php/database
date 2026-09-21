<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

class FilterRegistryTest extends TestCase
{
    private DatabaseMemory $adapter;

    private Cache $cache;

    private string $namespace;

    private Database $database;

    protected function setUp(): void
    {
        $this->adapter = new DatabaseMemory();
        $this->cache = new Cache(new HashAwareMemoryCache());
        $this->namespace = 'filter_registry_' . \uniqid();

        $this->database = $this->createDatabase();
        $this->database->create();
        $this->database->createCollection('projects');
        $this->database->createAttribute('projects', 'name', Database::VAR_STRING, 255, false);
        $this->database->createDocument('projects', new Document([
            '$id' => 'project',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'cached',
        ]));
    }

    private function createDatabase(): Database
    {
        $database = new Database($this->adapter, $this->cache);

        return $database
            ->setDatabase('utopiaTests')
            ->setNamespace($this->namespace);
    }

    /**
     * Write through the adapter, bypassing Database and therefore the cache
     * purge, so the cache holds a copy the source no longer agrees with. A read
     * returning 'cached' was served from the cache; one returning 'fresh' missed
     * and went to the adapter.
     */
    private function writeBehindTheCache(string $value): void
    {
        $collection = $this->database->getCollection('projects');
        $document = $this->adapter->getDocument($collection, 'project');
        $document->setAttribute('name', $value);
        $this->adapter->updateDocument($collection, 'project', $document, true);
    }

    private function read(?Database $database = null): string
    {
        return ($database ?? $this->database)
            ->getDocument('projects', 'project')
            ->getAttribute('name');
    }

    public function testRegisteringAGlobalFilterStopsStaleEntriesBeingServed(): void
    {
        $this->assertSame('cached', $this->read());

        $this->writeBehindTheCache('fresh');
        $this->assertSame('cached', $this->read(), 'read should still be served from cache');

        $noop = fn (mixed $value) => $value;
        Database::addFilter(__FUNCTION__, $noop, $noop);

        $this->assertSame(
            'fresh',
            $this->read(),
            'a document cached under the previous filter set must not be served after it changes',
        );
    }

    public function testInstancesSharingAConfigShareCachedDocuments(): void
    {
        $this->assertSame('cached', $this->read());

        $this->writeBehindTheCache('fresh');

        $this->assertSame(
            'cached',
            $this->read($this->createDatabase()),
            'a later instance with the same config must hit the entry the first one cached',
        );
    }
}
