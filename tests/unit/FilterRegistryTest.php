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

    /**
     * @var array<string, array{encode: callable, decode: callable, signature: string}>
     */
    private array $registry;

    protected function setUp(): void
    {
        $this->adapter = new DatabaseMemory();
        $this->cache = new Cache(new HashAwareMemoryCache());
        $this->namespace = 'filter_registry_' . \uniqid();

        $this->database = $this->createDatabase();

        // Snapshot once the constructor has registered the built-ins, so the
        // restore in tearDown puts back a populated registry rather than an
        // empty one.
        $this->registry = (new \ReflectionProperty(Database::class, 'filters'))->getValue();

        $this->database->create();
        $this->database->createCollection('projects');
        $this->database->createAttribute('projects', 'name', Database::VAR_STRING, 255, false);
        $this->database->createDocument('projects', new Document([
            '$id' => 'project',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'cached',
        ]));
    }

    protected function tearDown(): void
    {
        // addFilter() writes to a static registry with no removal API, so a test
        // registering one would otherwise leak into every later test.
        (new \ReflectionProperty(Database::class, 'filters'))->setValue(null, $this->registry);
        (new \ReflectionProperty(Database::class, 'defaultFiltersRegistered'))->setValue(null, true);
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

    public function testChangingInstanceFiltersStopsStaleEntriesBeingServed(): void
    {
        $database = new class ($this->adapter, $this->cache) extends Database {
            public function swapInstanceFilter(string $signature): void
            {
                $noop = fn (mixed $value) => $value;

                $this->instanceFilters = [
                    'probe' => ['encode' => $noop, 'decode' => $noop, 'signature' => $signature],
                ];
            }
        };
        $database->setDatabase('utopiaTests')->setNamespace($this->namespace);

        $this->assertSame('cached', $this->read($database));

        $this->writeBehindTheCache('fresh');
        $this->assertSame('cached', $this->read($database), 'read should still be served from cache');

        $database->swapInstanceFilter('v2');

        $this->assertSame(
            'fresh',
            $this->read($database),
            'a subclass replacing its instance filters must not keep serving the previous entry',
        );
    }

    public function testOverridingABuiltInFilterBeforeTheFirstInstanceStillWins(): void
    {
        $registry = new \ReflectionProperty(Database::class, 'filters');

        // A fresh process: nothing has constructed a Database yet, so the
        // built-ins are not in the registry.
        $registry->setValue(null, []);
        (new \ReflectionProperty(Database::class, 'defaultFiltersRegistered'))->setValue(null, false);

        $identity = fn (mixed $value) => $value;
        Database::addFilter('datetime', $identity, $identity);
        $override = $registry->getValue()['datetime']['signature'];

        $this->createDatabase();

        $this->assertSame(
            $override,
            $registry->getValue()['datetime']['signature'],
            'constructing a database must not restore a built-in filter the caller replaced before it',
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
