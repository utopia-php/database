<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Cache\CountingCache;
use Tests\Unit\Cache\RedisLeasableCache;
use Tests\Unit\Support\CountingMemory;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

/**
 * Cache round trips of the core operations on a warm cache, pinned at or below database 7.3.12's (bench-core,
 * MariaDB): one round trip per cache lookup, the collection's document-cache epoch travelling with its definition,
 * and what that lookup must keep: an epoch per tenant, and `_metadata` purges that still reach every definition.
 */
final class DocumentCacheRoundTripTest extends TestCase
{
    public function testAGetCollectionHitCostsOneRoundTrip(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->getCollection('webhooks');

        $adapter->reset();
        $cache->resetOperations();
        $this->assertFalse($database->getCollection('webhooks')->isEmpty());

        $this->assertSame(1, $cache->getOperations(), 'getCollection() on a warm cache (7.3.12: 1 round trip)');
        $this->assertSame(0, $adapter->metadataReads);
    }

    public function testAGetDocumentHitCostsTwoRoundTrips(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->createDocument('webhooks', $this->hook('hook'));
        $database->getDocument('webhooks', 'hook');

        $adapter->reset();
        $cache->resetOperations();
        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'));

        $this->assertSame(2, $cache->getOperations(), 'getDocument() on a warm cache: the collection lookup and the document (7.3.12: 2 round trips)');
        $this->assertSame(0, $adapter->documentReads + $adapter->metadataReads);
    }

    public function testACachedMissCostsTwoRoundTrips(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->getDocument('webhooks', 'missing');

        $adapter->reset();
        $cache->resetOperations();
        $this->assertTrue($database->getDocument('webhooks', 'missing')->isEmpty());

        $this->assertSame(2, $cache->getOperations(), 'getDocument() of a missing document on a warm cache (7.3.12: 2 round trips)');
        $this->assertSame(0, $adapter->documentReads);
    }

    public function testAnUncachedGetDocumentCostsFourRoundTrips(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->createDocument('webhooks', $this->hook('hook'));
        $database->getCollection('webhooks');

        $adapter->reset();
        $cache->resetOperations();
        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'));

        $this->assertSame(4, $cache->getOperations(), 'getDocument() of an uncached document: the collection lookup, the document, its lease and the fill (7.3.12: 5 round trips)');
        $this->assertSame(1, $adapter->documentReads);
    }

    /**
     * @return array<string, array{Closure(Database): mixed}>
     */
    public static function collectionReads(): array
    {
        return [
            'find' => [static fn (Database $database): array => $database->find('webhooks', [Query::equal('name', ['hook'])])],
            'count' => [static fn (Database $database): int => $database->count('webhooks', [Query::equal('name', ['hook'])])],
            'sum' => [static fn (Database $database): int|float => $database->sum('webhooks', 'count')],
        ];
    }

    /**
     * @param  Closure(Database): mixed  $read
     */
    #[DataProvider('collectionReads')]
    public function testCollectionReadsCostOneRoundTripBeforeTheirStatement(Closure $read): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->createDocument('webhooks', $this->hook('hook'));
        $read($database);

        $adapter->reset();
        $cache->resetOperations();
        $read($database);

        $this->assertSame(1, $cache->getOperations(), 'find(), count() and sum() without a query cache look up their collection only (7.3.12: 1 round trip)');
        $this->assertSame(0, $adapter->metadataReads);
    }

    public function testASiblingReadAfterAWriteCostsTwoRoundTripsAndNoRead(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->createDocument('webhooks', $this->hook('written'));
        $database->createDocument('webhooks', $this->hook('sibling'));
        $database->getDocument('webhooks', 'sibling');
        $database->updateDocument('webhooks', 'written', new Document(['name' => 'renamed']));

        $adapter->reset();
        $cache->resetOperations();
        $this->assertSame('hook', $database->getDocument('webhooks', 'sibling')->getAttribute('name'));

        $this->assertSame(2, $cache->getOperations(), 'A read of a sibling after a write (7.3.12: 2 round trips)');
        $this->assertSame(0, $adapter->documentReads + $adapter->metadataReads, 'A read of a sibling after a write (7.3.12: 0 statements)');
    }

    public function testATenantNeverServesUnderAnotherTenantsEpochOfAGlobalDefinition(): void
    {
        $adapter = new CountingMemory();
        $database = new Database($adapter, new Cache(new RedisLeasableCache()));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('global_'.\uniqid())
            ->setSharedTables(true)
            ->setTenant(null)
            ->setGlobalCollections(['webhooks']);
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(new Collection(id: 'webhooks', attributes: [
            Attribute::string(key: 'name'),
            Attribute::integer(key: 'count', default: 10),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
        ]));
        foreach ([1, 2] as $tenant) {
            $database->setTenant($tenant);
            $database->createDocument('webhooks', $this->hook('hook'));
        }
        $database->purgeCachedDocument(Database::METADATA, 'webhooks');

        $database->setTenant(2);
        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        $database->setTenant(1);
        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'));

        $database->updateDocuments('webhooks', new Document(['name' => 'renamed']));
        $database->setTenant(2);
        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        $database->setTenant(1);

        $this->assertSame('renamed', $database->getDocument('webhooks', 'hook')->getAttribute('name'), 'A global definition is shared by every tenant, but the epoch it carries is each tenant\'s own');
    }

    public function testPurgingTheMetadataCollectionRereadsEveryDefinition(): void
    {
        [$database, $adapter] = $this->createDatabase();
        $database->createCollection(new Collection(id: 'logs', permissions: [Permission::read(Role::any())]));
        $database->getCollection('webhooks');
        $database->getCollection('logs');

        $database->purgeCachedCollection(Database::METADATA);
        $adapter->reset();
        $this->assertFalse($database->getCollection('webhooks')->isEmpty());
        $this->assertFalse($database->getCollection('logs')->isEmpty());

        $this->assertSame(2, $adapter->metadataReads, 'purgeCachedCollection(\'_metadata\') must retire every cached definition');
    }

    public function testPurgingTheMetadataCollectionRetiresACachedMissingCollection(): void
    {
        [$database, $adapter] = $this->createDatabase();
        $this->assertTrue($database->getCollection('logs')->isEmpty());

        $uncached = new Database($adapter, new Cache(new None()));
        $uncached
            ->setDatabase('utopiaTests')
            ->setNamespace($database->getNamespace());
        $uncached->createCollection(new Collection(id: 'logs', permissions: [Permission::read(Role::any())]));
        $this->assertTrue($database->getCollection('logs')->isEmpty(), 'A definition written without this cache leaves the cached miss in place');

        $database->purgeCachedCollection(Database::METADATA);

        $this->assertFalse($database->getCollection('logs')->isEmpty(), 'purgeCachedCollection(\'_metadata\') must retire a cached missing collection');
    }

    /**
     * @return array{Database, CountingMemory, CountingCache}
     */
    private function createDatabase(): array
    {
        $adapter = new CountingMemory();
        $cache = new CountingCache(new RedisLeasableCache());
        $database = $this->configure(new Database($adapter, new Cache($cache)), 'round_trips_'.\uniqid());
        $database->create();
        $database->createCollection(new Collection(id: 'webhooks', attributes: [
            Attribute::string(key: 'name'),
            Attribute::integer(key: 'count', default: 10),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        return [$database, $adapter, $cache];
    }

    private function hook(string $id): Document
    {
        return new Document([
            '$id' => $id,
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'hook',
            'count' => 1,
        ]);
    }

    private function configure(Database $database, string $namespace): Database
    {
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace($namespace);
        $database->getAuthorization()->addRole(Role::any()->toString());

        return $database;
    }
}
