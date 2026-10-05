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
 * Cache round trips and adapter reads of the core operations on a warm cache, bounded by database 7.3.12's
 * (bench-core, MariaDB), and the freshness those bounds must not cost: documents cached per tenant, and `_metadata`
 * purges that still reach every definition.
 */
final class DocumentCacheRoundTripTest extends TestCase
{
    public function testAGetCollectionHitStaysWithinSevenThreeRoundTrips(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->getCollection('webhooks');

        $adapter->reset();
        $cache->resetOperations();
        $this->assertFalse($database->getCollection('webhooks')->isEmpty());

        $this->assertLessThanOrEqual(1, $cache->getOperations(), '7.3.12: 1');
        $this->assertSame(0, $adapter->metadataReads);
    }

    public function testAGetDocumentHitStaysWithinSevenThreeRoundTrips(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->createDocument('webhooks', $this->hook('hook'));
        $database->getDocument('webhooks', 'hook');

        $adapter->reset();
        $cache->resetOperations();
        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'));

        $this->assertLessThanOrEqual(2, $cache->getOperations(), '7.3.12: 2');
        $this->assertSame(0, $adapter->documentReads + $adapter->metadataReads);
    }

    public function testACachedMissStaysWithinSevenThreeRoundTrips(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->getDocument('webhooks', 'missing');

        $adapter->reset();
        $cache->resetOperations();
        $this->assertTrue($database->getDocument('webhooks', 'missing')->isEmpty());

        $this->assertLessThanOrEqual(2, $cache->getOperations(), '7.3.12: 2');
        $this->assertSame(0, $adapter->documentReads);
    }

    public function testAnUncachedGetDocumentStaysWithinSevenThreeRoundTrips(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->createDocument('webhooks', $this->hook('hook'));
        $database->getCollection('webhooks');

        $adapter->reset();
        $cache->resetOperations();
        $this->assertSame('hook', $database->getDocument('webhooks', 'hook')->getAttribute('name'));

        $this->assertLessThanOrEqual(5, $cache->getOperations(), '7.3.12: 5');
        $this->assertSame(1, $adapter->documentReads, 'An uncached document is read once');
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
    public function testCollectionReadsStayWithinSevenThreeRoundTrips(Closure $read): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->createDocument('webhooks', $this->hook('hook'));
        $read($database);

        $adapter->reset();
        $cache->resetOperations();
        $read($database);

        $this->assertLessThanOrEqual(1, $cache->getOperations(), '7.3.12: 1');
        $this->assertSame(0, $adapter->metadataReads);
    }

    public function testASiblingReadAfterAWriteStaysWithinSevenThreeRoundTripsAndReadsNothing(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->createDocument('webhooks', $this->hook('written'));
        $database->createDocument('webhooks', $this->hook('sibling'));
        $database->getDocument('webhooks', 'sibling');
        $database->updateDocument('webhooks', 'written', new Document(['name' => 'renamed']));

        $adapter->reset();
        $cache->resetOperations();
        $this->assertSame('hook', $database->getDocument('webhooks', 'sibling')->getAttribute('name'));

        $this->assertLessThanOrEqual(2, $cache->getOperations(), '7.3.12: 2');
        $this->assertSame(0, $adapter->documentReads + $adapter->metadataReads, '7.3.12: 0');
    }

    /**
     * @return array<string, array{Closure(Database): mixed, int}>
     */
    public static function singleDocumentWrites(): array
    {
        return [
            'createDocument' => [
                static fn (Database $database): Document => $database->createDocument('webhooks', new Document([
                    '$id' => 'created',
                    '$permissions' => [Permission::read(Role::any())],
                    'name' => 'created',
                ])),
                3,
            ],
            'updateDocument' => [
                static fn (Database $database): Document => $database->updateDocument('webhooks', 'hook', new Document(['name' => 'renamed'])),
                6,
            ],
            'increaseDocumentAttribute' => [
                static fn (Database $database): Document => $database->increaseDocumentAttribute('webhooks', 'hook', 'count'),
                4,
            ],
            'decreaseDocumentAttribute' => [
                static fn (Database $database): Document => $database->decreaseDocumentAttribute('webhooks', 'hook', 'count'),
                4,
            ],
            'deleteDocument' => [
                static fn (Database $database): bool => $database->deleteDocument('webhooks', 'hook'),
                6,
            ],
        ];
    }

    /**
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('singleDocumentWrites')]
    public function testSingleDocumentWritesStayWithinSevenThreeRoundTrips(Closure $write, int $baseline): void
    {
        [$database, , $cache] = $this->createDatabase();
        $database->createDocument('webhooks', $this->hook('hook'));
        $database->getDocument('webhooks', 'hook');

        $cache->resetOperations();
        $write($database);

        $this->assertLessThanOrEqual($baseline, $cache->getOperations(), "7.3.12: {$baseline}");
    }

    public function testAnUpdateAndAReadInATransactionStayWithinSevenThreeRoundTrips(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->createDocument('webhooks', $this->hook('written'));
        $database->createDocument('webhooks', $this->hook('sibling'));
        $database->getDocument('webhooks', 'written');
        $database->getDocument('webhooks', 'sibling');

        $adapter->reset();
        $cache->resetOperations();
        $read = $database->withTransaction(function () use ($database): Document {
            $database->updateDocument('webhooks', 'written', new Document(['name' => 'renamed']));

            return $database->getDocument('webhooks', 'sibling');
        });

        $this->assertSame('hook', $read->getAttribute('name'));
        $this->assertLessThanOrEqual(11, $cache->getOperations(), '7.3.12: 11');
        $this->assertSame(0, $adapter->metadataReads, '7.3.12: 0');
        $this->assertLessThanOrEqual(1, $adapter->documentReads, '7.3.12: 1');
    }

    public function testAnUpdateAndAReadOfItStayWithinSevenThreeRoundTrips(): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();
        $database->createDocument('webhooks', $this->hook('hook'));
        $database->getDocument('webhooks', 'hook');

        $adapter->reset();
        $cache->resetOperations();
        for ($round = 1; $round <= 10; $round++) {
            $database->updateDocument('webhooks', 'hook', new Document(['name' => 'round '.$round]));
            $this->assertSame('round '.$round, $database->getDocument('webhooks', 'hook')->getAttribute('name'));
        }

        $this->assertLessThanOrEqual(110, $cache->getOperations(), '7.3.12: 110');
        $this->assertLessThanOrEqual(20, $adapter->documentReads, '7.3.12: 20');
        $this->assertSame(0, $adapter->metadataReads);
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

    public function testPurgingTheMetadataCollectionRetiresEveryCachedDefinition(): void
    {
        [$database, $adapter] = $this->createDatabase();
        $database->createCollection(new Collection(id: 'logs', permissions: [Permission::read(Role::any())]));
        $this->assertTrue($database->getCollection('webhooks')->getAttribute('documentSecurity'));
        $this->assertTrue($database->getCollection('logs')->getAttribute('documentSecurity'));

        $uncached = $this->createUncachedTwin($database, $adapter);
        $uncached->updateCollection('webhooks', [Permission::read(Role::any())], false);
        $uncached->updateCollection('logs', [Permission::read(Role::any())], false);
        $this->assertTrue($database->getCollection('webhooks')->getAttribute('documentSecurity'), 'A definition written without this cache leaves the cached definition in place');

        $database->purgeCachedCollection(Database::METADATA);

        $this->assertFalse($database->getCollection('webhooks')->getAttribute('documentSecurity'), 'purgeCachedCollection(\'_metadata\') must retire every cached definition');
        $this->assertFalse($database->getCollection('logs')->getAttribute('documentSecurity'), 'purgeCachedCollection(\'_metadata\') must retire every cached definition');
    }

    public function testPurgingTheMetadataCollectionRetiresACachedMissingCollection(): void
    {
        [$database, $adapter] = $this->createDatabase();
        $this->assertTrue($database->getCollection('logs')->isEmpty());

        $uncached = $this->createUncachedTwin($database, $adapter);
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

    private function createUncachedTwin(Database $database, CountingMemory $adapter): Database
    {
        return (new Database($adapter, new Cache(new None())))
            ->setAuthorization($database->getAuthorization())
            ->setDatabase($database->getDatabase())
            ->setNamespace($database->getNamespace());
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
