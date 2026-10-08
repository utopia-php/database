<?php

namespace Tests\Unit\Documents;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Cache\CountingCache;
use Tests\Unit\Cache\RedisLeasableCache;
use Tests\Unit\Support\CountingMemory;
use Tests\Unit\Support\UncachedTwin;
use Utopia\Cache\Cache;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Hook\Decorator;
use Utopia\Database\Permission;
use Utopia\Database\PermissionType;
use Utopia\Database\Role;
use Utopia\Query\CursorDirection;

/**
 * A single-document write reads the document it writes under the definition it resolved for the write, instead of
 * resolving that definition a second time for its locked read.
 */
final class ResolvedCollectionReadTest extends TestCase
{
    private const string COLLECTION = 'webhooks';

    /**
     * @return array<string, array{Closure(Database): mixed}>
     */
    public static function writes(): array
    {
        return [
            'updateDocument' => [static fn (Database $database): Document => $database->updateDocument(self::COLLECTION, 'hook', new Document(['name' => 'renamed']))],
            'increaseDocumentAttribute' => [static fn (Database $database): Document => $database->increaseDocumentAttribute(self::COLLECTION, 'hook', 'count')],
            'decreaseDocumentAttribute' => [static fn (Database $database): Document => $database->decreaseDocumentAttribute(self::COLLECTION, 'hook', 'count')],
            'deleteDocument' => [static fn (Database $database): bool => $database->deleteDocument(self::COLLECTION, 'hook')],
        ];
    }

    /**
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('writes')]
    public function testAWriteOfACachedDocumentTakesThreeRoundTrips(Closure $write): void
    {
        [$database, $adapter, $cache] = $this->createDatabase();

        $adapter->reset();
        $cache->resetOperations();
        $write($database);

        $this->assertSame(3, $cache->getOperations(), 'One definition load, the purge inside the transaction and the purge after it commits');
        $this->assertSame(0, $adapter->metadataReads);
    }

    /**
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('writes')]
    public function testAWriteLoadsItsDefinitionOnce(Closure $write): void
    {
        [$database, , $cache] = $this->createDatabase();
        $definitionKey = \strtolower($database->getCacheKeys(Database::METADATA, self::COLLECTION)[1]);

        $cache->resetOperations();
        $write($database);

        $this->assertSame(1, $cache->getLoads()[$definitionKey] ?? 0);
    }

    public function testAnUpdateReturnsTheDocumentAReadReturnsAfterIt(): void
    {
        [$database] = $this->createDatabase();

        $updated = $database->updateDocument(self::COLLECTION, 'hook', new Document(['name' => 'renamed']));

        $this->assertSame('renamed', $updated->getAttribute('name'));
        $this->assertSame(UncachedTwin::of($database)->getDocument(self::COLLECTION, 'hook')->getArrayCopy(), $updated->getArrayCopy());
        $this->assertSame($updated->getArrayCopy(), $database->getDocument(self::COLLECTION, 'hook')->getArrayCopy());
    }

    public function testTheCountersReturnTheDocumentAReadReturnsAfterThem(): void
    {
        [$database] = $this->createDatabase();

        $increased = $database->increaseDocumentAttribute(self::COLLECTION, 'hook', 'count', 5);
        $this->assertSame(6, $increased->getAttribute('count'));
        $this->assertSame('hook', $increased->getAttribute('name'));
        $this->assertSame($increased->getId(), UncachedTwin::of($database)->getDocument(self::COLLECTION, 'hook')->getId());
        $this->assertSame(6, $database->getDocument(self::COLLECTION, 'hook')->getAttribute('count'));

        $decreased = $database->decreaseDocumentAttribute(self::COLLECTION, 'hook', 'count', 2);
        $this->assertSame(4, $decreased->getAttribute('count'));
        $this->assertSame('hook', $decreased->getAttribute('name'));
        $this->assertSame(4, UncachedTwin::of($database)->getDocument(self::COLLECTION, 'hook')->getAttribute('count'));
        $this->assertSame(4, $database->getDocument(self::COLLECTION, 'hook')->getAttribute('count'));
    }

    public function testADeleteRemovesTheDocumentForEveryReader(): void
    {
        [$database] = $this->createDatabase();

        $this->assertTrue($database->deleteDocument(self::COLLECTION, 'hook'));

        $this->assertTrue($database->getDocument(self::COLLECTION, 'hook')->isEmpty());
        $this->assertTrue(UncachedTwin::of($database)->getDocument(self::COLLECTION, 'hook')->isEmpty());
    }

    public function testWritesOfAMissingDocumentStillFail(): void
    {
        [$database] = $this->createDatabase();

        $this->assertTrue($database->updateDocument(self::COLLECTION, 'missing', new Document(['name' => 'renamed']))->isEmpty());
        $this->assertFalse($database->deleteDocument(self::COLLECTION, 'missing'));

        $this->expectException(\Utopia\Database\Exception\NotFound::class);
        $database->increaseDocumentAttribute(self::COLLECTION, 'missing', 'count');
    }

    /**
     * A schema change committed after a write resolved its definition and before its transaction locks the row is
     * not seen by the locked read: the row is cast and decoded under the definition the write validates, encodes and
     * writes with, as that write never honoured the newer one.
     *
     * @param  Closure(Database): mixed  $write
     */
    #[DataProvider('writes')]
    public function testTheLockedReadUsesTheDefinitionTheWriteResolved(Closure $write): void
    {
        $adapter = new class () extends CountingMemory {
            private const string LOCKED = 'webhooks';

            /** @var (Closure(): void)|null */
            public ?Closure $beforeTransaction = null;

            /** @var list<list<string>> */
            public array $lockedReads = [];

            #[\Override]
            public function startTransaction(): bool
            {
                $before = $this->beforeTransaction;
                $this->beforeTransaction = null;
                if ($before !== null) {
                    $before();
                }

                return parent::startTransaction();
            }

            #[\Override]
            public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                if ($forUpdate && $collection->getId() === self::LOCKED) {
                    $this->lockedReads[] = \array_map(
                        static fn (Attribute $attribute): string => $attribute->key,
                        Collection::fromDocument($collection)->attributes(),
                    );
                }

                return parent::getDocument($collection, $id, $queries, $forUpdate);
            }
        };
        [$database, , , $cache] = $this->createDatabase($adapter);
        $concurrent = (new Database($adapter, $cache))
            ->setAuthorization($database->getAuthorization())
            ->setDatabase($database->getDatabase())
            ->setNamespace($database->getNamespace());
        $adapter->beforeTransaction = static function () use ($concurrent): void {
            $concurrent->createAttribute(self::COLLECTION, Attribute::string(key: 'label'));
        };

        $write($database);

        $this->assertNull($adapter->beforeTransaction, 'The schema change ran between the definition lookup and the locked read');
        $this->assertSame([['name', 'count']], $adapter->lockedReads);
        $this->assertSame(['name', 'count', 'label'], \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $database->getCollection(self::COLLECTION)->attributes(),
        ), 'The next call resolves the changed definition');
    }

    /**
     * @return array<string, array{Closure(Database, ?callable): int}>
     */
    public static function bulkWrites(): array
    {
        return [
            'updateDocuments' => [static fn (Database $database, ?callable $onNext): int => $database->updateDocuments(self::COLLECTION, new Document(['name' => 'renamed']), batchSize: 2, onNext: $onNext)],
            'deleteDocuments' => [static fn (Database $database, ?callable $onNext): int => $database->deleteDocuments(self::COLLECTION, batchSize: 2, onNext: $onNext)],
        ];
    }

    /**
     * @param  Closure(Database, ?callable): int  $write
     */
    #[DataProvider('bulkWrites')]
    public function testABulkWriteReadsNoDefinitionForItsPages(Closure $write): void
    {
        [$database, $adapter] = $this->createDatabase();
        $this->createSiblings($database);

        $adapter->reset();
        $this->assertSame(6, $write($database, null));

        $this->assertSame(0, $adapter->metadataReads, 'Three pages of a cached definition read no _metadata row');
    }

    /**
     * @param  Closure(Database, ?callable): int  $write
     */
    #[DataProvider('bulkWrites')]
    public function testABulkWriteOfAnUncachedDefinitionReadsItOnce(Closure $write): void
    {
        [$database, $adapter] = $this->createDatabase();
        $this->createSiblings($database);
        $database->purgeCachedDocument(Database::METADATA, self::COLLECTION);

        $adapter->reset();
        $this->assertSame(6, $write($database, null));

        $this->assertSame(1, $adapter->metadataReads);
    }

    /**
     * A schema change committed while a bulk write pages is not seen by its later pages: every page is read under
     * the definition the call validated its queries and encoded its updates with.
     *
     * @param  Closure(Database, ?callable): int  $write
     */
    #[DataProvider('bulkWrites')]
    public function testEveryPageOfABulkWriteUsesTheDefinitionItResolved(Closure $write): void
    {
        $adapter = new class () extends CountingMemory {
            private const string PAGED = 'webhooks';

            /** @var list<list<string>> */
            public array $pages = [];

            #[\Override]
            public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], CursorDirection $cursorDirection = CursorDirection::After, PermissionType $forPermission = PermissionType::Read): array
            {
                if ($collection->getId() === self::PAGED && $forPermission !== PermissionType::Read) {
                    $this->pages[] = \array_map(
                        static fn (Attribute $attribute): string => $attribute->key,
                        Collection::fromDocument($collection)->attributes(),
                    );
                }

                return parent::find($collection, $queries, $limit, $offset, $orderAttributes, $orderTypes, $cursor, $cursorDirection, $forPermission);
            }
        };
        [$database, , , $cache] = $this->createDatabase($adapter);
        $this->createSiblings($database);
        $concurrent = (new Database($adapter, $cache))
            ->setAuthorization($database->getAuthorization())
            ->setDatabase($database->getDatabase())
            ->setNamespace($database->getNamespace());
        $changed = false;
        $onNext = static function () use ($concurrent, &$changed): void {
            if (! $changed) {
                $changed = true;
                $concurrent->createAttribute(self::COLLECTION, Attribute::string(key: 'label'));
            }
        };

        $this->assertSame(6, $write($database, $onNext));

        $this->assertTrue($changed);
        $this->assertSame(\array_fill(0, 4, ['name', 'count']), $adapter->pages);
        $this->assertSame(['name', 'count', 'label'], \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $database->getCollection(self::COLLECTION)->attributes(),
        ), 'The next call resolves the changed definition');
    }

    public function testADecoratorMutatingTheMetadataDefinitionLeavesTheNextReadIntact(): void
    {
        [$database] = $this->createDatabase();
        $database->addHook(new class () implements Decorator {
            #[\Override]
            public function decorate(Event $event, Document $collection, Document $document): Document
            {
                if ($collection->getId() === Database::METADATA) {
                    $collection->setAttribute(Collection::NAME, 'mutated');
                    $collection->setAttribute(Collection::ATTRIBUTES, []);
                    $collection->setAttribute(Collection::INDEXES, []);
                }

                return $document;
            }
        });

        foreach ([1, 2] as $read) {
            $stored = $database->getDocument(Database::METADATA, self::COLLECTION);
            $this->assertSame(self::COLLECTION, $stored->getId(), "Read {$read}");
            $attributes = $stored->getAttribute(Collection::ATTRIBUTES);
            $this->assertIsArray($attributes, "Read {$read} decodes the attributes under the metadata definition");
            $this->assertCount(2, $attributes);
        }

        $definition = Database::collectionDefinition();
        $this->assertSame('collections', $definition->getAttribute(Collection::NAME));
        $this->assertCount(4, $definition->attributes());
        $this->assertSame(['name', 'count'], \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $database->getCollection(self::COLLECTION)->attributes(),
        ));
    }

    private function createSiblings(Database $database): void
    {
        foreach (['b', 'c', 'd', 'e', 'f'] as $id) {
            $database->createDocument(self::COLLECTION, new Document([
                '$id' => $id,
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())],
                'name' => $id,
                'count' => 1,
            ]));
        }
        $database->getCollection(self::COLLECTION);
    }

    /**
     * @return array{Database, CountingMemory, CountingCache, Cache}
     */
    private function createDatabase(?CountingMemory $adapter = null): array
    {
        $adapter ??= new CountingMemory();
        $counting = new CountingCache(new RedisLeasableCache());
        $cache = new Cache($counting);
        $database = (new Database($adapter, $cache))
            ->setDatabase('utopiaTests')
            ->setNamespace('resolved_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(Collection::create(id: self::COLLECTION, attributes: [
            Attribute::string(key: 'name'),
            Attribute::integer(key: 'count', default: 10),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));
        $database->createDocument(self::COLLECTION, new Document([
            '$id' => 'hook',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())],
            'name' => 'hook',
            'count' => 1,
        ]));
        $database->getDocument(self::COLLECTION, 'hook');

        return [$database, $adapter, $counting, $cache];
    }
}
