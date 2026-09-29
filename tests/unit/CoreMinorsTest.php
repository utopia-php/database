<?php

namespace Tests\Unit;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Character as CharacterException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Database\Validator\Attribute as AttributeValidator;
use Utopia\Query\Schema\ColumnType;

final class CoreMinorsTest extends TestCase
{
    /**
     * @return array<string, array{Throwable}>
     */
    public static function deterministicFailures(): array
    {
        return [
            'authorization' => [new AuthorizationException('denied')],
            'character' => [new CharacterException('bad character')],
            'duplicate' => [new DuplicateException('duplicate')],
            'limit' => [new LimitException('limit')],
            'not found' => [new NotFoundException('missing')],
            'order' => [new OrderException('order')],
            'query' => [new QueryException('query')],
            'relationship' => [new RelationshipException('relationship')],
            'restricted' => [new RestrictedException('restricted')],
            'structure' => [new StructureException('structure')],
            'type' => [new TypeException('type')],
        ];
    }

    #[DataProvider('deterministicFailures')]
    public function testDeterministicFailuresAreNotRetried(Throwable $failure): void
    {
        $writes = 0;
        $database = $this->metadataFailing($failure, $writes);

        $error = $this->attempt(fn (): bool => $database->createAttribute('logs', Attribute::integer(key: 'count')));

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertSame($failure, $error->getPrevious(), 'The deterministic failure must reach the caller');
        $this->assertSame(1, $writes, 'A deterministic failure must not be retried');
    }

    public function testTransientFailuresAreRetried(): void
    {
        $failure = new RuntimeException('connection reset');
        $writes = 0;
        $database = $this->metadataFailing($failure, $writes);

        $error = $this->attempt(fn (): bool => $database->createAttribute('logs', Attribute::integer(key: 'count')));

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertSame($failure, $error->getPrevious());
        $this->assertSame(3, $writes, 'An unknown failure must still be retried');
    }

    public function testMetadataFailureKeepsThePersistenceErrorFirst(): void
    {
        $failure = new StructureException('metadata rejected');
        $failing = false;
        $adapter = $this->interceptingAdapter(beforeDeleteIndex: function () use (&$failing): void {
            if ($failing) {
                throw new RuntimeException('index cleanup failed');
            }
        });
        $database = $this->interceptingMetadataWrites(function () use (&$failing, $failure): void {
            if ($failing) {
                throw $failure;
            }
        }, $adapter);
        $this->configure($database);
        $database->createCollection(new Collection(id: 'logs', attributes: [Attribute::integer(key: 'count')]));
        $failing = true;

        $error = $this->attempt(fn (): bool => $database->createIndex('logs', Index::key(key: 'by_count', attributes: ['count'])));

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertSame(
            "Failed to persist metadata after retries and cleanup failed for index creation 'by_count': metadata rejected | Cleanup error: index cleanup failed",
            $error->getMessage(),
        );
        $this->assertSame($failure, $error->getPrevious(), 'The persistence error must stay the cause');
    }

    public function testSilentRollbackKeepsThePersistenceError(): void
    {
        $failure = new StructureException('metadata rejected');
        $failing = false;
        $adapter = $this->interceptingAdapter(beforeCreateIndex: function () use (&$failing): void {
            if ($failing) {
                throw new RuntimeException('index restore failed');
            }
        });
        $database = $this->interceptingMetadataWrites(function () use (&$failing, $failure): void {
            if ($failing) {
                throw $failure;
            }
        }, $adapter);
        $this->configure($database);
        $database->createCollection(new Collection(
            id: 'logs',
            attributes: [Attribute::integer(key: 'count')],
            indexes: [Index::key(key: 'by_count', attributes: ['count'])],
        ));
        $failing = true;

        $error = $this->attempt(fn (): bool => $database->deleteIndex('logs', 'by_count'));

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertSame(
            "Failed to persist metadata after retries for index deletion 'by_count': metadata rejected | Cleanup error: index restore failed",
            $error->getMessage(),
        );
        $this->assertSame($failure, $error->getPrevious(), 'A failed silent rollback must not replace the persistence error');
    }

    public function testRollbackWhoseCleanupKeepsFailingRethrows(): void
    {
        $failing = false;
        $deletes = 0;
        $adapter = $this->interceptingAdapter(beforeDeleteIndex: function () use (&$failing, &$deletes): void {
            if ($failing) {
                $deletes++;

                throw new RuntimeException('index cleanup failed');
            }
        });
        $database = $this->interceptingMetadataWrites(function () use (&$failing): void {
            if ($failing) {
                throw new StructureException('metadata rejected');
            }
        }, $adapter);
        $this->configure($database);
        $database->createCollection(new Collection(id: 'logs', attributes: [Attribute::integer(key: 'count')]));
        $failing = true;

        $error = $this->attempt(fn (): bool => $database->createIndex('logs', Index::key(key: 'by_count', attributes: ['count'])));
        $failing = false;

        $this->assertInstanceOf(DatabaseException::class, $error, 'createIndex() must fail when its rollback keeps failing');
        $this->assertStringStartsWith(
            "Failed to persist metadata after retries and cleanup failed for index creation 'by_count'",
            $error->getMessage(),
        );
        $this->assertSame(3, $deletes, 'The index cleanup must be attempted three times');
        $this->assertSame([], $database->getCollection('logs')->indexes, 'The metadata must list no index');
    }

    /**
     * The definition with the new index committed and only the cache invalidation after the commit
     * failed: the index stays, the write is not repeated, and the failure reaches the caller as raised.
     */
    public function testCreateIndexKeepsItsIndexWhenTheInvalidationAfterTheCommitFails(): void
    {
        $failing = false;
        /** @var list<RuntimeException> $failures */
        $failures = [];
        $cache = $this->interceptingCache(function () use (&$failing, &$failures): void {
            if ($failing) {
                $failures[] = $failure = new RuntimeException('cache unavailable');

                throw $failure;
            }
        });
        $armed = false;
        $writes = 0;
        $deletes = 0;
        $adapter = $this->interceptingAdapter(
            beforeDeleteIndex: function () use (&$deletes): void {
                $deletes++;
            },
            afterCommit: function () use (&$armed, &$failing): void {
                $failing = $armed;
            },
        );
        $database = $this->interceptingMetadataWrites(function () use (&$armed, &$writes): void {
            if ($armed) {
                $writes++;
            }
        }, $adapter, new Cache($cache));
        $this->configure($database);
        $database->createCollection(new Collection(id: 'logs', attributes: [Attribute::integer(key: 'count')]));
        $armed = true;

        $error = $this->attempt(fn (): bool => $database->createIndex('logs', Index::key(key: 'by_count', attributes: ['count'])));
        $armed = false;
        $failing = false;

        $this->assertSame(0, $deletes, 'An index whose definition committed must not be rolled back');
        $this->assertSame(1, $writes, 'A write that committed must not be repeated');
        $this->assertSame($failures[0] ?? null, $error, 'The failure after the commit must reach the caller as it was raised');
        $this->assertSame(['by_count'], $this->indexKeys($database, 'logs'));
        $this->assertTrue($this->hasSchemaIndex($database, 'logs', 'by_count'), 'The committed index must still exist');
    }

    /**
     * A failure of the metadata write itself still rolls the index back.
     */
    public function testCreateIndexRollsItsIndexBackWhenTheDefinitionIsNotStored(): void
    {
        $failing = false;
        $deletes = 0;
        $adapter = $this->interceptingAdapter(beforeDeleteIndex: function () use (&$deletes): void {
            $deletes++;
        });
        $database = $this->interceptingMetadataWrites(function () use (&$failing): void {
            if ($failing) {
                throw new StructureException('metadata rejected');
            }
        }, $adapter);
        $this->configure($database);
        $database->createCollection(new Collection(id: 'logs', attributes: [Attribute::integer(key: 'count')]));
        $failing = true;

        $error = $this->attempt(fn (): bool => $database->createIndex('logs', Index::key(key: 'by_count', attributes: ['count'])));
        $failing = false;

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertSame(1, $deletes, 'An index without a stored definition must be dropped');
        $this->assertSame([], $database->getCollection('logs')->indexes);
    }

    /**
     * The cache invalidation fails after every commit while the writes themselves succeed: the
     * relationship keeps its columns and definitions, each of its indexes is still created, and
     * the first failure reaches the caller as raised once they are.
     */
    public function testCreateRelationshipKeepsItsWorkWhenTheInvalidationAfterTheCommitFails(): void
    {
        $failing = false;
        /** @var list<RuntimeException> $failures */
        $failures = [];
        $cache = $this->interceptingCache(function () use (&$failing, &$failures): void {
            if ($failing) {
                $failures[] = $failure = new RuntimeException('cache unavailable');

                throw $failure;
            }
        });
        $armed = false;
        $adapter = $this->interceptingAdapter(
            beforeTransaction: function () use (&$failing): void {
                $failing = false;
            },
            afterCommit: function () use (&$armed, &$failing): void {
                $failing = $armed;
            },
        );
        $database = $this->interceptingMetadataWrites(static function (): void {
        }, $adapter, new Cache($cache));
        $this->configure($database);
        $database->createCollection(new Collection(id: 'profiles'));
        $database->createCollection(new Collection(id: 'accounts'));
        $armed = true;

        $error = $this->attempt(fn (): bool => $database->createRelationship(new Relationship(
            collection: 'profiles',
            relatedCollection: 'accounts',
            type: RelationType::OneToOne,
            twoWay: true,
            key: 'account',
            twoWayKey: 'profile',
        )));
        $armed = false;
        $failing = false;

        $this->assertTrue($this->hasSchemaAttribute($database, 'profiles', 'account'), 'A committed relationship must keep its column');
        $this->assertTrue($this->hasSchemaAttribute($database, 'accounts', 'profile'), 'A committed relationship must keep its column');
        $this->assertSame(['account'], $this->attributeKeys($database, 'profiles'), 'A committed relationship must keep its definition');
        $this->assertSame(['profile'], $this->attributeKeys($database, 'accounts'), 'A committed relationship must keep its definition');
        $this->assertSame(['_index_account'], $this->indexKeys($database, 'profiles'), 'The relationship index must still be created');
        $this->assertSame(['_index_profile'], $this->indexKeys($database, 'accounts'), 'The two-way index must still be created');
        $this->assertTrue($this->hasSchemaIndex($database, 'profiles', '_index_account'));
        $this->assertTrue($this->hasSchemaIndex($database, 'accounts', '_index_profile'));
        $this->assertSame($failures[0] ?? null, $error, 'The failure after the commit must reach the caller as it was raised');
    }

    /**
     * The cache stays unavailable after the relationship's definitions committed, so its index
     * cannot be recorded and the relationship is rolled back; the definitions cannot be removed
     * either, so the columns they describe must stay with them.
     */
    public function testCreateRelationshipKeepsItsColumnsWhenItsDefinitionsCannotBeRemoved(): void
    {
        $failing = false;
        $cache = $this->interceptingCache(function () use (&$failing): void {
            if ($failing) {
                throw new RuntimeException('cache unavailable');
            }
        });
        $armed = false;
        $adapter = $this->interceptingAdapter(afterCommit: function () use (&$armed, &$failing): void {
            if ($armed) {
                $failing = true;
            }
        });
        $database = $this->interceptingMetadataWrites(static function (): void {
        }, $adapter, new Cache($cache));
        $this->configure($database);
        $database->createCollection(new Collection(id: 'profiles'));
        $database->createCollection(new Collection(id: 'accounts'));
        $armed = true;

        $error = $this->attempt(fn (): bool => $database->createRelationship(new Relationship(
            collection: 'profiles',
            relatedCollection: 'accounts',
            type: RelationType::OneToOne,
            twoWay: true,
            key: 'account',
            twoWayKey: 'profile',
        )));
        $armed = false;
        $failing = false;
        $fresh = $this->uncached($adapter, $database);

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertStringStartsWith('Failed to create relationship indexes: ', $error->getMessage());
        $this->assertSame(['account'], $this->attributeKeys($fresh, 'profiles'));
        $this->assertSame(['profile'], $this->attributeKeys($fresh, 'accounts'));
        $this->assertTrue($this->hasSchemaAttribute($fresh, 'profiles', 'account'), 'A column whose definition stays must not be dropped');
        $this->assertTrue($this->hasSchemaAttribute($fresh, 'accounts', 'profile'), 'A column whose definition stays must not be dropped');
    }

    public function testTypeMismatchMessagesSayBigint(): void
    {
        $validator = new AttributeValidator(attributes: []);
        $error = $this->attempt(fn (): bool => $validator->isValid(new Attribute(key: 'total', type: ColumnType::BigInteger, default: 'many')));

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertSame('Default value "many" does not match given type bigint', $error->getMessage());

        $database = $this->interceptingMetadataWrites(static function (): void {
        });
        $this->configure($database);
        $database->createCollection(new Collection(id: 'logs', attributes: [Attribute::bigInteger(key: 'total')]));

        $error = $this->attempt(fn (): Document => $database->updateAttributeDefault('logs', 'total', 'many'));

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertSame('Default value many does not match given type bigint', $error->getMessage());

        $error = $this->attempt(fn (): bool => $validator->isValid(new Attribute(key: 'value', type: ColumnType::Timestamp)));

        $this->assertInstanceOf(DatabaseException::class, $error);
        $this->assertStringContainsString(', bigint, ', $error->getMessage(), 'The listed types must use the stored spelling');
    }

    /**
     * A database with a `logs` collection whose later metadata writes count into $writes and throw $failure.
     */
    private function metadataFailing(Throwable $failure, int &$writes): Database
    {
        $failing = false;
        $database = $this->interceptingMetadataWrites(function () use (&$failing, &$writes, $failure): void {
            if (! $failing) {
                return;
            }

            $writes++;

            throw $failure;
        });
        $this->configure($database);
        $database->createCollection(new Collection(id: 'logs'));
        $failing = true;

        return $database;
    }

    /**
     * A database that runs $intercept before every write of a collection definition.
     *
     * @param  Closure(): void  $intercept
     */
    private function interceptingMetadataWrites(Closure $intercept, ?Adapter $adapter = null, ?Cache $cache = null): Database
    {
        return new class ($adapter ?? $this->adapter(), $cache ?? new Cache(new None()), $intercept) extends Database {
            /**
             * @param  Closure(): void  $intercept
             */
            public function __construct(Adapter $adapter, Cache $cache, private readonly Closure $intercept)
            {
                parent::__construct($adapter, $cache);
            }

            #[\Override]
            public function updateDocument(string $collection, string $id, Document $document): Document
            {
                if ($collection === self::METADATA) {
                    ($this->intercept)();
                }

                return parent::updateDocument($collection, $id, $document);
            }
        };
    }

    /**
     * An adapter that runs the given hooks ahead of each index creation and deletion, ahead of
     * each outermost transaction and after each outermost commit.
     *
     * @param  (Closure(): void)|null  $beforeCreateIndex
     * @param  (Closure(): void)|null  $beforeDeleteIndex
     * @param  (Closure(): void)|null  $beforeTransaction
     * @param  (Closure(): void)|null  $afterCommit
     */
    private function interceptingAdapter(
        ?Closure $beforeCreateIndex = null,
        ?Closure $beforeDeleteIndex = null,
        ?Closure $beforeTransaction = null,
        ?Closure $afterCommit = null,
    ): SQLite {
        return new class (new PDO('sqlite::memory:'), $beforeCreateIndex, $beforeDeleteIndex, $beforeTransaction, $afterCommit) extends SQLite {
            /**
             * @param  (Closure(): void)|null  $beforeCreateIndex
             * @param  (Closure(): void)|null  $beforeDeleteIndex
             * @param  (Closure(): void)|null  $beforeTransaction
             * @param  (Closure(): void)|null  $afterCommit
             */
            public function __construct(
                PDO $pdo,
                private readonly ?Closure $beforeCreateIndex,
                private readonly ?Closure $beforeDeleteIndex,
                private readonly ?Closure $beforeTransaction,
                private readonly ?Closure $afterCommit,
            ) {
                parent::__construct($pdo);
            }

            #[\Override]
            public function startTransaction(): bool
            {
                if (! $this->inTransaction()) {
                    $this->beforeTransaction?->__invoke();
                }

                return parent::startTransaction();
            }

            #[\Override]
            public function createIndex(
                string $collection,
                Index $index,
                array $indexAttributeTypes = [],
                array $collation = [],
                Event $event = Event::IndexCreate,
            ): bool {
                $this->beforeCreateIndex?->__invoke();

                return parent::createIndex($collection, $index, $indexAttributeTypes, $collation, $event);
            }

            #[\Override]
            public function deleteIndex(string $collection, string $id, Event $event = Event::IndexDelete): bool
            {
                $this->beforeDeleteIndex?->__invoke();

                return parent::deleteIndex($collection, $id, $event);
            }

            #[\Override]
            public function commitTransaction(): bool
            {
                $committed = parent::commitTransaction();
                if (! $this->inTransaction()) {
                    $this->afterCommit?->__invoke();
                }

                return $committed;
            }
        };
    }

    /**
     * @return list<string>
     */
    private function attributeKeys(Database $database, string $collection): array
    {
        return \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            \array_values($database->getCollection($collection)->attributes),
        );
    }

    /**
     * @return list<string>
     */
    private function indexKeys(Database $database, string $collection): array
    {
        return \array_map(
            static fn (Index $index): string => $index->key,
            \array_values($database->getCollection($collection)->indexes),
        );
    }

    private function hasSchemaAttribute(Database $database, string $collection, string $key): bool
    {
        foreach ($database->getSchemaAttributes($collection) as $attribute) {
            if ($attribute->getId() === $key) {
                return true;
            }
        }

        return false;
    }

    private function hasSchemaIndex(Database $database, string $collection, string $key): bool
    {
        foreach ($database->getSchemaIndexes($collection) as $index) {
            if (\str_contains($index->getId(), $key)) {
                return true;
            }
        }

        return false;
    }

    /**
     * A cache that runs $beforeWrite ahead of each save and purge.
     *
     * @param  Closure(): void  $beforeWrite
     */
    private function interceptingCache(Closure $beforeWrite): MemoryCache
    {
        return new class ($beforeWrite) extends MemoryCache {
            /**
             * @param  Closure(): void  $beforeWrite
             */
            public function __construct(private readonly Closure $beforeWrite)
            {
            }

            #[\Override]
            public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                ($this->beforeWrite)();

                return parent::save($key, $data, $hash);
            }

            #[\Override]
            public function purge(string $key, string $hash = ''): bool
            {
                ($this->beforeWrite)();

                return parent::purge($key, $hash);
            }
        };
    }

    /**
     * A second database over the same adapter and namespace that reads definitions past the cache.
     */
    private function uncached(Adapter $adapter, Database $database): Database
    {
        return (new Database($adapter, new Cache(new None())))
            ->setDatabase($database->getDatabase())
            ->setNamespace($database->getNamespace());
    }

    private function adapter(): Adapter
    {
        return new SQLite(new PDO('sqlite::memory:'));
    }

    private function configure(Database $database): void
    {
        $database
            ->setDatabase('core_minors')
            ->setNamespace('core_minors_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
    }

    /**
     * @param  callable(): mixed  $operation
     */
    private function attempt(callable $operation): ?Throwable
    {
        try {
            $operation();
        } catch (Throwable $error) {
            return $error;
        }

        return null;
    }
}
