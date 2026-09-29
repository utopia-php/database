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
        $failure = new RuntimeException('cache unavailable');
        $cache = $this->failingCache($failure);
        $armed = false;
        $writes = 0;
        $deletes = 0;
        $adapter = $this->interceptingAdapter(
            beforeDeleteIndex: function () use (&$deletes): void {
                $deletes++;
            },
            afterCommit: function () use (&$armed, $cache): void {
                if ($armed) {
                    $cache->failing = true;
                }
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
        $cache->failing = false;

        $this->assertSame(0, $deletes, 'An index whose definition committed must not be rolled back');
        $this->assertSame(1, $writes, 'A write that committed must not be repeated');
        $this->assertSame($failure, $error, 'The failure after the commit must reach the caller as it was raised');
        $this->assertSame(['by_count'], \array_map(
            static fn (Index $index): string => $index->key,
            \array_values($database->getCollection('logs')->indexes),
        ));
        $this->assertCount(1, \array_filter(
            $database->getSchemaIndexes('logs'),
            static fn (Document $index): bool => \str_contains($index->getId(), 'by_count'),
        ), 'The committed index must still exist');
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
     * An adapter that runs the given hooks ahead of each index creation and deletion, and after
     * each outermost commit.
     *
     * @param  Closure(): void|null  $beforeCreateIndex
     * @param  Closure(): void|null  $beforeDeleteIndex
     * @param  Closure(): void|null  $afterCommit
     */
    private function interceptingAdapter(
        ?Closure $beforeCreateIndex = null,
        ?Closure $beforeDeleteIndex = null,
        ?Closure $afterCommit = null,
    ): SQLite {
        return new class (new PDO('sqlite::memory:'), $beforeCreateIndex, $beforeDeleteIndex, $afterCommit) extends SQLite {
            /**
             * @param  Closure(): void|null  $beforeCreateIndex
             * @param  Closure(): void|null  $beforeDeleteIndex
             * @param  Closure(): void|null  $afterCommit
             */
            public function __construct(
                PDO $pdo,
                private readonly ?Closure $beforeCreateIndex,
                private readonly ?Closure $beforeDeleteIndex,
                private readonly ?Closure $afterCommit,
            ) {
                parent::__construct($pdo);
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
     * @return MemoryCache&object{failing: bool}
     */
    private function failingCache(Throwable $failure): MemoryCache
    {
        return new class ($failure) extends MemoryCache {
            public bool $failing = false;

            public function __construct(private readonly Throwable $failure)
            {
            }

            #[\Override]
            public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                if ($this->failing) {
                    throw $this->failure;
                }

                return parent::save($key, $data, $hash);
            }

            #[\Override]
            public function purge(string $key, string $hash = ''): bool
            {
                if ($this->failing) {
                    throw $this->failure;
                }

                return parent::purge($key, $hash);
            }
        };
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
