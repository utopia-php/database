<?php

namespace Tests\Unit;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;
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
        $adapter = $this->interceptingIndexes(
            static function (): void {
            },
            function () use (&$failing): void {
                if ($failing) {
                    throw new RuntimeException('index cleanup failed');
                }
            },
        );
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
        $adapter = $this->interceptingIndexes(
            function () use (&$failing): void {
                if ($failing) {
                    throw new RuntimeException('index restore failed');
                }
            },
            static function (): void {
            },
        );
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
    private function interceptingMetadataWrites(Closure $intercept, ?Adapter $adapter = null): Database
    {
        return new class ($adapter ?? $this->adapter(), new Cache(new None()), $intercept) extends Database {
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
     * An adapter that runs $beforeCreate and $beforeDelete ahead of each index creation and deletion.
     *
     * @param  Closure(): void  $beforeCreate
     * @param  Closure(): void  $beforeDelete
     */
    private function interceptingIndexes(Closure $beforeCreate, Closure $beforeDelete): SQLite
    {
        return new class (new PDO('sqlite::memory:'), $beforeCreate, $beforeDelete) extends SQLite {
            /**
             * @param  Closure(): void  $beforeCreate
             * @param  Closure(): void  $beforeDelete
             */
            public function __construct(PDO $pdo, private readonly Closure $beforeCreate, private readonly Closure $beforeDelete)
            {
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
                ($this->beforeCreate)();

                return parent::createIndex($collection, $index, $indexAttributeTypes, $collation, $event);
            }

            #[\Override]
            public function deleteIndex(string $collection, string $id, Event $event = Event::IndexDelete): bool
            {
                ($this->beforeDelete)();

                return parent::deleteIndex($collection, $id, $event);
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
