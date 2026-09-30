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
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
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
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
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
        /** @var bool $failing */
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
        /** @var bool $failing */
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
        /** @var bool $failing */
        $failing = false;
        /** @var int $deletes */
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
        /** @var bool $failing */
        $failing = false;
        /** @var list<RuntimeException> $failures */
        $failures = [];
        $cache = $this->interceptingCache(function () use (&$failing, &$failures): void {
            if ($failing) {
                $failures[] = $failure = new RuntimeException('cache unavailable');

                throw $failure;
            }
        });
        /** @var bool $armed */
        $armed = false;
        /** @var int $writes */
        $writes = 0;
        /** @var int $deletes */
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
        /** @var bool $failing */
        $failing = false;
        /** @var int $deletes */
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
        /** @var bool $failing */
        $failing = false;
        /** @var list<RuntimeException> $failures */
        $failures = [];
        $cache = $this->interceptingCache(function () use (&$failing, &$failures): void {
            if ($failing) {
                $failures[] = $failure = new RuntimeException('cache unavailable');

                throw $failure;
            }
        });
        /** @var bool $armed */
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
        /** @var bool $failing */
        $failing = false;
        $cache = $this->interceptingCache(function () use (&$failing): void {
            if ($failing) {
                throw new RuntimeException('cache unavailable');
            }
        });
        /** @var bool $armed */
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
     * @return array<string, array{Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'memory' => [static fn (): Adapter => new Memory()],
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
        ];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testEveryUniqueViolationUsesOneMessage(Closure $adapter): void
    {
        $database = $this->interceptingMetadataWrites(static function (): void {
        }, $adapter());
        $this->configure($database);
        $database->createCollection(new Collection(
            id: 'users',
            attributes: [Attribute::string(key: 'email', size: 64)],
            indexes: [Index::unique(key: 'by_email', attributes: ['email'])],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
        $database->createDocument('users', new Document([Document::ID => 'first', 'email' => 'a@example.com']));
        $database->createDocument('users', new Document([Document::ID => 'second', 'email' => 'b@example.com']));

        $violations = [
            'create' => fn (): Document => $database->createDocument('users', new Document([Document::ID => 'third', 'email' => 'a@example.com'])),
            'create many' => fn (): int => $database->createDocuments('users', [
                new Document([Document::ID => 'fourth', 'email' => 'c@example.com']),
                new Document([Document::ID => 'fifth', 'email' => 'c@example.com']),
            ]),
            'update' => fn (): Document => $database->updateDocument('users', 'second', new Document(['email' => 'a@example.com'])),
            'update many into one value' => fn (): int => $database->updateDocuments('users', new Document(['email' => 'd@example.com'])),
            'update many into a stored value' => fn (): int => $database->updateDocuments(
                'users',
                new Document(['email' => 'a@example.com']),
                [Query::equal(Document::ID, ['second'])],
            ),
        ];

        foreach ($violations as $name => $violation) {
            $error = $this->attempt($violation);

            $this->assertInstanceOf(UniqueException::class, $error, $name);
            $this->assertSame('Document with the requested unique attributes already exists', $error->getMessage(), $name);
        }
    }

    /**
     * @return array<string, array{ColumnType, array<mixed>, string}>
     */
    public static function invalidSpatialDefaults(): array
    {
        return [
            'point with one coordinate' => [ColumnType::Point, [1.0], 'Point must be an array of two numeric values [x, y]'],
            'point out of range' => [ColumnType::Point, [200.0, 0.0], 'Longitude'],
            'linestring with one point' => [ColumnType::Linestring, [[0.0, 0.0]], 'LineString must contain at least two points'],
            'polygon with an open ring' => [ColumnType::Polygon, [[[0.0, 0.0], [1.0, 1.0]]], 'must contain at least 4 points'],
        ];
    }

    /**
     * @param  array<mixed>  $default
     */
    #[DataProvider('invalidSpatialDefaults')]
    public function testSpatialDefaultsAreValidated(ColumnType $type, array $default, string $reason): void
    {
        $validator = new AttributeValidator(attributes: [], supportForSpatialAttributes: true);
        $created = $this->attempt(fn (): bool => $validator->isValid(new Attribute(key: 'shape', type: $type, default: $default)));

        $this->assertInstanceOf(DatabaseException::class, $created, 'A create must reject the default');
        $this->assertStringContainsString($reason, $created->getMessage());

        $database = new class ($this->adapter(), new Cache(new None())) extends Database {
            public function checkDefault(ColumnType $type, mixed $default): void
            {
                $this->validateDefaultTypes($type->value, $default);
            }
        };
        $updated = $this->attempt(function () use ($database, $type, $default): void {
            $database->checkDefault($type, $default);
        });

        $this->assertInstanceOf(DatabaseException::class, $updated, 'An update must reject the default');
        $this->assertStringContainsString($reason, $updated->getMessage());
    }

    public function testValidSpatialDefaultsAreAccepted(): void
    {
        $defaults = [
            [ColumnType::Point, [1.0, 2.0]],
            [ColumnType::Linestring, [[0.0, 0.0], [1.0, 1.0]]],
            [ColumnType::Polygon, [[[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [0.0, 0.0]]]],
        ];
        $validator = new AttributeValidator(attributes: [], supportForSpatialAttributes: true);
        $database = new class ($this->adapter(), new Cache(new None())) extends Database {
            public function checkDefault(ColumnType $type, mixed $default): void
            {
                $this->validateDefaultTypes($type->value, $default);
            }
        };

        foreach ($defaults as [$type, $default]) {
            $this->assertTrue($validator->isValid(new Attribute(key: 'shape', type: $type, default: $default)), $type->value);
            $database->checkDefault($type, $default);
        }
    }

    public function testStoredObjectValueDoesNotBlockAnUpdateOfAnotherAttribute(): void
    {
        $database = $this->interceptingMetadataWrites(static function (): void {
        }, new Memory());
        $this->configure($database);
        $database->createCollection(new Collection(
            id: 'items',
            attributes: [Attribute::string(key: 'title', size: 64), Attribute::object(key: 'meta')],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
        $database->skipValidation(fn (): Document => $database->createDocument('items', new Document([
            Document::ID => 'stored',
            'title' => 'first',
            'meta' => [1, 2],
        ])));

        $renamed = $database->updateDocument('items', 'stored', new Document(['title' => 'renamed']));

        $this->assertSame('renamed', $renamed->getAttribute('title'));
        $this->assertSame([1, 2], $renamed->getAttribute('meta'));

        $returned = $database->updateDocument('items', 'stored', $database->getDocument('items', 'stored')->setAttribute('title', 'again'));

        $this->assertSame('again', $returned->getAttribute('title'), 'A stored value passed back unchanged must not block the update');

        $error = $this->attempt(fn (): Document => $database->updateDocument('items', 'stored', new Document(['meta' => [3, 4]])));

        $this->assertInstanceOf(StructureException::class, $error, 'A list written as an object must still be rejected');
        $this->assertSame([1, 2], $database->getDocument('items', 'stored')->getAttribute('meta'));
    }

    public function testAnAssociativeVectorIsRejectedNamingItsAttribute(): void
    {
        $adapter = new class () extends Memory {
            #[\Override]
            public function capabilities(): array
            {
                return [...parent::capabilities(), Capability::Vectors];
            }
        };
        $database = $this->interceptingMetadataWrites(static function (): void {
        }, $adapter);
        $this->configure($database);
        $database->createCollection(new Collection(
            id: 'embeddings',
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $database->createAttribute('embeddings', Attribute::vector(key: 'embedding', size: 3));
        $filters = [];
        foreach ($database->getCollection('embeddings')->attributes as $attribute) {
            if ($attribute->key === 'embedding') {
                $filters = $attribute->filters;
            }
        }
        $this->assertContains(ColumnType::Vector->value, $filters, 'createAttribute() must add the vector filter');

        $error = $this->attempt(fn (): Document => $database->createDocument('embeddings', new Document([
            'embedding' => ['x' => 1.0, 'y' => 0.0, 'z' => 0.0],
        ])));

        $this->assertInstanceOf(StructureException::class, $error);
        $this->assertSame(
            'Invalid document structure: Attribute "embedding" has invalid type. Value must be an array of 3 numeric values',
            $error->getMessage(),
        );
    }

    /**
     * A database with a `logs` collection whose later metadata writes count into $writes and throw $failure.
     */
    private function metadataFailing(Throwable $failure, int &$writes): Database
    {
        /** @var bool $failing */
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
