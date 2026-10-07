<?php

namespace Tests\Unit;

use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Redis;
use Throwable;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\Redis as RedisAdapter;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Unique as UniqueException;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;
use Utopia\Mongo\Exception as MongoException;
use Utopia\Query\Schema\IndexType;

final class UniqueViolationTest extends TestCase
{
    private const string COLLECTION = 'movies';

    private const string INDEX = 'slug_unique';

    private const string TAKEN_SLUG = 'prefix_uid_suffix';

    public function testMySQLDocumentIdConflictIsDuplicate(): void
    {
        $this->assertDuplicate($this->createFailure(MySQL::class, $this->mysqlException(
            "SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry 'movie-1' for key 'movies._uid'"
        )));
    }

    public function testMySQLPrimaryKeyConflictIsDuplicate(): void
    {
        $this->assertDuplicate($this->createFailure(MySQL::class, $this->mysqlException(
            "SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry '5' for key 'PRIMARY'"
        )));
    }

    public function testMySQLUniqueIndexConflictWithUidInValueIsUnique(): void
    {
        $this->assertUnique($this->createFailure(MySQL::class, $this->mysqlException(
            "SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry 'prefix_uid_suffix' for key 'slug'"
        )));
    }

    public function testMySQLUniqueIndexConflictWithUidInIndexNameIsUnique(): void
    {
        $this->assertUnique($this->createFailure(MySQL::class, $this->mysqlException(
            "SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry 'a' for key 'movies.slug_uid_index'"
        )));
    }

    public function testMySQLUnparsableMessageIsDuplicate(): void
    {
        $this->assertDuplicate($this->createFailure(MySQL::class, $this->mysqlException(
            'SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry'
        )));
    }

    public function testPostgresDocumentIdConflictIsDuplicate(): void
    {
        $this->assertDuplicate($this->createFailure(Postgres::class, $this->postgresException(
            'SQLSTATE[23505]: Unique violation: 7 ERROR:  duplicate key value violates unique constraint "ns_1_movies_uid"'
            . "\nDETAIL:  Key (_uid, _tenant)=(movie-1, 1) already exists."
        )));
    }

    public function testPostgresUniqueIndexConflictWithUidInValueIsUnique(): void
    {
        $this->assertUnique($this->createFailure(Postgres::class, $this->postgresException(
            'SQLSTATE[23505]: Unique violation: 7 ERROR:  duplicate key value violates unique constraint "ns_1_movies_slug"'
            . "\nDETAIL:  Key (slug)=(prefix_uid_suffix) already exists."
        )));
    }

    public function testPostgresCompositeIndexOnDocumentIdIsUnique(): void
    {
        $this->assertUnique($this->createFailure(Postgres::class, $this->postgresException(
            'SQLSTATE[23505]: Unique violation: 7 ERROR:  duplicate key value violates unique constraint "ns_1_movies_pair"'
            . "\nDETAIL:  Key (_uid, email)=(movie-1, a@b.co) already exists."
        )));
    }

    public function testPostgresMissingDetailIsDuplicate(): void
    {
        $this->assertDuplicate($this->createFailure(Postgres::class, $this->postgresException(
            'SQLSTATE[23505]: Unique violation: 7 ERROR:  duplicate key value violates unique constraint "ns_1_movies_uid"'
        )));
    }

    public function testSQLiteDocumentIdConflictIsDuplicate(): void
    {
        $this->assertDuplicate($this->createFailure(SQLite::class, $this->sqliteException(
            'SQLSTATE[23000]: Integrity constraint violation: 19 UNIQUE constraint failed: ns_movies._tenant, ns_movies._uid'
        )));
    }

    public function testSQLiteCompositeIndexOnDocumentIdIsUnique(): void
    {
        $this->assertUnique($this->createFailure(SQLite::class, $this->sqliteException(
            'SQLSTATE[23000]: Integrity constraint violation: 19 UNIQUE constraint failed: ns_movies._uid, ns_movies.email'
        )));
    }

    public function testSQLiteUniqueIndexConflictIsUnique(): void
    {
        $this->assertUnique($this->createFailure(SQLite::class, $this->sqliteException(
            'SQLSTATE[23000]: Integrity constraint violation: 19 UNIQUE constraint failed: ns_movies.slug'
        )));
    }

    public function testMongoDocumentIdConflictIsDuplicate(): void
    {
        $this->assertDuplicate($this->mongoCreateFailure(new MongoException(
            'E11000 duplicate key error collection: db.ns_movies index: _uid dup key: { _uid: "movie-1" }',
            11000
        )));
    }

    public function testMongoUniqueIndexConflictWithUidInValueIsUnique(): void
    {
        $this->assertUnique($this->mongoCreateFailure(new MongoException(
            'E11000 duplicate key error collection: db.ns_movies index: slug dup key: { slug: "prefix_uid_suffix" }',
            11000
        )));
    }

    public function testMongoUnparsableMessageIsDuplicate(): void
    {
        $this->assertDuplicate($this->mongoCreateFailure(new MongoException('E11000 duplicate key error', 11000)));
    }

    public function testMemoryDocumentIdConflictIsDuplicate(): void
    {
        $database = $this->memory();

        $this->assertDuplicate($this->thrown(
            fn () => $database->createDocument(self::COLLECTION, $this->movie('movie-1', 'sequel'))
        ));
    }

    public function testMemoryUniqueIndexConflictWithUidInValueIsUnique(): void
    {
        $database = $this->memory();

        $this->assertUnique($this->thrown(
            fn () => $database->createDocument(self::COLLECTION, $this->movie('movie-3', self::TAKEN_SLUG))
        ));
    }

    public function testMemoryUniqueIndexConflictOnUpdateIsUnique(): void
    {
        $database = $this->memory();

        $this->assertUnique($this->thrown(
            fn () => $database->updateDocument(self::COLLECTION, 'movie-2', new Document(['slug' => self::TAKEN_SLUG]))
        ));
    }

    public function testMemoryBatchUpdateConflictWithStoredRowIsUnique(): void
    {
        $database = $this->memory();

        $this->assertUnique($this->thrown(fn () => $database->updateDocuments(
            self::COLLECTION,
            new Document(['slug' => self::TAKEN_SLUG]),
            [Query::equal('$id', ['movie-2'])],
        )));
    }

    public function testMemoryBatchUpdateConflictBetweenUpdatedRowsIsUnique(): void
    {
        $database = $this->memory();

        $this->assertUnique($this->thrown(
            fn () => $database->updateDocuments(self::COLLECTION, new Document(['slug' => 'sequel']))
        ));
    }

    public function testMemoryUniqueHashCollisionIsUnique(): void
    {
        $memory = new class () extends Memory {
            public function collide(string $collection, string $index): void
            {
                $this->uniqueIndexHashes[$collection][$index]['signature'] = 'movie-1';
                $this->probeUniqueHash($collection, $index, 'signature', null, 'movie-2');
            }
        };

        $this->assertUnique($this->thrown(fn () => $memory->collide(self::COLLECTION, self::INDEX)));
    }

    public function testRedisDocumentIdConflictIsDuplicate(): void
    {
        $redis = $this->redis(documentExists: true);

        $this->assertDuplicate($this->thrown(
            fn () => $redis->createDocument($this->collection(), $this->movie('movie-1', 'sequel'))
        ));
    }

    public function testRedisUniqueIndexConflictWithUidInValueIsUnique(): void
    {
        $redis = $this->redis();

        $this->assertUnique($this->thrown(
            fn () => $redis->createDocument($this->collection(), $this->movie('movie-3', self::TAKEN_SLUG))
        ));
    }

    public function testRedisUniqueIndexConflictOnUpdateIsUnique(): void
    {
        $redis = $this->redis();

        $this->assertUnique($this->thrown(
            fn () => $redis->updateDocument($this->collection(), 'movie-2', $this->movie('movie-2', self::TAKEN_SLUG), false)
        ));
    }

    public function testRedisUniqueIndexConflictIsSkippedWhenSkippingDuplicates(): void
    {
        $redis = $this->redis();
        $movie = $this->movie('movie-3', self::TAKEN_SLUG);

        $this->assertSame($movie, $redis->ignoreDuplicates(fn () => $redis->createDocument($this->collection(), $movie)));
    }

    private function mysqlException(string $message): PDOException
    {
        $exception = new class ($message, '23000') extends PDOException {
            public function __construct(string $message, string $state)
            {
                parent::__construct($message);
                $this->code = $state;
            }
        };
        $exception->errorInfo = ['23000', 1062, $message];

        return $exception;
    }

    private function postgresException(string $message): PDOException
    {
        $exception = new class ($message, '23505') extends PDOException {
            public function __construct(string $message, string $state)
            {
                parent::__construct($message);
                $this->code = $state;
            }
        };
        $exception->errorInfo = ['23505', 7, $message];

        return $exception;
    }

    private function sqliteException(string $message): PDOException
    {
        $exception = new class ($message, 'HY000') extends PDOException {
            public function __construct(string $message, string $state)
            {
                parent::__construct($message);
                $this->code = $state;
            }
        };
        $exception->errorInfo = ['HY000', 19, $message];

        return $exception;
    }

    private function memory(): Database
    {
        $database = new Database(new Memory(), new Cache(new NoCache()));
        $database
            ->setDatabase('unique_violation')
            ->setNamespace('unique_violation')
            ->setAuthorization(new Authorization());
        $database->create();

        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'slug', size: 128)],
            indexes: [Index::unique(key: self::INDEX, attributes: ['slug'], lengths: [128])],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ));

        $database->createDocuments(self::COLLECTION, [
            $this->movie('movie-1', self::TAKEN_SLUG),
            $this->movie('movie-2', 'frozen'),
        ]);

        return $database;
    }

    private function redis(bool $documentExists = false): RedisAdapter
    {
        $payloads = [
            'movie-1' => \json_encode($this->movie('movie-1', self::TAKEN_SLUG)->getArrayCopy(), JSON_THROW_ON_ERROR),
            'movie-2' => \json_encode($this->movie('movie-2', 'frozen')->getArrayCopy(), JSON_THROW_ON_ERROR),
        ];
        $indexes = \json_encode([
            ['$id' => self::INDEX, 'type' => IndexType::Unique->value, 'attributes' => ['slug']],
        ], JSON_THROW_ON_ERROR);
        $read = fn (mixed $key): string|false => \is_string($key) ? ($payloads[self::documentId($key)] ?? false) : false;

        $client = self::createStub(Redis::class);
        $client->method('exists')->willReturn($documentExists ? 1 : 0);
        $client->method('sMembers')->willReturn(\array_keys($payloads));
        $client->method('get')->willReturnCallback($read);
        $client->method('mGet')->willReturnCallback(fn (array $keys): array => \array_map($read, $keys));
        $client->method('hGet')->willReturnCallback(
            fn (string $key, string $field): string|false => $field === 'indexes' ? $indexes : false
        );

        $adapter = new RedisAdapter($client);
        $adapter->setNamespace('unique_violation');
        $adapter->setDatabase('unique_violation');

        return $adapter;
    }

    private static function documentId(string $key): string
    {
        $separator = \strrpos($key, RedisAdapter::SEP);

        return $separator === false ? $key : \substr($key, $separator + 1);
    }

    private function collection(): Document
    {
        return new Document(['$id' => self::COLLECTION]);
    }

    private function movie(string $id, string $slug): Document
    {
        return new Document([
            '$id' => $id,
            '$permissions' => [Permission::read(Role::any())],
            'slug' => $slug,
        ]);
    }

    /**
     * @param  class-string<SQL>  $adapter
     */
    private function createFailure(string $adapter, PDOException $exception): Throwable
    {
        $statement = self::createStub(PDOStatement::class);
        $statement->method('execute')->willThrowException($exception);
        $pdo = self::createStub(PDO::class);
        $pdo->method('prepare')->willReturn($statement);

        $sql = new $adapter($pdo);
        $sql->setDatabase('unique_violation');
        $sql->setNamespace('unique_violation');

        return $this->thrown(fn () => $sql->createDocument($this->collection(), $this->movie('movie-1', self::TAKEN_SLUG)));
    }

    private function mongoCreateFailure(MongoException $exception): Throwable
    {
        $client = new class ($exception) extends Client {
            public function __construct(private readonly MongoException $failure)
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }

            /**
             * @param  array<mixed>  $document
             * @param  array<mixed>  $options
             * @return array<mixed>
             */
            #[\Override]
            public function insert(string $collection, array $document, array $options = []): array
            {
                throw $this->failure;
            }
        };

        $mongo = new Mongo($client);
        $mongo->setNamespace('unique_violation');

        return $this->thrown(fn () => $mongo->createDocument($this->collection(), $this->movie('movie-1', self::TAKEN_SLUG)));
    }

    private function thrown(callable $action): Throwable
    {
        try {
            $action();
        } catch (Throwable $exception) {
            return $exception;
        }

        $this->fail('Expected a duplicate or unique violation');
    }

    private function assertDuplicate(Throwable $exception): void
    {
        $this->assertInstanceOf(DuplicateException::class, $exception, $exception->getMessage());
        $this->assertNotInstanceOf(UniqueException::class, $exception);
        $this->assertSame('Document already exists', $exception->getMessage());
    }

    private function assertUnique(Throwable $exception): void
    {
        $this->assertInstanceOf(UniqueException::class, $exception, $exception->getMessage());
        $this->assertSame('Document with the requested unique attributes already exists', $exception->getMessage());
    }
}
