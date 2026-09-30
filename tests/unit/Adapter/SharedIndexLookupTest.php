<?php

namespace Tests\Unit\Adapter;

use Closure;
use PDO;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Index;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Mongo\Client;
use Utopia\Query\Schema\IndexType;

/**
 * Engines that keep one index per table and key under shared tables: PostgreSQL names it
 * without the tenant, and each adapter finds another tenant's definition of a key in the
 * metadata it keeps across tenants.
 */
final class SharedIndexLookupTest extends TestCase
{
    private const string INDEXES = '[{"$id":"byAge","key":"byAge","type":"key","attributes":["age"],"lengths":[],"orders":[],"ttl":1},{"$id":"by_email","key":"by_email","type":"unique","attributes":["email"],"lengths":[],"orders":[],"ttl":1}]';

    /** @var list<string> */
    private array $statements = [];

    /** @var array<string, mixed> */
    private array $bindings = [];

    public function testPostgresNamesASharedIndexWithoutTheTenant(): void
    {
        $adapter = $this->createPostgres(sharedTables: true);

        $adapter->createIndex('users', Index::unique(key: 'byEmail', attributes: ['email']));
        $adapter->deleteIndex('users', 'byEmail');

        $this->assertSame([
            'CREATE UNIQUE INDEX IF NOT EXISTS "namespace__users_byEmail" ON "database"."namespace_users" ("_tenant", "email")',
            'DROP INDEX IF EXISTS "database"."namespace__users_byEmail"; DROP INDEX IF EXISTS "database"."namespace_2_users_byEmail"',
        ], $this->statements);
    }

    public function testPostgresKeepsItsNamesOutsideSharedTables(): void
    {
        $adapter = $this->createPostgres(sharedTables: false);

        $adapter->createIndex('users', Index::unique(key: 'byEmail', attributes: ['email']));
        $adapter->deleteIndex('users', 'byEmail');
        $adapter->renameIndex('users', 'byEmail', 'emailIndex');

        $this->assertSame([
            'CREATE UNIQUE INDEX "namespace_2_users_byEmail" ON "database"."namespace_users" ("email")',
            'DROP INDEX IF EXISTS "database"."namespace_2_users_byEmail"',
            'ALTER INDEX IF EXISTS "database"."namespace_2_users_byEmail" RENAME TO "namespace_2_users_emailIndex"',
        ], $this->statements);
    }

    public function testPostgresFindsAnotherTenantsIndexByKey(): void
    {
        $adapter = $this->createPostgres(sharedTables: true, row: self::INDEXES);

        $index = $adapter->findSharedIndex('users', 'BY_EMAIL');

        $this->assertSame(
            ['SELECT "indexes" FROM "database"."namespace__metadata" WHERE "_uid" = :collection AND ("_tenant" IS NULL OR "_tenant" <> :tenant) AND LOWER("indexes") LIKE :pattern ESCAPE \'!\' LIMIT 1'],
            $this->statements,
        );
        $this->assertSame([':collection' => 'users', ':pattern' => '%"key":"by!_email"%', ':tenant' => 2], $this->bindings);
        $this->assertNotNull($index);
        $this->assertSame('by_email', $index->key);
        $this->assertSame(IndexType::Unique, $index->type);
        $this->assertSame(['email'], $index->attributes);
    }

    public function testNoOtherTenantListingTheKeyFindsNothing(): void
    {
        $this->assertNull($this->createPostgres(sharedTables: true, row: false)->findSharedIndex('users', 'byAge'));
        $this->assertNull($this->createPostgres(sharedTables: true, row: '[]')->findSharedIndex('users', 'byAge'));

        $this->statements = [];
        $this->assertNull($this->createPostgres(sharedTables: false, row: self::INDEXES)->findSharedIndex('users', 'byAge'));
        $this->assertSame([], $this->statements, 'Outside shared tables no index is shared');
    }

    public function testMariaDBWithoutATenantLooksAtEveryTenant(): void
    {
        $adapter = $this->createMariaDB(self::INDEXES);
        $adapter->setTenant(null);

        $this->assertSame('byAge', $adapter->findSharedIndex('users', 'byAge')?->key);
        $this->assertStringContainsString('WHERE `_uid` = :collection AND `_tenant` IS NOT NULL AND LOWER(`indexes`) LIKE :pattern', $this->statements[0]);
        $this->assertArrayNotHasKey(':tenant', $this->bindings);
    }

    public function testMongoFindsAnotherTenantsIndexByKey(): void
    {
        $filters = [];
        $adapter = $this->createMongo(function (array $filter) use (&$filters): void {
            $filters = $filter;
        });

        $index = $adapter->findSharedIndex('users', 'by_email');

        $this->assertSame([
            Storage::UID => 'users',
            Storage::TENANT => ['$ne' => 2],
            'indexes' => ['$regex' => '"key"\:"by_email"', '$options' => 'i'],
        ], $filters);
        $this->assertSame('by_email', $index?->key);
        $this->assertSame(IndexType::Unique, $index->type);
    }

    private function createPostgres(bool $sharedTables, string|false $row = false): Postgres
    {
        $adapter = new Postgres($this->createPDO($row));
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setSharedTables($sharedTables);
        $adapter->setTenant(2);

        return $adapter;
    }

    private function createMariaDB(string|false $row): MariaDB
    {
        $adapter = new MariaDB($this->createPDO($row));
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $adapter->setSharedTables(true);
        $adapter->setTenant(2);

        return $adapter;
    }

    private function createPDO(string|false $row): PDO
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query) use ($row): PDOStatement {
            $this->statements[] = \trim($query);

            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);
            $statement->method('bindValue')->willReturnCallback(function (string|int $parameter, mixed $value): bool {
                $this->bindings[(string) $parameter] = $value;

                return true;
            });
            $statement->method('fetchColumn')->willReturn($row);

            return $statement;
        });

        return $pdo;
    }

    /**
     * @param  Closure(array<mixed>): void  $record
     */
    private function createMongo(Closure $record): Mongo
    {
        $client = new class ($record) extends Client {
            /**
             * @param  Closure(array<mixed>): void  $record
             */
            public function __construct(private readonly Closure $record)
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
             * @param  array<mixed>  $filters
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function find(string $collection, array $filters = [], array $options = []): stdClass
            {
                ($this->record)($filters);

                return (object) ['cursor' => (object) ['firstBatch' => [(object) ['indexes' => SharedIndexLookupTest::indexes()]], 'id' => 0]];
            }
        };

        $adapter = new Mongo($client);
        $adapter->setAuthorization(new Authorization());
        $adapter->setNamespace('scope');
        $adapter->setSharedTables(true);
        $adapter->setTenant(2);

        return $adapter;
    }

    public static function indexes(): string
    {
        return self::INDEXES;
    }
}
