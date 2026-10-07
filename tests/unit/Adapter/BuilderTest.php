<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;
use Utopia\Query\Schema\MySQL as MySQLSchema;
use Utopia\Query\Schema\PostgreSQL as PostgresSchema;

final class BuilderTest extends TestCase
{
    private const string COLLECTION = 'posts';

    /**
     * @return iterable<string, array{SQL, class-string}>
     */
    public static function dialects(): iterable
    {
        yield 'MariaDB' => [new MariaDB(new stdClass()), MySQLSchema::class];
        yield 'Postgres' => [new Postgres(new stdClass()), PostgresSchema::class];
        yield 'SQLite' => [new SQLite(new PDO('sqlite::memory:')), MySQLSchema::class];
    }

    /**
     * @param  class-string  $schema
     */
    #[DataProvider('dialects')]
    public function testEachSqlAdapterHandsOutABuilderAndASchemaInItsDialect(SQL $adapter, string $schema): void
    {
        $adapter->setDatabase('builder');
        $adapter->setNamespace('dialect');

        $this->assertTrue($adapter->hasFeature(Feature\QueryBuilder::class));
        $this->assertInstanceOf($schema, $adapter->schema());
    }

    public function testTheDatabaseRunsABuilderOverACollection(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'first', 'title' => 'hello']));

        $rows = $database->getAuthorization()->skip(
            fn (): array|int => $database->from(self::COLLECTION)->select(['$id', 'title'])->execute(),
        );

        $this->assertIsArray($rows);
        $this->assertCount(1, $rows);
        $this->assertInstanceOf(Document::class, $rows[0]);
        $this->assertSame('hello', $rows[0]->getAttribute('title'));
    }

    public function testTheDatabaseHandsOutASchemaBuilder(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));

        $this->assertInstanceOf(MySQLSchema::class, $database->schema());
    }

    public function testAPoolHandsOutTheBuilderOfItsConnection(): void
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $database = $this->database($adapter);
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'first', 'title' => 'pooled']));
        $pool = $this->pool($adapter);
        $pool->setDatabase($database->getDatabase());
        $pool->setNamespace($database->getNamespace());

        $statement = $pool->builder(self::COLLECTION)->select(['title'])->build();
        $rows = $pool->rawQuery($statement->query, $statement->bindings);

        $this->assertTrue($pool->hasFeature(Feature\QueryBuilder::class));
        $this->assertInstanceOf(MySQLSchema::class, $pool->schema());
        $this->assertSame(['pooled'], \array_map(static fn (Document $row): mixed => $row->getAttribute('title'), $rows));
    }

    public function testAnAdapterWithoutABuilderIsRefused(): void
    {
        $database = $this->database(new Memory());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Query builder is not supported by this adapter');

        $database->getAuthorization()->skip(fn (): mixed => $database->from(self::COLLECTION));
    }

    public function testAPoolOverAnAdapterWithoutABuilderRefusesIt(): void
    {
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support query builder');

        $this->pool(new Memory())->schema();
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('builder')
            ->setNamespace('builder_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));

        return $database;
    }

    private function pool(Adapter $adapter): Pool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(static fn (callable $callback): mixed => $callback($adapter));

        $pool = new Pool($connections);
        $pool->setAuthorization(new Authorization());

        return $pool;
    }
}
