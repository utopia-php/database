<?php

namespace Tests\Unit\Adapter;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Tests\Unit\Support\NativeFullOuterJoinSQLite;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Feature\QueryBuilder;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Builder\MySQL as MySQLBuilder;
use Utopia\Database\Builder\Postgres as PostgresBuilder;
use Utopia\Database\Builder\Scope;
use Utopia\Database\Builder\Scoping;
use Utopia\Database\Builder\SQLite as SQLiteBuilder;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Builder;
use Utopia\Query\Builder\Feature\FullOuterJoins;
use Utopia\Query\Schema;
use Utopia\Query\Schema\SQLite as SQLiteSchema;

/**
 * A builder that read a collection through its scope keeps that collection's tenant hooks, so it must not read
 * another table under them, nor take another scope.
 */
final class ScopedBuilderTest extends TestCase
{
    public function testABuilderThatReadACollectionRefusesAnother(): void
    {
        $adapter = $this->adapter();

        $this->expectException(QueryException::class);

        $adapter->builder()->from(Database::METADATA)->from('authors');
    }

    public function testABuilderThatReadACollectionRefusesTheMetadataCollection(): void
    {
        $adapter = $this->adapter();

        $this->expectException(QueryException::class);

        $adapter->builder()->from('authors')->from(Database::METADATA);
    }

    public function testABuilderThatReadACollectionRefusesAStoredTable(): void
    {
        $adapter = $this->adapter();

        $this->expectException(QueryException::class);

        $adapter->builder()->from('authors')->fromTable('namespace_books');
    }

    public function testABuilderThatReadACollectionRefusesAnotherScope(): void
    {
        $adapter = $this->adapter();
        $scope = (new ReflectionMethod($adapter, 'scope'))->invoke($adapter);
        $this->assertInstanceOf(Scope::class, $scope);

        $this->expectException(QueryException::class);

        $adapter->builder()->from('authors')->scope($scope);
    }

    public function testReadingTheSameCollectionAgainKeepsItsTenant(): void
    {
        $builder = $this->adapter()->builder()->from('authors');
        $builder->build();

        $query = $builder->reset()->from('authors')->select(['name'])->build()->query;

        $this->assertSame(1, \substr_count($query, '_tenant'), $query);
    }

    public function testJoinsNameCollectionsKeptToTheTenant(): void
    {
        $query = $this->adapter()->builder()
            ->from('authors')
            ->join('books', 'namespace_authors._uid', 'book.authorId', '=', 'book')
            ->select(['book.pages'])
            ->build()
            ->query;

        $this->assertStringContainsString('JOIN `namespace_books` AS `book`', $query);
        $this->assertStringContainsString('`book`._tenant IN (?)', $query);
    }

    public function testABuilderThatReadACollectionRefusesAnInsertIntoATable(): void
    {
        $this->expectException(QueryException::class);

        $this->adapter()->builder()->from('authors')->into('namespace_books');
    }

    public function testABuilderThatReadACollectionRefusesAnotherAlias(): void
    {
        $builder = $this->adapter()->builder()->from('authors');

        $this->expectException(QueryException::class);

        $builder->reset()->from('authors', 'author');
    }

    /**
     * @return iterable<string, array{Closure(SQL): mixed}>
     */
    public static function joinedWrites(): iterable
    {
        yield 'MySQL updateJoin' => [static fn (SQL $adapter) => self::mysqlBuilder($adapter)->updateJoin('books', 'namespace_authors._uid', 'books.authorId')];
        yield 'MySQL deleteJoin' => [static fn (SQL $adapter) => self::mysqlBuilder($adapter)->deleteJoin('namespace_authors', 'books', 'namespace_authors._uid', 'books.authorId')];
        yield 'Postgres updateFrom' => [static fn (SQL $adapter) => self::postgresBuilder($adapter)->updateFrom('books')];
        yield 'Postgres deleteUsing' => [static fn (SQL $adapter) => self::postgresBuilder($adapter)->deleteUsing('books', 'books.authorId = namespace_authors._uid')];
    }

    /**
     * @param  Closure(SQL): mixed  $write
     */
    #[DataProvider('joinedWrites')]
    public function testABuilderThatReadACollectionRefusesAMultiTableWrite(Closure $write): void
    {
        $pdo = $this->createStub(PDO::class);
        $adapter = \str_contains((string) $this->dataName(), 'Postgres') ? new Postgres($pdo) : new MySQL($pdo);
        $adapter->setNamespace('namespace');
        $adapter->setSharedTables(true);
        $adapter->setTenant(7);

        $this->expectException(QueryException::class);

        $write($adapter);
    }

    public function testAJoinAddedBeforeFromNamesACollection(): void
    {
        $query = $this->adapter()->builder()
            ->join('books', 'namespace_authors._uid', 'book.authorId', '=', 'book')
            ->from('authors')
            ->select(['book.pages'])
            ->build()
            ->query;

        $this->assertStringContainsString('JOIN `namespace_books` AS `book`', $query);
        $this->assertStringContainsString('`book`._tenant IN (?)', $query);
    }

    public function testAFullOuterJoinOfABuilderWithItsOwnJoinMethodsNamesACollection(): void
    {
        $adapter = new NativeFullOuterJoinSQLite(new PDO('sqlite::memory:'));
        $adapter->setNamespace('namespace');
        $builder = $adapter->builder()->from('authors');
        $this->assertInstanceOf(FullOuterJoins::class, $builder);

        $query = $builder->fullOuterJoin('books', 'namespace_authors._uid', 'book.authorId', '=', 'book')->select(['book.pages'])->build()->query;

        $this->assertStringContainsString('FULL OUTER JOIN `namespace_books` AS `book`', $query);
    }

    public function testAResetBuilderKeepsTheTenantOfItsCollection(): void
    {
        $builder = $this->adapter()->builder()->from('authors')
            ->rightJoin('books', 'namespace_authors._uid', 'book.authorId', '=', 'book');
        $builder->build();

        $query = $builder->reset()->from('authors')->select(['name'])->build()->query;

        $this->assertStringEndsWith('WHERE `namespace_authors`._tenant IN (?)', $query);
    }

    public function testDatabaseFromNamesTheCollectionByTheAlias(): void
    {
        $database = $this->database($this->adapter());

        $query = $database->getAuthorization()->skip(fn (): string => $database->from('authors', 'author')->select(['author.name'])->build()->query);

        $this->assertStringContainsString('FROM `namespace_authors` AS `author`', $query);
        $this->assertStringContainsString('`author`._tenant IN (?)', $query);
    }

    public function testDatabaseFromRefusesABuilderWithNoScope(): void
    {
        $adapter = new class () extends Memory implements QueryBuilder {
            #[\Override]
            public function builder(): Builder&Scoping
            {
                return new SQLiteBuilder();
            }

            #[\Override]
            public function schema(): Schema
            {
                return new SQLiteSchema();
            }
        };
        $database = $this->database($adapter);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('The adapter handed out a query builder with no scope');

        $database->getAuthorization()->skip(fn () => $database->from('authors'));
    }

    private static function mysqlBuilder(SQL $adapter): MySQLBuilder
    {
        $builder = $adapter->builder()->from('authors');
        \assert($builder instanceof MySQLBuilder);

        return $builder;
    }

    private static function postgresBuilder(SQL $adapter): PostgresBuilder
    {
        $builder = $adapter->builder()->from('authors');
        \assert($builder instanceof PostgresBuilder);

        return $builder;
    }

    private function adapter(): SQLite
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $adapter->setNamespace('namespace');
        $adapter->setSharedTables(true);
        $adapter->setTenant(7);

        return $adapter;
    }

    private function database(\Utopia\Database\Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->setAuthorization(new Authorization());

        return $database;
    }
}
