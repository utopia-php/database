<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Feature\QueryBuilder;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Builder\Scope;
use Utopia\Database\Builder\Scoping;
use Utopia\Database\Builder\SQLite as SQLiteBuilder;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Builder;
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
