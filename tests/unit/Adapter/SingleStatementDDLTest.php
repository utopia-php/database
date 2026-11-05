<?php

namespace Tests\Unit\Adapter;

use Closure;
use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\AssertionFailedError;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Relationship;
use Utopia\Database\Relationship\Update;
use Utopia\Database\RelationshipSide;

final class SingleStatementDDLTest extends TestCase
{
    /**
     * @var list<string>
     */
    private array $prepared = [];

    private ?string $refused = null;

    /**
     * @return iterable<string, array{Closure(PDO): SQL}>
     */
    public static function adapters(): iterable
    {
        yield 'MariaDB' => [static fn (PDO $pdo): SQL => new MariaDB($pdo)];
        yield 'MySQL' => [static fn (PDO $pdo): SQL => new MySQL($pdo)];
        yield 'Postgres' => [static fn (PDO $pdo): SQL => new Postgres($pdo)];
        yield 'SQLite' => [static fn (PDO $pdo): SQL => new SQLite($pdo)];
    }

    /**
     * @return iterable<string, array{Closure(PDO): SQL, Closure(SQL): bool}>
     */
    public static function operations(): iterable
    {
        $operations = [
            'create collection' => static fn (SQL $adapter): bool => $adapter->createCollection('books', [Attribute::string(key: 'title', size: 64)]),
            'delete collection' => static fn (SQL $adapter): bool => $adapter->deleteCollection('books'),
            'analyze collection' => static fn (SQL $adapter): bool => $adapter->analyzeCollection('books'),
            'create one-to-one relationship' => static fn (SQL $adapter): bool => $adapter->createRelationship('books', self::oneToOne()),
            'update one-to-one relationship' => static fn (SQL $adapter): bool => $adapter->updateRelationship('books', self::oneToOne(), RelationshipSide::Parent, new Update(key: 'writer', twoWayKey: 'novel')),
            'delete one-to-one relationship' => static fn (SQL $adapter): bool => $adapter->deleteRelationship('books', self::oneToOne(), RelationshipSide::Parent),
            'update many-to-many relationship' => static fn (SQL $adapter): bool => $adapter->updateRelationship('books', self::manyToMany(), RelationshipSide::Parent, new Update(key: 'writers', twoWayKey: 'novels')),
            'delete many-to-many relationship' => static fn (SQL $adapter): bool => $adapter->deleteRelationship('books', self::manyToMany(), RelationshipSide::Parent),
        ];

        foreach (self::adapters() as $adapterName => [$factory]) {
            foreach ($operations as $operationName => $operation) {
                yield "{$adapterName} {$operationName}" => [$factory, $operation];
            }
        }
    }

    /**
     * @param  Closure(PDO): SQL  $factory
     * @param  Closure(SQL): bool  $operation
     */
    #[DataProvider('operations')]
    public function testEveryPrepareHoldsOneStatement(Closure $factory, Closure $operation): void
    {
        $adapter = $this->adapter($factory);

        $this->assertTrue($operation($adapter));
        $this->assertNotEmpty($this->prepared, 'The operation must prepare its DDL');

        foreach ($this->prepared as $query) {
            $this->assertSame(1, self::countStatements($query), "Prepared more than one statement: {$query}");
        }
    }

    /**
     * @param  Closure(PDO): SQL  $factory
     */
    #[DataProvider('adapters')]
    public function testAFailedCreateDropsItsTablesOneStatementAtATime(Closure $factory): void
    {
        $adapter = $this->adapter($factory);
        $this->refused = 'namespace_books_perms';

        try {
            $adapter->createCollection('books');
            $this->fail('A create whose permissions table is refused must fail');
        } catch (Throwable $error) {
            $this->assertNotInstanceOf(AssertionFailedError::class, $error);
        }

        $drops = \array_values(\array_filter($this->prepared, static fn (string $query): bool => \str_contains($query, 'DROP TABLE')));
        $this->assertCount(2, $drops, 'The rollback must drop the collection and its permissions table');

        foreach ($this->prepared as $query) {
            $this->assertSame(1, self::countStatements($query), "Prepared more than one statement: {$query}");
        }
    }

    private static function oneToOne(): Relationship
    {
        return Relationship::oneToOne(relatedCollection: 'authors', key: 'author', twoWay: true, twoWayKey: 'book');
    }

    private static function manyToMany(): Relationship
    {
        return Relationship::manyToMany(relatedCollection: 'authors', key: 'authors', twoWay: true, twoWayKey: 'books');
    }

    private static function countStatements(string $query): int
    {
        $code = \preg_replace(['#/\*.*?\*/#s', "/'(?:[^']|'')*'/", '/"(?:[^"]|"")*"/', '/`[^`]*`/'], '', $query) ?? $query;
        $statements = \array_filter(\array_map(\trim(...), \explode(';', $code)), static fn (string $statement): bool => $statement !== '');

        return \count($statements);
    }

    /**
     * @param  Closure(PDO): SQL  $factory
     */
    private function adapter(Closure $factory): SQL
    {
        $adapter = $factory($this->recorder());
        $adapter->setDatabase('library');
        $adapter->setNamespace('namespace');

        return $adapter;
    }

    private function recorder(): PDO
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $this->prepared[] = $query;

            $refused = $this->refused !== null && \str_starts_with(\ltrim($query), 'CREATE TABLE') && \str_contains($query, $this->refused);

            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturnCallback(static fn (): bool => $refused ? throw new PDOException('SQLSTATE[HY000]: General error: refused') : true);
            $statement->method('fetchAll')->willReturn([]);
            $statement->method('fetch')->willReturn(false);
            $statement->method('closeCursor')->willReturn(true);
            $statement->method('rowCount')->willReturn(0);

            return $statement;
        });

        return $pdo;
    }
}
