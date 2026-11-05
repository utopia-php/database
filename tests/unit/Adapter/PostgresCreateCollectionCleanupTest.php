<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Tests\Unit\Support\StderrCapture;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Attribute;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Index;
use Utopia\Query\OrderDirection;

final class PostgresCreateCollectionCleanupTest extends TestCase
{
    private const string DECLARED_INDEX = 'CREATE INDEX "namespace__books_title_index"';

    private const string UID_INDEX = 'CREATE UNIQUE INDEX "namespace_books_uid"';

    private const string PERMISSIONS_TABLE = 'CREATE TABLE "database"."namespace_books_perms"';

    /** @var list<string> */
    private array $statements = [];

    /** @var array<string, PDOException> */
    private array $failures = [];

    public function testADeclaredIndexThatFailsDropsBothTablesAndReachesTheCaller(): void
    {
        $this->failures[self::DECLARED_INDEX] = $this->engineError('42703', 'column "title" does not exist');

        try {
            $this->adapter()->createCollection('books', [Attribute::string('title', size: 64)], [
                Index::key('title_index', ['title']),
            ]);
            $this->fail('A declared index that fails must fail the collection');
        } catch (NotFoundException $error) {
            $this->assertSame('Attribute not found', $error->getMessage());
        }

        $this->assertCount(11, $this->statements);
        $this->assertStringStartsWith(self::DECLARED_INDEX, $this->statements[8]);
        $this->assertSame('DROP TABLE IF EXISTS "database"."namespace_books"', $this->statements[9]);
        $this->assertSame('DROP TABLE IF EXISTS "database"."namespace_books_perms"', $this->statements[10]);
    }

    public function testACleanupThatFailsKeepsTheOriginalErrorAndLogsTheCleanupFailure(): void
    {
        $this->failures[self::DECLARED_INDEX] = $this->engineError('42703', 'column "title" does not exist');
        $this->failures['DROP TABLE IF EXISTS'] = $this->engineError('25P02', 'current transaction is aborted, commands ignored until end of transaction block');

        $error = null;
        $log = StderrCapture::during(function () use (&$error): void {
            try {
                $this->adapter()->createCollection('books', [Attribute::string('title', size: 64)], [
                    Index::key('title_index', ['title']),
                ]);
            } catch (\Throwable $caught) {
                $error = $caught;
            }
        });

        $this->assertInstanceOf(NotFoundException::class, $error, 'the index failure reaches the caller, not the failed drop');
        $this->assertSame('Attribute not found', $error->getMessage());
        $this->assertStringStartsWith('DROP TABLE IF EXISTS', $this->statements[9]);
        $this->assertStringContainsString("Failed to rollback collection 'books': SQLSTATE[25P02]", $log, 'the failed cleanup is logged');
    }

    public function testADeclaredIndexThatAlreadyExistsKeepsBothTables(): void
    {
        $this->failures[self::DECLARED_INDEX] = $this->engineError('42P07', 'relation "namespace__books_title_index" already exists');

        try {
            $this->adapter()->createCollection('books', [Attribute::string('title', size: 64)], [
                Index::key('title_index', ['title']),
            ]);
            $this->fail('A declared index that already exists must reach the caller');
        } catch (DuplicateException $error) {
            $this->assertInstanceOf(PDOException::class, $error->getPrevious());
        }

        $this->assertCount(9, $this->statements);
        foreach ($this->statements as $statement) {
            $this->assertStringNotContainsString('DROP TABLE', $statement);
        }
    }

    public function testAnInternalIndexThatAlreadyExistsDropsBothTables(): void
    {
        $this->failures[self::UID_INDEX] = $this->engineError('42P07', 'relation "namespace_books_uid" already exists');

        try {
            $this->adapter()->createCollection('books', [Attribute::string('title', size: 64)]);
            $this->fail('An internal index that already exists must fail the collection');
        } catch (DuplicateException $error) {
            $this->assertInstanceOf(PDOException::class, $error->getPrevious());
        }

        $this->assertCount(4, $this->statements);
        $this->assertStringStartsWith(self::UID_INDEX, $this->statements[1]);
        $this->assertSame('DROP TABLE IF EXISTS "database"."namespace_books"', $this->statements[2]);
        $this->assertSame('DROP TABLE IF EXISTS "database"."namespace_books_perms"', $this->statements[3]);
    }

    public function testAPermissionsTableThatAlreadyExistsDropsBothTables(): void
    {
        $this->failures[self::PERMISSIONS_TABLE] = $this->engineError('42P07', 'relation "namespace_books_perms" already exists');

        try {
            $this->adapter()->createCollection('books', [Attribute::string('title', size: 64)]);
            $this->fail('A permissions table that already exists must fail the collection');
        } catch (DuplicateException $error) {
            $this->assertInstanceOf(PDOException::class, $error->getPrevious());
        }

        $this->assertCount(8, $this->statements);
        $this->assertStringStartsWith(self::PERMISSIONS_TABLE, $this->statements[5]);
        $this->assertSame('DROP TABLE IF EXISTS "database"."namespace_books"', $this->statements[6]);
        $this->assertSame('DROP TABLE IF EXISTS "database"."namespace_books_perms"', $this->statements[7]);
    }

    public function testASpatialIndexWithOrdersDropsBothTablesAndReachesTheCaller(): void
    {
        try {
            $this->adapter()->createCollection('places', [Attribute::point('location', required: true)], [
                Index::spatial('location_index', 'location', order: OrderDirection::Desc),
            ]);
            $this->fail('A spatial index with orders must fail the collection');
        } catch (DatabaseException $error) {
            $this->assertSame('Spatial indexes with explicit orders are not supported. Remove the orders to create this index.', $error->getMessage());
        }

        $this->assertCount(10, $this->statements);
        $this->assertSame('DROP TABLE IF EXISTS "database"."namespace_places"', $this->statements[8]);
        $this->assertSame('DROP TABLE IF EXISTS "database"."namespace_places_perms"', $this->statements[9]);
    }

    public function testATableThatFailsToBeCreatedDropsNothing(): void
    {
        $this->failures['CREATE TABLE "database"."namespace_books"'] = $this->engineError('57014', 'canceling statement due to statement timeout');

        try {
            $this->adapter()->createCollection('books', [Attribute::string('title', size: 64)]);
            $this->fail('A table that fails to be created must fail the collection');
        } catch (TimeoutException $error) {
            $this->assertSame('Query timed out', $error->getMessage());
        }

        $this->assertCount(1, $this->statements);
    }

    private function engineError(string $state, string $message): PDOException
    {
        $error = new class ('SQLSTATE[' . $state . ']: ' . $message, $state) extends PDOException {
            public function __construct(string $message, string $state)
            {
                parent::__construct($message);
                $this->code = $state;
            }
        };
        $error->errorInfo = [$state, 7, $message];

        return $error;
    }

    private function adapter(): Postgres
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $this->statements[] = $query;
            $failure = null;
            foreach ($this->failures as $prefix => $error) {
                if (\str_starts_with($query, $prefix)) {
                    $failure = $error;
                }
            }

            $statement = $this->createStub(PDOStatement::class);
            if ($failure === null) {
                $statement->method('execute')->willReturn(true);
            } else {
                $statement->method('execute')->willThrowException($failure);
            }

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
