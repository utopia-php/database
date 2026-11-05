<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;

final class PostgresPartialDDLTest extends TestCase
{
    private const string DROP_BOOKS = 'DROP TABLE "database"."namespace_books"';

    private const string DROP_BOOKS_PERMISSIONS = 'DROP TABLE IF EXISTS "database"."namespace_books_perms"';

    private const string ADD_AUTHOR = 'ALTER TABLE "database"."namespace_books" ADD COLUMN "author"';

    private const string ADD_BOOK = 'ALTER TABLE "database"."namespace_authors" ADD COLUMN "book"';

    private const string DROP_AUTHOR = 'ALTER TABLE "database"."namespace_books" DROP COLUMN "author"';

    private const string DROP_BOOK = 'ALTER TABLE "database"."namespace_authors" DROP COLUMN "book"';

    /** @var list<string> */
    private array $statements = [];

    /** @var array<string, PDOException> */
    private array $failures = [];

    public function testADeleteWhosePermissionsDropFailedDropsThePermissionsTableOnRetry(): void
    {
        $adapter = $this->adapter();
        $this->failures[self::DROP_BOOKS_PERMISSIONS] = $this->engineError('57014', 'canceling statement due to statement timeout');

        try {
            $adapter->deleteCollection('books');
            $this->fail('A permissions table that fails to drop must fail the delete');
        } catch (TimeoutException $error) {
            $this->assertSame('Query timed out', $error->getMessage());
        }

        $this->statements = [];
        $this->failures = [self::DROP_BOOKS => $this->engineError('42P01', 'table "namespace_books" does not exist')];

        try {
            $adapter->deleteCollection('books');
            $this->fail('A retry whose collection table is gone must report it as not found');
        } catch (NotFoundException $error) {
            $this->assertSame('Collection not found', $error->getMessage());
        }

        $this->assertSame([self::DROP_BOOKS, self::DROP_BOOKS_PERMISSIONS], $this->statements);
    }

    public function testACreateRetriedAfterItsFirstColumnWasAddedAddsTheRest(): void
    {
        $this->failures[self::ADD_AUTHOR] = $this->engineError('42701', 'column "author" of relation "namespace_books" already exists');

        try {
            $this->adapter()->createRelationship('books', self::oneToOne());
            $this->fail('A relationship column that already exists must be reported as a duplicate');
        } catch (DuplicateException $error) {
            $this->assertSame('Attribute already exists', $error->getMessage());
        }

        $this->assertCount(2, $this->statements);
        $this->assertStringStartsWith(self::ADD_AUTHOR, $this->statements[0]);
        $this->assertStringStartsWith(self::ADD_BOOK, $this->statements[1]);
    }

    public function testADeleteRetriedAfterItsFirstColumnWasDroppedDropsTheRest(): void
    {
        $this->failures[self::DROP_AUTHOR] = $this->engineError('42703', 'column "author" of relation "namespace_books" does not exist');

        try {
            $this->adapter()->deleteRelationship('books', self::oneToOne(), RelationshipSide::Parent);
            $this->fail('A relationship column that is already gone must be reported as not found');
        } catch (NotFoundException $error) {
            $this->assertSame('Attribute not found', $error->getMessage());
        }

        $this->assertCount(2, $this->statements);
        $this->assertStringStartsWith(self::DROP_AUTHOR, $this->statements[0]);
        $this->assertStringStartsWith(self::DROP_BOOK, $this->statements[1]);
    }

    public function testAColumnThatFailsForAnotherReasonStopsTheCreate(): void
    {
        $this->failures[self::ADD_AUTHOR] = $this->engineError('57014', 'canceling statement due to statement timeout');

        try {
            $this->adapter()->createRelationship('books', self::oneToOne());
            $this->fail('A relationship column that times out must fail the create');
        } catch (TimeoutException $error) {
            $this->assertSame('Query timed out', $error->getMessage());
        }

        $this->assertCount(1, $this->statements);
    }

    public function testAnAppliedColumnInsideATransactionStopsTheCreate(): void
    {
        $adapter = $this->adapter();
        $adapter->startTransaction();
        $started = \count($this->statements);
        $this->failures[self::ADD_AUTHOR] = $this->engineError('42701', 'column "author" of relation "namespace_books" already exists');

        try {
            $adapter->createRelationship('books', self::oneToOne());
            $this->fail('A relationship column that already exists must be reported as a duplicate');
        } catch (DuplicateException $error) {
            $this->assertSame('Attribute already exists', $error->getMessage());
        }

        $this->assertCount(1, \array_slice($this->statements, $started), 'An aborted transaction refuses every later statement');
    }

    private static function oneToOne(): Relationship
    {
        return Relationship::oneToOne(relatedCollection: 'authors', key: 'author', twoWay: true, twoWayKey: 'book');
    }

    private function engineError(string $state, string $message): PDOException
    {
        $error = new class ('SQLSTATE['.$state.']: '.$message, $state) extends PDOException {
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
        $pdo->method('beginTransaction')->willReturn(true);
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
