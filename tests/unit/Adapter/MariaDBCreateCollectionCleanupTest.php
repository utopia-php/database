<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Attribute;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Timeout as TimeoutException;

final class MariaDBCreateCollectionCleanupTest extends TestCase
{
    private const string PERMISSIONS_TABLE = 'CREATE TABLE `database`.`namespace_books_perms`';

    /** @var list<string> */
    private array $statements = [];

    /** @var array<string, PDOException> */
    private array $failures = [];

    /**
     * @return iterable<string, array{class-string<MariaDB>}>
     */
    public static function engines(): iterable
    {
        yield 'MariaDB' => [MariaDB::class];
        yield 'MySQL' => [MySQL::class];
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('engines')]
    public function testAPermissionsTableThatFailsDropsTheCollectionTable(string $class): void
    {
        $this->failures[self::PERMISSIONS_TABLE] = $this->engineError('70100', 1969, 'Query execution was interrupted (max_statement_time exceeded)');

        try {
            $this->adapter($class)->createCollection('books', [Attribute::string('title', size: 64)]);
            $this->fail('A permissions table that fails must fail the collection');
        } catch (TimeoutException $error) {
            $this->assertSame('Query timed out', $error->getMessage());
        }

        $this->assertCount(3, $this->statements);
        $this->assertSame(
            'DROP TABLE IF EXISTS `database`.`namespace_books`; DROP TABLE IF EXISTS `database`.`namespace_books_perms`',
            $this->statements[2],
        );
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('engines')]
    public function testAPermissionsTableThatAlreadyExistsKeepsBothTables(string $class): void
    {
        $this->failures[self::PERMISSIONS_TABLE] = $this->engineError('42S01', 1050, "Table 'namespace_books_perms' already exists");

        try {
            $this->adapter($class)->createCollection('books', [Attribute::string('title', size: 64)]);
            $this->fail('An existing permissions table must reach the caller');
        } catch (DuplicateException $error) {
            $this->assertSame('Collection already exists', $error->getMessage());
        }

        $this->assertCount(2, $this->statements);
    }

    /**
     * @param class-string<MariaDB> $class
     */
    #[DataProvider('engines')]
    public function testACollectionTableThatFailsDropsNothing(string $class): void
    {
        $this->failures['CREATE TABLE `database`.`namespace_books` '] = $this->engineError('70100', 1969, 'Query execution was interrupted (max_statement_time exceeded)');

        try {
            $this->adapter($class)->createCollection('books', [Attribute::string('title', size: 64)]);
            $this->fail('A collection table that fails must fail the collection');
        } catch (TimeoutException $error) {
            $this->assertSame('Query timed out', $error->getMessage());
        }

        $this->assertCount(1, $this->statements);
    }

    private function engineError(string $state, int $code, string $message): PDOException
    {
        $error = new class ('SQLSTATE[' . $state . ']: ' . $message, $state) extends PDOException {
            public function __construct(string $message, string $state)
            {
                parent::__construct($message);
                $this->code = $state;
            }
        };
        $error->errorInfo = [$state, $code, $message];

        return $error;
    }

    /**
     * @param class-string<MariaDB> $class
     */
    private function adapter(string $class): MariaDB
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

        $adapter = new $class($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
