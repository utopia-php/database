<?php

namespace Tests\Unit;

use Closure;
use Exception;
use PDO;
use PDOException;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use stdClass;
use Throwable;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Exception\Character as CharacterException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Transaction as TransactionException;

final class EngineErrorMappingTest extends TestCase
{
    private const string NAMESPACE = 'engine';

    /**
     * @return array<string, array{0: Closure(PDOException): Throwable, 1: PDOException, 2: class-string<Throwable>, 3: string}>
     */
    public static function lockConflictProvider(): array
    {
        $deadlock = self::engineError('40001', 1213, 'SQLSTATE[40001]: Serialization failure: 1213 Deadlock found when trying to get lock; try restarting transaction');
        $lockWait = self::engineError('HY000', 1205, 'SQLSTATE[HY000]: General error: 1205 Lock wait timeout exceeded; try restarting transaction');
        $missingTable = self::engineError('42S02', 1146, "SQLSTATE[42S02]: Base table or view not found: 1146 Table 'utopiaTests.engine_orders' doesn't exist");

        return [
            'MariaDB deadlock' => [self::mariaDB(), $deadlock, TransactionException::class, 'Deadlock detected'],
            'MySQL deadlock' => [self::mySQL(), $deadlock, TransactionException::class, 'Deadlock detected'],
            'MariaDB lock wait timeout' => [self::mariaDB(), $lockWait, TransactionException::class, 'Lock wait timeout exceeded'],
            'MySQL lock wait timeout' => [self::mySQL(), $lockWait, TransactionException::class, 'Lock wait timeout exceeded'],
            'MariaDB statement on a missing table' => [self::mariaDB(), $missingTable, NotFoundException::class, 'Collection not found'],
            'MySQL statement on a missing table' => [self::mySQL(), $missingTable, NotFoundException::class, 'Collection not found'],
            'Postgres deadlock' => [
                self::postgres(),
                self::engineError('40P01', 7, "SQLSTATE[40P01]: Deadlock detected: 7 ERROR:  deadlock detected\nDETAIL:  Process 81 waits for ShareLock on transaction 740; blocked by process 82."),
                TransactionException::class,
                'Deadlock detected',
            ],
            'Postgres serialization failure' => [
                self::postgres(),
                self::engineError('40001', 7, 'SQLSTATE[40001]: Serialization failure: 7 ERROR:  could not serialize access due to concurrent update'),
                TransactionException::class,
                'Could not serialize access due to a concurrent update',
            ],
            'Postgres lock not available' => [
                self::postgres(),
                self::engineError('55P03', 7, 'SQLSTATE[55P03]: Lock not available: 7 ERROR:  canceling statement due to lock timeout'),
                TransactionException::class,
                'Lock not available',
            ],
            'Postgres invalid UTF-8' => [
                self::postgres(),
                self::engineError('22021', 7, 'SQLSTATE[22021]: Character not in repertoire: 7 ERROR:  invalid byte sequence for encoding "UTF8": 0xc3 0x28'),
                CharacterException::class,
                'Invalid character',
            ],
        ];
    }

    /**
     * @param  Closure(PDOException): Throwable  $map
     * @param  class-string<Throwable>  $expected
     */
    #[DataProvider('lockConflictProvider')]
    public function testLockConflictsMissingTablesAndBadCharactersAreMapped(Closure $map, PDOException $error, string $expected, string $message): void
    {
        $this->assertMapped($map, $error, $expected, $message);
    }

    public function testSQLiteDoesNotTreatTheMySQLTimeoutCodeAsATimeout(): void
    {
        $error = self::engineError('HY000', 3024, 'SQLSTATE[HY000]: General error: 3024 Query execution was interrupted');

        $this->assertSame($error, self::sqlite()($error));
    }

    public function testPostgresDeleteCollectionWithoutItsTableIsNotFoundAndStillDropsThePermissionsTable(): void
    {
        $statements = [];
        $adapter = $this->postgresRecording($statements, self::engineError('42P01', 7, 'SQLSTATE[42P01]: Undefined table: 7 ERROR:  table "engine_orders" does not exist'));

        $error = null;
        try {
            $adapter->deleteCollection('orders');
        } catch (Throwable $caught) {
            $error = $caught;
        }

        $this->assertInstanceOf(NotFoundException::class, $error);
        $this->assertSame('Collection not found', $error->getMessage());
        $this->assertSame([
            'DROP TABLE "utopiaTests"."engine_orders"; DROP TABLE IF EXISTS "utopiaTests"."engine_orders_perms"',
            'DROP TABLE IF EXISTS "utopiaTests"."engine_orders_perms"',
        ], $statements);
    }

    public function testPostgresDeleteCollectionDropsBothTablesInOneStatement(): void
    {
        $statements = [];
        $adapter = $this->postgresRecording($statements);

        $this->assertTrue($adapter->deleteCollection('orders'));
        $this->assertSame([
            'DROP TABLE "utopiaTests"."engine_orders"; DROP TABLE IF EXISTS "utopiaTests"."engine_orders_perms"',
        ], $statements);
    }

    public function testPostgresDeleteCollectionPassesOtherErrorsThrough(): void
    {
        $statements = [];
        $lockTimeout = self::engineError('55P03', 7, 'SQLSTATE[55P03]: Lock not available: 7 ERROR:  canceling statement due to lock timeout');
        $adapter = $this->postgresRecording($statements, $lockTimeout);

        $error = null;
        try {
            $adapter->deleteCollection('orders');
        } catch (Throwable $caught) {
            $error = $caught;
        }

        $this->assertInstanceOf(TransactionException::class, $error);
        $this->assertSame($lockTimeout, $error->getPrevious());
        $this->assertCount(1, $statements);
    }

    /**
     * @param  Closure(PDOException): Throwable  $map
     * @param  class-string<Throwable>  $expected
     */
    private function assertMapped(Closure $map, PDOException $error, string $expected, string $message): void
    {
        $mapped = $map($error);

        $this->assertInstanceOf($expected, $mapped);
        $this->assertSame($message, $mapped->getMessage());
        $this->assertSame($error, $mapped->getPrevious());
    }

    /**
     * @param  list<string>  $statements
     */
    private function postgresRecording(array &$statements, ?PDOException $firstError = null): Postgres
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $sql) use (&$statements, $firstError): PDOStatement {
            $statements[] = $sql;
            $statement = $this->createStub(PDOStatement::class);
            if ($firstError !== null && \count($statements) === 1) {
                $statement->method('execute')->willThrowException($firstError);
            } else {
                $statement->method('execute')->willReturn(true);
            }

            return $statement;
        });

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('utopiaTests');
        $adapter->setNamespace(self::NAMESPACE);

        return $adapter;
    }

    private static function engineError(string $state, int $code, string $message): PDOException
    {
        $error = new PDOException($message);
        (new ReflectionProperty(Exception::class, 'code'))->setValue($error, $state);
        $error->errorInfo = [$state, $code, $message];

        return $error;
    }

    /**
     * @return Closure(PDOException): Throwable
     */
    private static function mariaDB(): Closure
    {
        $adapter = new class (new stdClass()) extends MariaDB {
            public function map(PDOException $error): Throwable
            {
                return $this->processException($error);
            }
        };
        $adapter->setNamespace(self::NAMESPACE);

        return $adapter->map(...);
    }

    /**
     * @return Closure(PDOException): Throwable
     */
    private static function mySQL(): Closure
    {
        $adapter = new class (new stdClass()) extends MySQL {
            public function map(PDOException $error): Throwable
            {
                return $this->processException($error);
            }
        };
        $adapter->setNamespace(self::NAMESPACE);

        return $adapter->map(...);
    }

    /**
     * @return Closure(PDOException): Throwable
     */
    private static function postgres(): Closure
    {
        $adapter = new class (new stdClass()) extends Postgres {
            public function map(PDOException $error): Throwable
            {
                return $this->processException($error);
            }
        };
        $adapter->setNamespace(self::NAMESPACE);

        return $adapter->map(...);
    }

    /**
     * @return Closure(PDOException): Throwable
     */
    private static function sqlite(): Closure
    {
        $adapter = new class (new PDO('sqlite::memory:')) extends SQLite {
            public function map(PDOException $error): Throwable
            {
                return $this->processException($error);
            }
        };
        $adapter->setNamespace(self::NAMESPACE);

        return $adapter->map(...);
    }
}
