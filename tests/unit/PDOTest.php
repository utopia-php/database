<?php

namespace Tests\Unit;

use Closure;
use PDOException;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use ReflectionProperty;
use Utopia\Database\PDO;
use Utopia\Database\PDOStatement;

class PDOTest extends TestCase
{
    public function test_method_call_is_forwarded_to_pdo(): void
    {
        $dsn = 'sqlite::memory:';
        $pdoWrapper = new PDO($dsn, null, null);

        // Use Reflection to replace the internal PDO instance with a mock
        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');

        // Create a mock for the internal \PDO object.
        $pdoMock = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdoStatementStub = self::createStub(\PDOStatement::class);

        // Expect that when we call 'query', the mock returns our PDOStatement stub.
        $pdoMock->expects($this->once())
            ->method('query')
            ->with('SELECT 1')
            ->willReturn($pdoStatementStub);

        $pdoProperty->setValue($pdoWrapper, $pdoMock);

        $result = $pdoWrapper->query('SELECT 1');

        $this->assertSame($pdoStatementStub, $result);
    }

    public function test_lost_connection_retries_call(): void
    {
        $dsn = 'sqlite::memory:';
        $pdoWrapper = $this->getMockBuilder(PDO::class)
            ->setConstructorArgs([$dsn, null, null, []])
            ->onlyMethods(['reconnect'])
            ->getMock();

        $pdoMock = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdoStatementStub = self::createStub(\PDOStatement::class);

        $callCount = 0;
        $pdoMock->expects($this->exactly(2))
            ->method('query')
            ->with('SELECT 1')
            ->willReturnCallback(function () use (&$callCount, $pdoStatementStub) {
                $callCount++;
                if ($callCount === 1) {
                    throw new \Exception('Lost connection');
                }
                return $pdoStatementStub;
            });

        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');
        $pdoProperty->setValue($pdoWrapper, $pdoMock);

        $pdoWrapper->expects($this->once())
            ->method('reconnect')
            ->willReturnCallback(function () use ($pdoWrapper, $pdoMock, $pdoProperty) {
                $pdoProperty->setValue($pdoWrapper, $pdoMock);
            });

        $result = $pdoWrapper->query('SELECT 1');

        $this->assertSame($pdoStatementStub, $result);
    }

    public function test_non_lost_connection_exception_is_rethrown(): void
    {
        $dsn = 'sqlite::memory:';
        $pdoWrapper = new PDO($dsn, null, null);

        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');

        $pdoMock = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdoMock->expects($this->once())
            ->method('query')
            ->with('SELECT 1')
            ->will($this->throwException(new \Exception('Other error')));

        $pdoProperty->setValue($pdoWrapper, $pdoMock);

        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Other error');

        $pdoWrapper->query('SELECT 1');
    }

    public function test_reconnect_creates_new_pdo_instance(): void
    {
        $dsn = 'sqlite::memory:';
        $pdoWrapper = new PDO($dsn, null, null);

        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');

        $oldPDO = $pdoProperty->getValue($pdoWrapper);
        $pdoWrapper->reconnect();
        $newPDO = $pdoProperty->getValue($pdoWrapper);

        $this->assertNotSame($oldPDO, $newPDO, 'Reconnect should create a new PDO instance');
    }

    public function test_method_call_for_prepare(): void
    {
        $dsn = 'sqlite::memory:';
        $pdoWrapper = new PDO($dsn, null, null);

        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');

        $pdoMock = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdoStatementStub = self::createStub(\PDOStatement::class);

        $pdoMock->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM table', [\PDO::ATTR_CURSOR => \PDO::CURSOR_FWDONLY])
            ->willReturn($pdoStatementStub);

        $pdoProperty->setValue($pdoWrapper, $pdoMock);

        $result = $pdoWrapper->prepare('SELECT * FROM table', [\PDO::ATTR_CURSOR => \PDO::CURSOR_FWDONLY]);

        $this->assertSame($pdoStatementStub, $result->getStatement());
    }

    public function testPrepareNativeReconnectsOutsideTransaction(): void
    {
        $pdoWrapper = $this->getMockBuilder(PDO::class)
            ->setConstructorArgs(['sqlite::memory:', null, null, []])
            ->onlyMethods(['reconnect'])
            ->getMock();

        $pdoMock = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $statement = self::createStub(\PDOStatement::class);

        $pdoMock->method('inTransaction')->willReturn(false);
        $calls = 0;
        $pdoMock->expects($this->exactly(2))
            ->method('prepare')
            ->with('SELECT 1', [])
            ->willReturnCallback(function () use (&$calls, $statement): \PDOStatement {
                $calls++;
                if ($calls === 1) {
                    throw new \PDOException('server has gone away');
                }

                return $statement;
            });

        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');
        $pdoProperty->setValue($pdoWrapper, $pdoMock);

        $pdoWrapper->expects($this->once())->method('reconnect');

        $this->assertSame($statement, $pdoWrapper->prepareNative('SELECT 1'));
    }

    public function testPrepareNativeThrowsWhenNativePrepareReturnsFalse(): void
    {
        $pdoWrapper = new PDO('sqlite::memory:', null, null);

        $pdoMock = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdoMock->expects($this->once())
            ->method('prepare')
            ->with('INVALID', [])
            ->willReturn(false);

        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');
        $pdoProperty->setValue($pdoWrapper, $pdoMock);

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('Failed to prepare statement: INVALID');

        $pdoWrapper->prepareNative('INVALID');
    }

    public function testReconnectReplaysTheConfiguredSession(): void
    {
        $pdo = new PDO('sqlite::memory:', null, null);
        $pdo->configure('cache', 'PRAGMA cache_size = 100');
        $pdo->configure('cache', 'PRAGMA cache_size = 200');
        $pdo->configure('keys', 'PRAGMA foreign_keys = ON');

        $pdo->reconnect();

        $this->assertSame(200, $this->pragma($pdo, 'cache_size'), 'The latest statement for a setting must win');
        $this->assertSame(1, $this->pragma($pdo, 'foreign_keys'));
    }

    public function testCallRetriedAfterALostConnectionRunsOnTheConfiguredSession(): void
    {
        $pdo = new PDO('sqlite::memory:', null, null);
        $pdo->configure('marker', 'CREATE TEMP TABLE marker AS SELECT 7 AS value');

        $lost = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $lost->method('inTransaction')->willReturn(false);
        $lost->expects($this->once())
            ->method('query')
            ->willThrowException(new PDOException('SQLSTATE[HY000]: General error: 2006 MySQL server has gone away'));
        (new ReflectionProperty(PDO::class, 'pdo'))->setValue($pdo, $lost);

        $statement = $pdo->query('SELECT value FROM temp.marker');

        $this->assertInstanceOf(\PDOStatement::class, $statement);
        $this->assertSame(7, $statement->fetchColumn());
    }

    public function testReconnectKeepsTheLostConnectionWhenTheSessionCannotBeReplayed(): void
    {
        $pdo = new PDO('sqlite::memory:', null, null);
        $pdo->exec('CREATE TEMP TABLE local (value INTEGER)');
        $pdo->configure('row', 'INSERT INTO temp.local VALUES (1)');

        $connection = new ReflectionProperty(PDO::class, 'pdo');
        $lost = $connection->getValue($pdo);

        $failure = null;
        try {
            $pdo->reconnect();
        } catch (PDOException $error) {
            $failure = $error;
        }

        $this->assertInstanceOf(PDOException::class, $failure, 'The new connection has no temp.local to replay into');
        $this->assertSame($lost, $connection->getValue($pdo), 'A connection missing the configured session must never be used');
    }

    public function testReconnectReplaysAttributes(): void
    {
        $pdo = new PDO('sqlite::memory:', null, null);
        $this->assertTrue($pdo->setAttribute(\PDO::ATTR_CASE, \PDO::CASE_UPPER));
        $this->assertTrue($pdo->setAttribute(\PDO::ATTR_DEFAULT_FETCH_MODE, \PDO::FETCH_NUM));

        $pdo->reconnect();

        $this->assertSame(\PDO::CASE_UPPER, $pdo->getAttribute(\PDO::ATTR_CASE));
        $this->assertSame(\PDO::FETCH_NUM, $pdo->getAttribute(\PDO::ATTR_DEFAULT_FETCH_MODE));
    }

    public function testStatementsAfterALostTransactionAreRefusedUntilItIsRolledBack(): void
    {
        $path = $this->createDatabaseFile();
        [$pdo, $endSession] = $this->createLosableConnection($path);
        $pdo->exec('CREATE TABLE items (value INTEGER)');
        $pdo->beginTransaction();
        $pdo->exec('INSERT INTO items VALUES (1)');
        $endSession();

        try {
            $pdo->exec('INSERT INTO items VALUES (2)');
        } catch (PDOException) {
        }

        $this->assertStatementRefused(fn (): mixed => $pdo->exec('INSERT INTO items VALUES (3)'));
        $this->assertStatementRefused(fn (): mixed => $pdo->query('SELECT value FROM items'));
        $this->assertStatementRefused(fn (): mixed => $pdo->prepare('INSERT INTO items VALUES (4)'));
        $this->assertStatementRefused(fn (): mixed => $pdo->commit());
        $this->assertSame([], $this->values($path), 'Nothing may run on its own after the transaction was lost');
        $this->assertTrue($pdo->inTransaction(), 'The caller still holds a transaction until it rolls back');

        $this->assertTrue($pdo->rollBack());
        $this->assertFalse($pdo->inTransaction());

        $pdo->exec('INSERT INTO items VALUES (5)');
        $this->assertSame([5], $this->values($path));
    }

    public function testARollbackStatementEndsALostTransaction(): void
    {
        $path = $this->createDatabaseFile();
        [$pdo, $endSession] = $this->createLosableConnection($path);
        $pdo->exec('CREATE TABLE items (value INTEGER)');
        $pdo->beginTransaction();
        $endSession();

        try {
            $pdo->exec('INSERT INTO items VALUES (1)');
        } catch (PDOException) {
        }

        $pdo->prepare('ROLLBACK');

        $this->assertFalse($pdo->inTransaction());
        $pdo->exec('INSERT INTO items VALUES (2)');
        $this->assertSame([2], $this->values($path));
    }

    public function testAnExplicitReconnectEndsALostTransaction(): void
    {
        $path = $this->createDatabaseFile();
        [$pdo, $endSession] = $this->createLosableConnection($path);
        $pdo->exec('CREATE TABLE items (value INTEGER)');
        $pdo->beginTransaction();
        $endSession();

        try {
            $pdo->exec('INSERT INTO items VALUES (1)');
        } catch (PDOException) {
        }

        $pdo->reconnect();

        $this->assertFalse($pdo->inTransaction());
        $pdo->exec('INSERT INTO items VALUES (2)');
        $this->assertSame([2], $this->values($path));
    }

    private function createDatabaseFile(): string
    {
        $path = \tempnam(\sys_get_temp_dir(), 'pdo-test-');
        $this->assertIsString($path);
        \register_shutdown_function(static fn (): bool => @\unlink($path));

        return $path;
    }

    /**
     * A connection whose session the server can end: afterwards every statement on the old
     * handle fails as a dropped MySQL connection does, and the handle still reports its
     * transaction.
     *
     * @return array{PDO, Closure(): void}
     */
    private function createLosableConnection(string $path): array
    {
        $pdo = new class ("sqlite:{$path}", null, null) extends PDO {
            public function endSession(): void
            {
                $this->pdo = new class () extends \PDO {
                    public function __construct()
                    {
                    }

                    public function inTransaction(): bool
                    {
                        return true;
                    }

                    public function exec(string $statement): int|false
                    {
                        throw new PDOException('SQLSTATE[HY000]: General error: 2006 MySQL server has gone away');
                    }
                };
            }
        };

        return [$pdo, $pdo->endSession(...)];
    }

    /**
     * @param callable(): mixed $statement
     */
    private function assertStatementRefused(callable $statement): void
    {
        try {
            $statement();
        } catch (PDOException $error) {
            $this->assertStringContainsString('roll it back', $error->getMessage());

            return;
        }

        $this->fail('A statement after a lost transaction must be refused');
    }

    /**
     * @return array<int>
     */
    private function values(string $path): array
    {
        $statement = (new \PDO("sqlite:{$path}"))->query('SELECT value FROM items ORDER BY value');
        $this->assertInstanceOf(\PDOStatement::class, $statement);

        return \array_map(intval(...), $statement->fetchAll(\PDO::FETCH_COLUMN));
    }

    private function pragma(PDO $pdo, string $name): int
    {
        $statement = $pdo->query("PRAGMA {$name}");
        $this->assertInstanceOf(\PDOStatement::class, $statement);

        $value = $statement->fetchColumn();
        $this->assertIsInt($value);

        return $value;
    }
}
