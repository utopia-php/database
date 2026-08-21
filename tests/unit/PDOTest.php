<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use ReflectionClass;
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
}
