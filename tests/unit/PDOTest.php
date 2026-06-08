<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use ReflectionClass;
use Utopia\Database\PDO;

class PDOTest extends TestCase
{
    public function testMethodCallIsForwardedToPDO(): void
    {
        $dsn = 'sqlite::memory:';
        $pdoWrapper = new PDO($dsn, null, null);

        // Use Reflection to replace the internal PDO instance with a mock
        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');
        $pdoProperty->setAccessible(true);

        // Create a mock for the internal \PDO object.
        $pdoMock = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        // Create a PDOStatement mock since query returns a PDOStatement
        $pdoStatementMock = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();

        // Expect that when we call 'query', the mock returns our PDOStatement mock.
        $pdoMock->expects($this->once())
            ->method('query')
            ->with('SELECT 1')
            ->willReturn($pdoStatementMock);

        $pdoProperty->setValue($pdoWrapper, $pdoMock);

        $result = $pdoWrapper->query('SELECT 1');

        $this->assertSame($pdoStatementMock, $result);
    }

    public function testLostConnectionRetriesCall(): void
    {
        $dsn = 'sqlite::memory:';
        $pdoWrapper = $this->getMockBuilder(PDO::class)
            ->setConstructorArgs([$dsn, null, null, []])
            ->onlyMethods(['reconnect'])
            ->getMock();

        $pdoMock = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdoStatementMock = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdoMock->expects($this->exactly(2))
            ->method('query')
            ->with('SELECT 1')
            ->will($this->onConsecutiveCalls(
                $this->throwException(new \Exception("Lost connection")),
                $pdoStatementMock
            ));

        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');
        $pdoProperty->setAccessible(true);
        $pdoProperty->setValue($pdoWrapper, $pdoMock);

        $pdoWrapper->expects($this->once())
            ->method('reconnect')
            ->willReturnCallback(function () use ($pdoWrapper, $pdoMock, $pdoProperty) {
                $pdoProperty->setValue($pdoWrapper, $pdoMock);
            });

        $result = $pdoWrapper->query('SELECT 1');

        $this->assertSame($pdoStatementMock, $result);
    }

    public function testNonLostConnectionExceptionIsRethrown(): void
    {
        $dsn = 'sqlite::memory:';
        $pdoWrapper = new PDO($dsn, null, null);

        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');
        $pdoProperty->setAccessible(true);

        $pdoMock = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdoMock->expects($this->once())
            ->method('query')
            ->with('SELECT 1')
            ->will($this->throwException(new \Exception("Other error")));

        $pdoProperty->setValue($pdoWrapper, $pdoMock);

        $this->expectException(\Exception::class);
        $this->expectExceptionMessage("Other error");

        $pdoWrapper->query('SELECT 1');
    }

    public function testReconnectCreatesNewPDOInstance(): void
    {
        $dsn = 'sqlite::memory:';
        $pdoWrapper = new PDO($dsn, null, null);

        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');
        $pdoProperty->setAccessible(true);

        $oldPDO = $pdoProperty->getValue($pdoWrapper);
        $pdoWrapper->reconnect();
        $newPDO = $pdoProperty->getValue($pdoWrapper);

        $this->assertNotSame($oldPDO, $newPDO, "Reconnect should create a new PDO instance");
    }

    public function testReconnectRetriesOnTransientFailure(): void
    {
        $attempt = 0;
        $pdoMock = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdoWrapper = new class ('', null, null, [], 3) extends PDO {
            public ?\Closure $factory = null;

            public function __construct(
                string $dsn,
                ?string $username,
                ?string $password,
                array $config = [],
                int $retries = 3
            ) {
                $this->retries = $retries;
            }

            protected function createPDO(): \PDO
            {
                if ($this->factory) {
                    return ($this->factory)();
                }
                return parent::createPDO();
            }
        };

        $pdoWrapper->factory = function () use (&$attempt, $pdoMock) {
            $attempt++;
            if ($attempt < 3) {
                throw new \PDOException('SQLSTATE[HY000] [1045] ProxySQL Error: Access denied for user');
            }
            return $pdoMock;
        };

        $pdoWrapper->reconnect();

        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');
        $pdoProperty->setAccessible(true);

        $this->assertSame($pdoMock, $pdoProperty->getValue($pdoWrapper));
        $this->assertEquals(3, $attempt, 'Should have retried 3 times');
    }

    public function testReconnectThrowsAfterMaxRetries(): void
    {
        $pdoWrapper = new class ('', null, null, [], 3) extends PDO {
            public function __construct(
                string $dsn,
                ?string $username,
                ?string $password,
                array $config = [],
                int $retries = 3
            ) {
                $this->retries = $retries;
            }

            protected function createPDO(): \PDO
            {
                throw new \PDOException('SQLSTATE[HY000] [1045] ProxySQL Error: Access denied for user');
            }
        };

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('Access denied');
        $pdoWrapper->reconnect();
    }

    public function testReconnectDoesNotRetryNonTransientErrors(): void
    {
        $attempt = 0;

        $pdoWrapper = new class ('', null, null, [], 3) extends PDO {
            public ?\Closure $factory = null;

            public function __construct(
                string $dsn,
                ?string $username,
                ?string $password,
                array $config = [],
                int $retries = 3
            ) {
                $this->retries = $retries;
            }

            protected function createPDO(): \PDO
            {
                if ($this->factory) {
                    return ($this->factory)();
                }
                return parent::createPDO();
            }
        };

        $pdoWrapper->factory = function () use (&$attempt) {
            $attempt++;
            throw new \PDOException('SQLSTATE[42000] Syntax error');
        };

        try {
            $pdoWrapper->reconnect();
        } catch (\PDOException $e) {
            // expected
        }

        $this->assertEquals(1, $attempt, 'Should NOT retry non-transient errors');
    }

    public function testMethodCallForPrepare(): void
    {
        $dsn = 'sqlite::memory:';
        $pdoWrapper = new PDO($dsn, null, null);

        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');
        $pdoProperty->setAccessible(true);

        $pdoMock = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdoStatementMock = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdoMock->expects($this->once())
            ->method('prepare')
            ->with('SELECT * FROM table', [\PDO::ATTR_CURSOR => \PDO::CURSOR_FWDONLY])
            ->willReturn($pdoStatementMock);

        $pdoProperty->setValue($pdoWrapper, $pdoMock);

        $result = $pdoWrapper->prepare('SELECT * FROM table', [\PDO::ATTR_CURSOR => \PDO::CURSOR_FWDONLY]);

        $this->assertSame($pdoStatementMock, $result);
    }
}
