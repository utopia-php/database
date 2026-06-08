<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use ReflectionClass;
use Utopia\Database\PDO;

class PDOTest extends TestCase
{
    /**
     * Create a PDO wrapper that uses an in-memory mock instead of a real driver.
     *
     * @return PDO
     */
    private function createMockPDOWrapper(): PDO
    {
        $pdoMock = $this->createMock(\PDO::class);

        return new class ($pdoMock) extends PDO {
            public function __construct(private \PDO $mock)
            {
                parent::__construct('mock:host=localhost', null, null);
            }

            protected function createPDO(): \PDO
            {
                return $this->mock;
            }
        };
    }

    public function testMethodCallIsForwardedToPDO(): void
    {
        $pdoWrapper = $this->createMockPDOWrapper();

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
            ->method('query')
            ->with('SELECT 1')
            ->willReturn($pdoStatementMock);

        $pdoProperty->setValue($pdoWrapper, $pdoMock);

        $result = $pdoWrapper->query('SELECT 1');

        $this->assertSame($pdoStatementMock, $result);
    }

    public function testLostConnectionRetriesCall(): void
    {
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

        $pdoWrapper = new class ($pdoMock) extends PDO {
            public bool $reconnectWasCalled = false;

            public function __construct(private \PDO $mock)
            {
                parent::__construct('mock:host=localhost', null, null);
            }

            protected function createPDO(): \PDO
            {
                return $this->mock;
            }

            public function reconnect(): void
            {
                $this->reconnectWasCalled = true;
                // Don't actually reconnect, keep the same mock
            }
        };

        $result = $pdoWrapper->query('SELECT 1');

        $this->assertTrue($pdoWrapper->reconnectWasCalled);
        $this->assertSame($pdoStatementMock, $result);
    }

    public function testNonLostConnectionExceptionIsRethrown(): void
    {
        $pdoWrapper = $this->createMockPDOWrapper();

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
        $callCount = 0;
        $pdoWrapper = new class ($callCount) extends PDO {
            public function __construct(private int &$callCounter)
            {
                parent::__construct('mock:host=localhost', null, null);
            }

            protected function createPDO(): \PDO
            {
                $this->callCounter++;
                $mock = new class () extends \PDO {
                    public function __construct()
                    {
                        // No-op, no real driver needed
                    }
                };
                return $mock;
            }
        };

        $reflection = new ReflectionClass($pdoWrapper);
        $pdoProperty = $reflection->getProperty('pdo');
        $pdoProperty->setAccessible(true);

        $oldPDO = $pdoProperty->getValue($pdoWrapper);
        $pdoWrapper->reconnect();
        $newPDO = $pdoProperty->getValue($pdoWrapper);

        $this->assertNotSame($oldPDO, $newPDO, "Reconnect should create a new PDO instance");
        $this->assertEquals(2, $callCount);
    }

    public function testMethodCallForPrepare(): void
    {
        $pdoWrapper = $this->createMockPDOWrapper();

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

    public function testConstructorRetriesOnTransientError(): void
    {
        $pdoWrapper = new class () extends PDO {
            public int $connectAttempts = 0;

            public function __construct()
            {
                parent::__construct('mock:host=localhost', null, null, [], 3);
            }

            protected function createPDO(): \PDO
            {
                $this->connectAttempts++;
                if ($this->connectAttempts < 3) {
                    throw new \PDOException(
                        'SQLSTATE[HY000] [1045] ProxySQL Error: Access denied for user \'test\'@\'10.0.0.1\' (using password: YES)'
                    );
                }
                return new class () extends \PDO {
                    public function __construct()
                    {
                    }
                };
            }
        };

        $this->assertEquals(3, $pdoWrapper->connectAttempts);
    }

    public function testConstructorThrowsAfterMaxRetries(): void
    {
        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('Access denied');

        new class () extends PDO {
            public function __construct()
            {
                parent::__construct('mock:host=localhost', null, null, [], 3);
            }

            protected function createPDO(): \PDO
            {
                throw new \PDOException(
                    'SQLSTATE[HY000] [1045] ProxySQL Error: Access denied for user \'test\'@\'10.0.0.1\' (using password: YES)'
                );
            }
        };
    }

    public function testReconnectRetriesOnTransientError(): void
    {
        $pdoWrapper = new class () extends PDO {
            public int $connectAttempts = 0;
            private int $reconnectFailures = 0;

            public function __construct()
            {
                parent::__construct('mock:host=localhost', null, null, [], 3);
            }

            protected function createPDO(): \PDO
            {
                $this->connectAttempts++;
                // First call succeeds (constructor), then fail twice on reconnect
                if ($this->connectAttempts > 1 && $this->reconnectFailures < 2) {
                    $this->reconnectFailures++;
                    throw new \PDOException(
                        'SQLSTATE[HY000] [1045] ProxySQL Error: Access denied for user \'test\'@\'10.0.0.1\' (using password: YES)'
                    );
                }
                return new class () extends \PDO {
                    public function __construct()
                    {
                    }
                };
            }
        };

        $this->assertEquals(1, $pdoWrapper->connectAttempts);

        $pdoWrapper->reconnect();

        // Constructor (1) + 2 failed reconnect attempts + 1 successful = 4
        $this->assertEquals(4, $pdoWrapper->connectAttempts);
    }

    public function testConstructorDoesNotRetryNonTransientError(): void
    {
        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('Some other error');

        new class () extends PDO {
            public function __construct()
            {
                parent::__construct('mock:host=localhost', null, null, [], 3);
            }

            protected function createPDO(): \PDO
            {
                throw new \PDOException('Some other error');
            }
        };
    }
}
