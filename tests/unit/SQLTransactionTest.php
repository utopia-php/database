<?php

namespace Tests\Unit;

use PDOException;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MySQL;

class SQLTransactionTest extends TestCase
{
    /**
     * A pooled connection (e.g. Swoole PDOProxy) can keep its own transaction
     * counter that survives a reconnect, so it reports an open transaction the
     * underlying connection no longer holds. The cleanup rollBack() then throws
     * "There is no active transaction". startTransaction() must swallow that and
     * still begin a fresh transaction instead of surfacing it as a failure.
     */
    public function testStartTransactionRecoversFromDesyncedRollback(): void
    {
        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();

        $pdo->method('inTransaction')->willReturn(true);
        $pdo->method('rollBack')->willThrowException(
            new PDOException('There is no active transaction')
        );
        $pdo->expects($this->once())
            ->method('beginTransaction')
            ->willReturn(true);

        $adapter = new MySQL($pdo);

        $this->assertTrue($adapter->startTransaction());
        $this->assertTrue($adapter->inTransaction());
    }
}
