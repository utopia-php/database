<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Connection;

class ConnectionTest extends TestCase
{
    public function testAccessDeniedIsRecognizedAsConnectionError(): void
    {
        $e = new \PDOException(
            'SQLSTATE[HY000] [1045] ProxySQL Error: Access denied for user \'appwrite\'@\'10.0.2.101\' (using password: YES)'
        );
        $this->assertTrue(Connection::hasError($e));
    }

    public function testMaxConnectTimeoutIsRecognizedAsConnectionError(): void
    {
        $e = new \PDOException('Max connect timeout reached');
        $this->assertTrue(Connection::hasError($e));
    }

    public function testUnrelatedErrorIsNotConnectionError(): void
    {
        $e = new \PDOException('SQLSTATE[42S02]: Base table or view not found');
        $this->assertFalse(Connection::hasError($e));
    }

    public function testAccessDeniedWithoutProxySQLIsRecognizedAsConnectionError(): void
    {
        $e = new \PDOException(
            'SQLSTATE[HY000] [1045] Access denied for user \'root\'@\'localhost\' (using password: NO)'
        );
        $this->assertTrue(Connection::hasError($e));
    }
}
