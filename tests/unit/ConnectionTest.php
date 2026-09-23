<?php

namespace Tests\Unit;

use PDOException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionClassConstant;
use ReflectionProperty;
use Swoole\Database\DetectsLostConnections;
use Utopia\Database\Connection;

final class ConnectionTest extends TestCase
{
    /**
     * @return iterable<string, array{string, int}>
     */
    public static function lostConnectionErrors(): iterable
    {
        yield 'MySQL disconnected an idle client' => ['HY000', 4031];
        yield 'MySQL server has gone away' => ['HY000', 2006];
        yield 'MySQL connection lost during a query' => ['HY000', 2013];
        yield 'MySQL cannot connect' => ['HY000', 2002];
        yield 'MySQL server shutting down' => ['08S01', 1053];
        yield 'connection exception class' => ['08006', 7];
        yield 'PostgreSQL administrator shutdown' => ['57P01', 7];
        yield 'PostgreSQL crash shutdown' => ['57P02', 7];
        yield 'PostgreSQL cannot connect now' => ['57P03', 7];
        yield 'PostgreSQL database dropped' => ['57P04', 7];
        yield 'PostgreSQL idle session timeout' => ['57P05', 7];
    }

    /**
     * The message names no symptom any message list knows, so only the driver's
     * error code can classify it.
     */
    #[DataProvider('lostConnectionErrors')]
    public function testDriverErrorCodesOfALostConnectionAreDetected(string $state, int $code): void
    {
        $this->assertTrue(Connection::hasError($this->driverError($state, $code)));
    }

    /**
     * @return iterable<string, array{string, int}>
     */
    public static function otherErrors(): iterable
    {
        yield 'MariaDB statement timeout' => ['70100', 1969];
        yield 'MySQL statement timeout' => ['HY000', 3024];
        yield 'PostgreSQL statement timeout' => ['57014', 7];
        yield 'duplicate key' => ['23000', 1062];
        yield 'syntax error' => ['42000', 1064];
        yield 'deadlock' => ['40001', 1213];
    }

    #[DataProvider('otherErrors')]
    public function testDriverErrorCodesOfALiveConnectionAreNotDetected(string $state, int $code): void
    {
        $this->assertFalse(Connection::hasError($this->driverError($state, $code)));
    }

    public function testConnectFailureCarryingTheDriverCodeIsDetected(): void
    {
        $error = new PDOException('SQLSTATE[HY000] [2002] refused', 2002);
        $error->errorInfo = ['HY000', 2002, 'refused'];

        $this->assertTrue(Connection::hasError($error));
    }

    /**
     * Without Swoole's library the local messages are all there is, so they must
     * cover every message Swoole recognises for detection to stay the same.
     */
    public function testLocalMessagesCoverSwoolesLostConnectionMessages(): void
    {
        if (! \class_exists(DetectsLostConnections::class)) {
            $this->markTestSkipped('Swoole\'s library is not loaded, so its messages cannot be compared');
        }

        $swoole = (new ReflectionClassConstant(DetectsLostConnections::class, 'ERROR_MESSAGES'))->getValue();
        $local = (new ReflectionProperty(Connection::class, 'errors'))->getValue();
        $this->assertIsArray($swoole);
        $this->assertIsArray($local);

        $missing = \array_filter($swoole, fn (mixed $message): bool => ! \in_array($message, $local, true));

        $this->assertSame([], \array_values($missing), 'Messages Swoole detects that the local list misses');
    }

    private function driverError(string $state, int $code): PDOException
    {
        $error = new PDOException("SQLSTATE[{$state}]: driver code {$code}");
        $error->errorInfo = [$state, $code, "driver code {$code}"];

        return $error;
    }
}
