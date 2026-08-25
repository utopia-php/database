<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Database;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;

/**
 * A connection records a timeout and applies it to the statements it builds
 * afterwards; it never asks the server for one. The pooled adapter must
 * therefore hold the timeout itself and apply it to each connection as it is
 * checked out, rather than checking one out to carry the value.
 */
class PoolTimeoutTest extends TestCase
{
    public function testSetTimeoutDoesNotOpenAConnection(): void
    {
        $opened = 0;
        $adapter = new Pool(new UtopiaPool(new Stack(), 'unreachable', 1, function () use (&$opened) {
            $opened++;

            throw new \RuntimeException('SQLSTATE[HY000] [2002] Connection refused');
        }, timeout: 0.0));

        $adapter->setTimeout(300000);

        $this->assertSame(0, $opened);
        $this->assertSame(300000, $adapter->getTimeout());
    }

    /**
     * The pool outlives the handle that configured it, so a connection carries
     * whatever the last handle to hold it asked for. A timeout applied once, at
     * the moment it was set, leaves every later checkout unbounded.
     */
    public function testTimeoutIsAppliedToEveryCheckout(): void
    {
        $connection = new TimeoutRecordingMemory();
        $adapter = new Pool(new UtopiaPool(new Stack(), 'memory', 1, fn () => $connection, timeout: 0.0));

        $adapter->setAuthorization(new Authorization());
        $adapter->setTimeout(300000);
        $adapter->getSupportForTimeouts();

        $this->assertSame([Database::EVENT_ALL => 300000], $connection->timeouts);

        $connection->timeouts = [];
        $adapter->getSupportForTimeouts();

        $this->assertSame([Database::EVENT_ALL => 300000], $connection->timeouts);
    }

    public function testClearedTimeoutIsClearedOnEveryCheckout(): void
    {
        $connection = new TimeoutRecordingMemory();
        $adapter = new Pool(new UtopiaPool(new Stack(), 'memory', 1, fn () => $connection, timeout: 0.0));

        $adapter->setAuthorization(new Authorization());
        $adapter->setTimeout(300000);
        $adapter->getSupportForTimeouts();
        $adapter->clearTimeout(Database::EVENT_ALL);
        $adapter->getSupportForTimeouts();

        $this->assertSame([], $connection->timeouts);
        $this->assertSame(0, $adapter->getTimeout());
    }

    public function testPerEventTimeoutReachesTheConnection(): void
    {
        $connection = new TimeoutRecordingMemory();
        $adapter = new Pool(new UtopiaPool(new Stack(), 'memory', 1, fn () => $connection, timeout: 0.0));

        $adapter->setAuthorization(new Authorization());
        $adapter->setTimeout(300000);
        $adapter->setTimeout(5000, Database::EVENT_DOCUMENT_READ);
        $adapter->getSupportForTimeouts();

        $this->assertSame([
            Database::EVENT_ALL => 300000,
            Database::EVENT_DOCUMENT_READ => 5000,
        ], $connection->timeouts);
    }

    /**
     * A caller whose own default is zero is asking for no timeout, which is
     * what a factory building a handle without one passes. It reaches no
     * connection as a timeout the connection would refuse.
     */
    public function testZeroIsHeldAsNoTimeout(): void
    {
        $connection = new TimeoutRecordingMemory();
        $adapter = new Pool(new UtopiaPool(new Stack(), 'memory', 1, fn () => $connection, timeout: 0.0));

        $adapter->setAuthorization(new Authorization());
        $adapter->setTimeout(0);
        $adapter->getSupportForTimeouts();

        $this->assertSame([], $connection->timeouts);
        $this->assertSame(0, $adapter->getTimeout());
    }
}

/**
 * Stands in for a connection: records what the pooled adapter applied to it,
 * the way a concrete adapter records a timeout for the statements it builds.
 */
class TimeoutRecordingMemory extends Memory
{
    /**
     * @var array<string, int>
     */
    public array $timeouts = [];

    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void
    {
        $this->timeouts[$event] = $milliseconds;
        $this->timeout = $milliseconds;
    }

    public function clearTimeout(string $event): void
    {
        unset($this->timeouts[$event]);

        parent::clearTimeout($event);
    }
}
