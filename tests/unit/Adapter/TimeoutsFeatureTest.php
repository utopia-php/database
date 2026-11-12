<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Database;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class TimeoutsFeatureTest extends TestCase
{
    private int $checkouts = 0;

    public function testAPoolOverMariaDBHasTheFeatureWithoutCheckingOutAConnection(): void
    {
        $pool = $this->pool(new MariaDB(self::createStub(PDO::class)));

        $this->assertTrue($pool->hasFeature(Feature\Timeouts::class));
        $this->assertSame(0, $this->checkouts);
    }

    public function testAPoolHoldsItsTimeoutsWhateverItPools(): void
    {
        $pool = $this->pool(new Memory());

        $this->assertTrue($pool->hasFeature(Feature\Timeouts::class));
        $this->assertFalse((new Memory())->hasFeature(Feature\Timeouts::class));
    }

    public function testAnEventTimeoutOverridesTheGlobalOneUntilItIsCleared(): void
    {
        $adapter = new MariaDB(self::createStub(PDO::class));
        $adapter->setTimeout(1_000);
        $adapter->setTimeout(250, Event::DocumentFind);

        $this->assertSame(1_000, $adapter->getTimeout());
        $this->assertSame(250, $adapter->getTimeout(Event::DocumentFind));
        $this->assertSame(1_000, $adapter->getTimeout(Event::DocumentCreate));

        $adapter->clearTimeout(Event::DocumentFind);

        $this->assertSame(1_000, $adapter->getTimeout(Event::DocumentFind));
    }

    public function testClearingTheGlobalTimeoutClearsEveryEvent(): void
    {
        $adapter = new MariaDB(self::createStub(PDO::class));
        $adapter->setTimeout(1_000);
        $adapter->setTimeout(250, Event::DocumentFind);

        $adapter->clearTimeout();

        $this->assertSame(0, $adapter->getTimeout());
        $this->assertSame(0, $adapter->getTimeout(Event::DocumentFind));
    }

    public function testAnAdapterWithoutTimeoutsHasNoTimeoutUntilOneIsSet(): void
    {
        $adapter = new MariaDB(self::createStub(PDO::class));

        $this->assertSame(0, $adapter->getTimeout());
        $this->assertSame(0, $adapter->getTimeout(Event::DocumentFind));
    }

    public function testThePoolReportsTheTimeoutItWasGivenWithoutCheckingOut(): void
    {
        $pool = $this->pool(new MariaDB(self::createStub(PDO::class)));
        $database = $this->database($pool);

        $database->setTimeout(500, Event::DocumentFind);
        $database->setTimeout(2_000);

        $this->assertSame(2_000, $pool->getTimeout());
        $this->assertSame(500, $pool->getTimeout(Event::DocumentFind));
        $this->assertSame(0, $this->checkouts);
    }

    public function testADatabaseOverAnAdapterWithoutTimeoutsRefusesOne(): void
    {
        $database = $this->database(new Memory());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support timeouts');

        $database->setTimeout(500);
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->setAuthorization(new Authorization());

        return $database;
    }

    private function pool(Adapter $connection): Pool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(function (callable $callback) use ($connection): mixed {
            $this->checkouts++;

            return $callback($connection);
        });

        $pool = new Pool($connections);
        $pool->setAuthorization(new Authorization());

        return $pool;
    }
}
