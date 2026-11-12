<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\ReadWritePool;
use Utopia\Database\Database;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class PoolHostnameTest extends TestCase
{
    public function testAPoolWithoutAHostnameAnswersWithThePooledAdaptersHostname(): void
    {
        $this->assertSame('db-1', $this->pool(new HostnameSQLite('db-1'))->hostname());
    }

    public function testAHostnameSetOnThePoolWinsOverThePooledAdaptersHostname(): void
    {
        $pool = $this->pool(new HostnameSQLite('db-1'));

        $this->assertSame($pool, $pool->setHostname('edge-1'));
        $this->assertSame('edge-1', $pool->hostname());
    }

    public function testAPoolClearedOfItsHostnameDelegatesAgain(): void
    {
        $pool = $this->pool(new HostnameSQLite('db-1'));

        $pool->setHostname('edge-1');
        $pool->setHostname('');

        $this->assertSame('db-1', $pool->hostname());
    }

    public function testADatabaseOverAPoolNamesThePoolsHostname(): void
    {
        $pool = $this->pool(new HostnameSQLite('db-1'));
        $database = new Database($pool, new Cache(new None()));
        $database->setAuthorization(new Authorization());

        $this->assertSame('db-1', $database->getHostname());

        $pool->setHostname('edge-1');

        $this->assertSame('edge-1', $database->getHostname());
    }

    public function testAReadWritePoolFollowsTheHostnameSetAfterItAnswered(): void
    {
        $pool = new ReadWritePool(
            $this->connections(new HostnameSQLite('db-1')),
            $this->connections(new HostnameSQLite('replica-1')),
        );
        $pool->setAuthorization(new Authorization());

        $this->assertSame('db-1', $pool->hostname());

        $pool->setHostname('edge-1');
        $this->assertSame('edge-1', $pool->hostname());

        $pool->setHostname('');
        $this->assertSame('db-1', $pool->hostname());
    }

    public function testThePoolHandsBackTheConnectionsItWasBuiltWith(): void
    {
        $connections = $this->connections(new HostnameSQLite('db-1'));

        $this->assertSame($connections, (new Pool($connections))->getPool());
    }

    private function pool(Adapter $connection): Pool
    {
        $pool = new Pool($this->connections($connection));
        $pool->setAuthorization(new Authorization());

        return $pool;
    }

    /**
     * @return UtopiaPool<Adapter>&Stub
     */
    private function connections(Adapter $connection): UtopiaPool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(static fn (callable $callback): mixed => $callback($connection));

        return $connections;
    }
}
