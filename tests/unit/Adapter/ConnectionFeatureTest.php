<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class ConnectionFeatureTest extends TestCase
{
    private int $checkouts = 0;

    public function testADatabaseWithoutAConnectionIsAlwaysReachableAndNamesNoConnection(): void
    {
        $database = $this->database(new Memory());

        $this->assertTrue($database->ping());
        $database->reconnect();
        $this->assertNull($database->getConnectionId());
        $this->assertNull($database->getHostname());
    }

    public function testADatabaseForwardsToTheAdaptersConnection(): void
    {
        $database = $this->database(new HostnameSQLite('db-1'));

        $this->assertTrue($database->ping());
        $this->assertSame('db-1', $database->getHostname());
        $this->assertNotSame('', $database->getConnectionId());
    }

    public function testAReconnectedConnectionKeepsAnswering(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        $id = $database->getConnectionId();

        $database->reconnect();

        $this->assertTrue($database->ping());
        $this->assertSame($id, $database->getConnectionId());
    }

    public function testAPoolAnswersForTheConnectionOfWhatItPools(): void
    {
        $connected = $this->pool(new HostnameSQLite('db-1'));
        $unconnected = $this->pool(new Memory());

        $this->assertTrue($connected->hasFeature(Feature\Connection::class));
        $this->assertFalse($unconnected->hasFeature(Feature\Connection::class));
        $this->assertSame('db-1', $this->database($connected)->getHostname());
        $this->assertNull($this->database($unconnected)->getHostname());
    }

    public function testADatabaseOverAPoolWithoutAConnectionPingsWithoutCallingIt(): void
    {
        $database = $this->database($this->pool(new Memory()));

        $this->assertTrue($database->ping());
        $database->reconnect();
        $this->assertNull($database->getConnectionId());
    }

    public function testAPoolRefusesAConnectionCallItsAdapterCannotServe(): void
    {
        $pool = $this->pool(new Memory());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support connections');

        $pool->ping();
    }

    public function testTheConnectionQuestionIsAskedOncePerPool(): void
    {
        $connections = $this->connections(new HostnameSQLite('db-1'));
        $this->assertTrue($this->handle($connections)->hasFeature(Feature\Connection::class));

        $this->checkouts = 0;

        $this->assertTrue($this->handle($connections)->hasFeature(Feature\Connection::class));
        $this->assertSame(0, $this->checkouts);
    }

    public function testCacheKeysNameTheHostOfAConnectedAdapter(): void
    {
        $first = $this->database(new HostnameSQLite('db-1'));
        $second = $this->database(new HostnameSQLite('db-2'));

        $this->assertNotSame($first->getCacheKeys('posts', 'post'), $second->getCacheKeys('posts', 'post'));
        $this->assertNotSame($first->getQueryCacheKey('posts'), $second->getQueryCacheKey('posts'));
    }

    public function testCacheKeysOfAdaptersWithoutAConnectionDoNotDependOnAHost(): void
    {
        $first = $this->database(new Memory());
        $second = $this->database(new Memory());
        $second->getAdapter()->setHostname('db-2');

        $this->assertSame($first->getCacheKeys('posts', 'post'), $second->getCacheKeys('posts', 'post'));
        $this->assertSame($first->getQueryCacheKey('posts'), $second->getQueryCacheKey('posts'));
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->setAuthorization(new Authorization());

        return $database;
    }

    private function pool(Adapter $connection): Pool
    {
        return $this->handle($this->connections($connection));
    }

    /**
     * @param  UtopiaPool<Adapter>  $connections
     */
    private function handle(UtopiaPool $connections): Pool
    {
        $pool = new Pool($connections);
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
        $connections->method('use')->willReturnCallback(function (callable $callback) use ($connection): mixed {
            $this->checkouts++;

            return $callback($connection);
        });

        return $connections;
    }
}
