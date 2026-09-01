<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Tenancy;
use Utopia\Database\Storage;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class PoolTest extends TestCase
{
    public function testDelegateReplacesStatefulWriteHookOnReusedAdapter(): void
    {
        $adapter = new Memory();
        $adapter->addWriteHook(new Tenancy('old'));
        $pool = $this->createPool($adapter);

        $hook = new Tenancy('new');
        $pool->addWriteHook($hook);

        $this->assertTrue($pool->ping());
        $this->assertSame([$hook], $adapter->getWriteHooks());
        $this->assertSame([Storage::TENANT => 'new'], $adapter->getTenantHook()?->decorateRow([]));
    }

    public function testTransactionReplacesStatefulWriteHookOnReusedAdapter(): void
    {
        $adapter = new Memory();
        $adapter->addWriteHook(new Permissions());
        $adapter->addWriteHook(new Tenancy('old'));
        $pool = $this->createPool($adapter);

        $hook = new Tenancy('new');
        $pool->addWriteHook($hook);

        $pool->withTransaction(static fn (): string => 'committed');
        $this->assertSame([$hook], $adapter->getWriteHooks());
        $this->assertSame($hook, $adapter->getTenantHook());
    }

    public function testDelegateRemovesWriteHookRemovedFromPool(): void
    {
        $adapter = new Memory();
        $pool = $this->createPool($adapter);

        $hook = new Permissions();
        $pool->addWriteHook($hook);
        $this->assertTrue($pool->ping());
        $this->assertSame([$hook], $adapter->getWriteHooks());

        $pool->removeWriteHook(Permissions::class);
        $this->assertTrue($pool->ping());
        $this->assertSame([], $adapter->getWriteHooks());
    }

    public function testTransactionRemovesWriteHookRemovedFromPool(): void
    {
        $adapter = new Memory();
        $pool = $this->createPool($adapter);

        $hook = new Permissions();
        $pool->addWriteHook($hook);
        $pool->withTransaction(static fn (): string => 'committed');
        $this->assertSame([$hook], $adapter->getWriteHooks());

        $pool->removeWriteHook(Permissions::class);
        $pool->withTransaction(static fn (): string => 'committed');
        $this->assertSame([], $adapter->getWriteHooks());
    }

    public function testTransactionPropagatesWriteHooksToPinnedAdapter(): void
    {
        /** @var Adapter&MockObject $adapter */
        $adapter = $this->createMock(Adapter::class);
        $adapter->method('getWriteHooks')->willReturn([]);
        $adapter->method('withTransaction')->willReturnCallback(
            static fn (callable $callback): mixed => $callback(),
        );

        $hook = new Permissions();
        $adapter->expects($this->once())
            ->method('addWriteHook')
            ->with($this->identicalTo($hook))
            ->willReturnSelf();

        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($adapter),
        );

        $pool = new Pool($connections);
        $pool->setAuthorization(new Authorization());
        $pool->addWriteHook($hook);

        $pool->withTransaction(static fn (): string => 'committed');
    }

    public function testPinnedAdapterResyncsTenantAndDatabaseBeforeDelegatedCall(): void
    {
        $adapter = new Memory();
        $adapter->setDatabase('old_db');
        $adapter->setNamespace('old_ns');
        $adapter->setTenant(1);

        $pool = $this->createPool($adapter);
        $pool->setDatabase('old_db');
        $pool->setNamespace('old_ns');
        $pool->setTenant(1);

        $pool->withTransaction(function () use ($pool, $adapter): void {
            $pool->setDatabase('new_db');
            $pool->setNamespace('new_ns');
            $pool->setTenant(2);

            $this->assertTrue($pool->ping());
            $this->assertSame('new_db', $adapter->getDatabase());
            $this->assertSame('new_ns', $adapter->getNamespace());
            $this->assertSame(2, $adapter->getTenant());
        });
    }

    public function testMemoryPoolPingDoesNotRequireTimeouts(): void
    {
        $pool = $this->createPool(new Memory());

        $this->assertTrue($pool->ping());
    }

    public function testDefinedAttributesSupportPropagatesAcrossBorrowedAdapters(): void
    {
        $first = new class () extends Mongo {
            public function __construct()
            {
            }
        };
        $second = new class () extends Mongo {
            public function __construct()
            {
            }
        };
        $adapters = [$first, $second];

        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static function (callable $callback) use (&$adapters): mixed {
                return $callback(\array_shift($adapters));
            },
        );

        $pool = new Pool($connections);
        $pool->setAuthorization(new Authorization());
        $pool->setSupportForAttributes(false);

        $this->assertFalse($pool->supports(Capability::DefinedAttributes));
        $this->assertFalse($second->supports(Capability::DefinedAttributes));
    }

    /**
     * A timeout is adapter state, so setting one must not check a connection out
     * — a handle built against an unreachable backing would otherwise fail before
     * the caller had issued a single query. The adapter's capabilities are first
     * known when the timeout is applied, which is where the refusal belongs.
     */
    public function testMemoryPoolSetTimeoutRefusesWhenTheTimeoutWouldBeApplied(): void
    {
        $pool = $this->createPool(new Memory());

        $pool->setTimeout(1000);
        $this->assertSame(1000, $pool->getTimeout());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support timeouts');

        $pool->getDriver();
    }

    public function testMissingFeatureThrows(): void
    {
        $pool = $this->createPool(new Memory());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support upserts');

        $pool->upsertDocuments(new Document(), 'id', []);
    }

    /**
     * Configuring a handle must not open a connection, or an unreachable
     * backing fails a caller that has issued no query yet. Pool::setTimeout()
     * holds the value without checking out, but Database::setTimeout() guarded
     * itself with hasFeature(), and on a pool that guard is a delegated call --
     * which dials.
     */
    public function testSettingATimeoutOnAPooledHandleDoesNotOpenAConnection(): void
    {
        $dials = 0;

        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static function (callable $callback) use (&$dials): mixed {
                $dials++;

                return $callback(new Memory());
            },
        );

        $pool = new Pool($connections);
        $pool->setAuthorization(new Authorization());

        (new Database($pool, new Cache(new NoCache())))->setTimeout(300000);

        $this->assertSame(0, $dials, 'Building a handle must not check a connection out');
        $this->assertSame(300000, $pool->getTimeout(), 'The handle must still hold the timeout it was given');
    }

    /**
     * syncPinnedTimeouts() reaches whatever the handle has pinned. Upstream that
     * is one adapter on the object, but a handle that pins per coroutine keeps
     * its pins elsewhere, and reading the property directly reached none of
     * them -- a timeout raised inside a transaction then arrived at the next
     * checkout, long after the body it was meant to bound had run.
     */
    public function testTimeoutRaisedWhilePinnedReachesTheSubclassPin(): void
    {
        $pinned = new TimeoutRecordingAdapter();

        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback(new TimeoutRecordingAdapter()),
        );

        $pool = new ElsewherePinnedPool($connections);
        $pool->setAuthorization(new Authorization());
        $pool->pinElsewhere($pinned);

        $pool->setTimeout(300000);

        $this->assertSame(300000, $pinned->getTimeout(), 'The connection the open transaction is running on must get the new bound');
    }

    private function createPool(Adapter $adapter): Pool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($adapter),
        );

        $pool = new Pool($connections);
        $pool->setAuthorization(new Authorization());

        return $pool;
    }
}

final class TimeoutRecordingAdapter extends Memory implements Feature\Timeouts
{
    public function setTimeout(int $milliseconds, Event $event = Event::All): void
    {
        $this->setTimeoutState($milliseconds, $event);
    }

    public function clearTimeout(Event $event = Event::All): void
    {
        $this->clearTimeoutState($event);
    }
}

final class ElsewherePinnedPool extends Pool
{
    private ?Adapter $elsewhere = null;

    public function pinElsewhere(Adapter $adapter): void
    {
        $this->elsewhere = $adapter;
    }

    protected function pin(): ?Adapter
    {
        return $this->elsewhere;
    }
}
