<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Document;
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

        $this->assertSame('committed', $pool->withTransaction(static fn (): string => 'committed'));
        $this->assertSame([Permissions::class, Tenancy::class], \array_map(
            static fn ($childHook): string => $childHook::class,
            $adapter->getWriteHooks(),
        ));
        $this->assertSame($hook, $adapter->getTenantHook());
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

        $this->assertSame('committed', $pool->withTransaction(static fn (): string => 'committed'));
    }

    public function testMemoryPoolPingDoesNotRequireTimeouts(): void
    {
        $pool = $this->createPool(new Memory());

        $this->assertTrue($pool->ping());
    }

    public function testMemoryPoolSetTimeoutThrows(): void
    {
        $pool = $this->createPool(new Memory());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support timeouts');

        $pool->setTimeout(1000);
    }

    public function testMissingFeatureThrows(): void
    {
        $pool = $this->createPool(new Memory());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support upserts');

        $pool->upsertDocuments(new Document(), 'id', []);
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
