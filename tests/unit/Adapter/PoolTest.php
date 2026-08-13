<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class PoolTest extends TestCase
{
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
}
