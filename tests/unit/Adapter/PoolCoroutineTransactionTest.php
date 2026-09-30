<?php

namespace Tests\Unit\Adapter;

use Closure;
use PHPUnit\Framework\TestCase;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;

use function Swoole\Coroutine\run;

final class PoolCoroutineTransactionTest extends TestCase
{
    private const int TENANT = 1;

    private const int CHILD_TENANT = 2;

    protected function setUp(): void
    {
        if (! \extension_loaded('swoole')) {
            $this->markTestSkipped('ext-swoole is required for coroutines sharing a pool');
        }
    }

    public function testASiblingRunsOutsideAnotherCoroutinesTransaction(): void
    {
        /** @var list<PingRecordingMemory> $connections */
        $connections = [];
        $pool = $this->pool($connections, 2);
        $seen = [];

        $this->inCoroutine(function () use ($pool, &$connections, &$seen): void {
            $entered = new Channel(1);
            $released = new Channel(1);

            Coroutine::create(function () use ($pool, &$seen, $entered, $released): void {
                $pool->withTransaction(function () use ($pool, &$seen, $entered, $released): void {
                    $seen['owner'] = $pool->inTransaction();
                    $pool->ping();
                    $entered->push(true);
                    $released->pop();
                });
            });

            $entered->pop();
            $sibling = null;
            $siblingDone = new Channel(1);
            Coroutine::create(function () use ($pool, &$seen, &$sibling, $siblingDone): void {
                $sibling = Coroutine::getCid();
                $seen['sibling'] = $pool->inTransaction();
                $pool->ping();
                $seen['siblingNested'] = $pool->withTransaction(fn (): bool => $pool->inTransaction());
                $siblingDone->push(true);
            });
            $siblingDone->pop();
            $released->push(true);

            $pinned = $connections[0];
            $seen['siblingOnPinned'] = \in_array($sibling, \array_column($pinned->pings, 'coroutine'), true);
        });

        $this->assertSame(
            ['owner' => true, 'sibling' => false, 'siblingNested' => true, 'siblingOnPinned' => false],
            $seen,
        );
        $this->assertFalse($pool->inTransaction());
    }

    public function testACoroutineStartedInsideATransactionRunsInIt(): void
    {
        /** @var list<PingRecordingMemory> $connections */
        $connections = [];
        $pool = $this->pool($connections, 2);
        $seen = [];

        $this->inCoroutine(function () use ($pool, &$seen): void {
            $pool->withTransaction(function () use ($pool, &$seen): void {
                $done = new Channel(1);
                Coroutine::create(function () use ($pool, &$seen, $done): void {
                    $seen['child'] = $pool->inTransaction();
                    $done->push(true);
                });
                $done->pop();
            });

            $seen['after'] = $pool->inTransaction();
        });

        $this->assertSame(['child' => true, 'after' => false], $seen);
    }

    public function testCoroutinesOnThePinnedConnectionKeepTheirOwnTenant(): void
    {
        /** @var list<PingRecordingMemory> $connections */
        $connections = [];
        $pool = $this->pool($connections, 1);

        $this->inCoroutine(function () use ($pool, &$connections): void {
            $pool->withTenant(self::TENANT, function () use ($pool, &$connections): void {
                $pool->withTransaction(function () use ($pool, &$connections): void {
                    $paused = new Channel(1);
                    $resumed = new Channel(1);
                    $done = new Channel(1);

                    $connections[0]->pauseNextPing(static function () use ($paused, $resumed): void {
                        $paused->push(true);
                        $resumed->pop();
                    });

                    Coroutine::create(function () use ($pool, $done): void {
                        $pool->withTenant(self::CHILD_TENANT, fn (): bool => $pool->ping());
                        $done->push(true);
                    });

                    $paused->pop();
                    $pool->ping();
                    $resumed->push(true);
                    $done->pop();
                });
            });
        });

        $this->assertCount(1, $connections);
        $this->assertSame(
            [
                [self::TENANT, self::TENANT],
                [self::CHILD_TENANT, self::CHILD_TENANT],
            ],
            \array_map(static fn (array $ping): array => [$ping['before'], $ping['after']], $connections[0]->pings),
        );
    }

    /**
     * @param  list<PingRecordingMemory>  $connections
     */
    private function pool(array &$connections, int $size): Pool
    {
        $pool = new Pool(new UtopiaPool(new Stack(), 'memory', $size, function () use (&$connections): PingRecordingMemory {
            $connection = new PingRecordingMemory();
            $connections[] = $connection;

            return $connection;
        }, timeout: 0.0));
        $pool->setAuthorization(new Authorization());

        return $pool;
    }

    private function inCoroutine(Closure $test): void
    {
        $hookFlags = Runtime::getHookFlags();

        try {
            run($test);
        } finally {
            Runtime::setHookFlags($hookFlags);
        }
    }
}
