<?php

namespace Tests\Unit\Adapter;

use Closure;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Event;
use Utopia\Database\Hook\Transform;
use Utopia\Database\Profiler;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;

final class PoolSyncTest extends TestCase
{
    private const int TENANT = 1;

    private const int OTHER_TENANT = 2;

    private const string NAMESPACE = 'before';

    private const string OTHER_NAMESPACE = 'after';

    private const int TIMEOUT = 100;

    private const int OTHER_TIMEOUT = 250;

    private const int EVENT_TIMEOUT = 50;

    private static ?Transform $first = null;

    private static ?Transform $second = null;

    private static ?Profiler $profiler = null;

    private static ?Profiler $otherProfiler = null;

    /**
     * @param  Closure(Pool): void  $change
     * @param  Closure(SyncSnapshot): mixed  $observe
     */
    #[DataProvider('changes')]
    public function testABorrowedCallSeesTheChange(Closure $change, Closure $observe, mixed $expected): void
    {
        [$pool, $connection] = $this->pool();

        $pool->ping();
        $this->assertNotSame($expected, $observe($connection->last()), 'The change must differ from the starting state');

        $change($pool);
        $pool->ping();

        $this->assertSame($expected, $observe($connection->last()));
    }

    /**
     * @param  Closure(Pool): void  $change
     * @param  Closure(SyncSnapshot): mixed  $observe
     */
    #[DataProvider('changes')]
    public function testAPinnedCallSeesTheChange(Closure $change, Closure $observe, mixed $expected): void
    {
        [$pool, $connection] = $this->pool();

        $pool->withTransaction(function () use ($pool, $change): void {
            $pool->ping();
            $change($pool);
            $pool->ping();
        });

        $this->assertCount(2, $connection->snapshots);
        $this->assertNotSame($expected, $observe($connection->snapshots[0]), 'The change must differ from the starting state');
        $this->assertSame($expected, $observe($connection->snapshots[1]));
    }

    /**
     * @param  Closure(Pool): void  $change
     * @param  Closure(SyncSnapshot): mixed  $observe
     */
    #[DataProvider('changes')]
    public function testACallAfterTheTransactionSeesAChangeMadeInIt(Closure $change, Closure $observe, mixed $expected): void
    {
        [$pool, $connection] = $this->pool();

        $pool->withTransaction(function () use ($pool, $change): void {
            $pool->ping();
            $change($pool);
        });
        $pool->ping();

        $this->assertSame($expected, $observe($connection->last()));
    }

    /**
     * @param  Closure(Pool): void  $change
     * @param  Closure(SyncSnapshot): mixed  $observe
     */
    #[DataProvider('changes')]
    public function testTheNextTransactionSeesAChangeMadeBeforeIt(Closure $change, Closure $observe, mixed $expected): void
    {
        [$pool, $connection] = $this->pool();

        $pool->withTransaction(fn (): bool => $pool->ping());
        $change($pool);
        $pool->withTransaction(fn (): bool => $pool->ping());

        $this->assertSame($expected, $observe($connection->last()));
    }

    /**
     * @param  Closure(SyncRecordingMemory): void  $mutation
     */
    #[DataProvider('mutations')]
    public function testAConnectionsOwnChangeDoesNotReachTheNextBorrowedCall(Closure $mutation): void
    {
        [$pool, $connection] = $this->pool();

        $connection->mutateOnNextPing($mutation);
        $pool->ping();
        $pool->ping();

        $this->assertCount(2, $connection->snapshots);
        $this->assertSame(\get_object_vars($connection->snapshots[0]), \get_object_vars($connection->snapshots[1]));
    }

    /**
     * @param  Closure(SyncRecordingMemory): void  $mutation
     */
    #[DataProvider('mutations')]
    public function testAConnectionsOwnChangeDoesNotReachTheNextPinnedCall(Closure $mutation): void
    {
        [$pool, $connection] = $this->pool();

        $pool->withTransaction(function () use ($pool, $connection, $mutation): void {
            $connection->mutateOnNextPing($mutation);
            $pool->ping();
            $pool->ping();
        });

        $this->assertCount(2, $connection->snapshots);
        $this->assertSame(\get_object_vars($connection->snapshots[0]), \get_object_vars($connection->snapshots[1]));
    }

    public function testABorrowedCallLeavesNoProfilerOnTheConnection(): void
    {
        [$pool, $connection] = $this->pool();

        $pool->ping();

        $this->assertSame(self::profiler(), $connection->last()->profiler);
        $this->assertNull($connection->getProfiler());
    }

    /**
     * @return iterable<string, array{Closure(Pool): void, Closure(SyncSnapshot): mixed, mixed}>
     */
    public static function changes(): iterable
    {
        $metadata = static fn (SyncSnapshot $snapshot): mixed => $snapshot->metadata;
        $transforms = static fn (SyncSnapshot $snapshot): mixed => $snapshot->transforms;
        $timeouts = static fn (SyncSnapshot $snapshot): mixed => $snapshot->timeouts;

        yield 'metadata added' => [
            static fn (Pool $pool): mixed => $pool->setMetadata('trace', 'b'),
            $metadata,
            ['request' => 'a', 'trace' => 'b'],
        ];
        yield 'metadata replaced' => [
            static fn (Pool $pool): mixed => $pool->setMetadata('request', 'c'),
            $metadata,
            ['request' => 'c'],
        ];
        yield 'metadata reset' => [
            static fn (Pool $pool) => $pool->resetMetadata(),
            $metadata,
            [],
        ];
        yield 'transform added' => [
            static fn (Pool $pool): mixed => $pool->addTransform('second', self::second()),
            $transforms,
            ['first' => self::first(), 'second' => self::second()],
        ];
        yield 'transform replaced' => [
            static fn (Pool $pool): mixed => $pool->addTransform('first', self::second()),
            $transforms,
            ['first' => self::second()],
        ];
        yield 'transform removed' => [
            static fn (Pool $pool): mixed => $pool->removeTransform('first'),
            $transforms,
            [],
        ];
        yield 'schemaless' => [
            static fn (Pool $pool): mixed => $pool->setSchemaless(true),
            static fn (SyncSnapshot $snapshot): mixed => $snapshot->schemaless,
            true,
        ];
        yield 'profiler replaced' => [
            static fn (Pool $pool): mixed => $pool->setProfiler(self::otherProfiler()),
            static fn (SyncSnapshot $snapshot): mixed => $snapshot->profiler,
            self::otherProfiler(),
        ];
        yield 'profiler removed' => [
            static fn (Pool $pool): mixed => $pool->setProfiler(null),
            static fn (SyncSnapshot $snapshot): mixed => $snapshot->profiler,
            null,
        ];
        yield 'tenant' => [
            static fn (Pool $pool): mixed => $pool->setTenant(self::OTHER_TENANT),
            static fn (SyncSnapshot $snapshot): mixed => $snapshot->tenant,
            self::OTHER_TENANT,
        ];
        yield 'namespace' => [
            static fn (Pool $pool): mixed => $pool->setNamespace(self::OTHER_NAMESPACE),
            static fn (SyncSnapshot $snapshot): mixed => $snapshot->namespace,
            self::OTHER_NAMESPACE,
        ];
        yield 'timeout replaced' => [
            static fn (Pool $pool) => $pool->setTimeout(self::OTHER_TIMEOUT),
            $timeouts,
            [Event::All->value => self::OTHER_TIMEOUT],
        ];
        yield 'timeout per event' => [
            static fn (Pool $pool) => $pool->setTimeout(self::EVENT_TIMEOUT, Event::DocumentFind),
            $timeouts,
            [Event::DocumentFind->value => self::EVENT_TIMEOUT, Event::All->value => self::TIMEOUT],
        ];
        yield 'timeout cleared' => [
            static fn (Pool $pool) => $pool->clearTimeout(),
            $timeouts,
            [],
        ];
    }

    /**
     * @return iterable<string, array{Closure(SyncRecordingMemory): void}>
     */
    public static function mutations(): iterable
    {
        yield 'state added' => [static function (SyncRecordingMemory $connection): void {
            $connection->setMetadata('leak', true);
            $connection->addTransform('leak', self::second());
            $connection->setSchemaless(true);
            $connection->setProfiler(self::otherProfiler());
            $connection->setTimeout(self::OTHER_TIMEOUT);
        }];
        yield 'state removed' => [static function (SyncRecordingMemory $connection): void {
            $connection->resetMetadata();
            $connection->resetTransforms();
            $connection->setProfiler(null);
            $connection->clearTimeout();
        }];
        yield 'state replaced' => [static function (SyncRecordingMemory $connection): void {
            $connection->setMetadata('request', 'leak');
            $connection->addTransform('first', self::second());
        }];
    }

    /**
     * @return array{Pool, SyncRecordingMemory}
     */
    private function pool(): array
    {
        $connection = new SyncRecordingMemory();
        $pool = new Pool(new UtopiaPool(new Stack(), 'memory', 1, static fn (): SyncRecordingMemory => $connection, timeout: 0.0));
        $pool->setAuthorization(new Authorization());
        $pool->setNamespace(self::NAMESPACE);
        $pool->setTenant(self::TENANT);
        $pool->setMetadata('request', 'a');
        $pool->addTransform('first', self::first());
        $pool->setSchemaless(false);
        $pool->setProfiler(self::profiler());
        $pool->setTimeout(self::TIMEOUT);

        return [$pool, $connection];
    }

    private static function first(): Transform
    {
        return self::$first ??= self::transform();
    }

    private static function second(): Transform
    {
        return self::$second ??= self::transform();
    }

    private static function profiler(): Profiler
    {
        return self::$profiler ??= new Profiler();
    }

    private static function otherProfiler(): Profiler
    {
        return self::$otherProfiler ??= new Profiler();
    }

    private static function transform(): Transform
    {
        return new class () implements Transform {
            #[\Override]
            public function transform(Event $event, string $query): string
            {
                return $query;
            }
        };
    }
}
