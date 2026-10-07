<?php

namespace Tests\Unit\Transactions;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Exception\Timeout;
use Utopia\Database\Exception\Unconfirmed;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;

/**
 * A timed-out statement or a commit that could not be confirmed may already have taken effect, so running the
 * transaction again could apply its writes twice.
 */
final class NoRetryTest extends TestCase
{
    private const string MEMORY = 'memory';

    private const string POOL = 'pool';

    /**
     * @return array<string, array{string, Throwable}>
     */
    public static function failures(): array
    {
        $failures = [
            'timeout' => new Timeout('Query timed out'),
            'unconfirmed' => new Unconfirmed('Commit could not be confirmed'),
        ];

        $cases = [];
        foreach ([self::MEMORY, self::POOL] as $entry) {
            foreach ($failures as $name => $failure) {
                $cases["{$entry} {$name}"] = [$entry, $failure];
            }
        }

        return $cases;
    }

    #[DataProvider('failures')]
    public function testTheCallbackRunsOnceAndTheFailurePropagates(string $entry, Throwable $failure): void
    {
        $adapter = $this->adapter($entry);
        $runs = 0;
        $thrown = null;

        try {
            $adapter->withTransaction(function () use ($failure, &$runs): never {
                $runs++;

                throw $failure;
            });
        } catch (Throwable $caught) {
            $thrown = $caught;
        }

        $this->assertSame($failure, $thrown);
        $this->assertSame(1, $runs);
        $this->assertFalse($adapter->inTransaction());
    }

    private function adapter(string $entry): Adapter
    {
        $memory = new Memory();
        $memory->setAuthorization(new Authorization());

        if ($entry === self::MEMORY) {
            return $memory;
        }

        $pool = new Pool(new UtopiaPool(new Stack(), self::MEMORY, 1, static fn (): Memory => $memory, timeout: 0.0));
        $pool->setAuthorization(new Authorization());

        return $pool;
    }
}
