<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Document;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Adapter\Stack;
use Utopia\Pools\Pool as UtopiaPool;

class PoolTest extends TestCase
{
    private function createStubAdapter(callable $getDocumentCallback): Adapter
    {
        $adapter = $this->getMockForAbstractClass(Adapter::class);
        $adapter->method('getDocument')->willReturnCallback($getDocumentCallback);
        return $adapter;
    }

    private function createPoolAdapter(Adapter $adapter): Pool
    {
        $pool = new UtopiaPool(new Stack(), 'test', 1, function () use ($adapter) {
            return $adapter;
        });
        $poolAdapter = new Pool($pool);
        $poolAdapter->setAuthorization(new Authorization());
        return $poolAdapter;
    }

    /**
     * Test that Pool::delegate retries on connection errors (e.g. ProxySQL timeout).
     */
    public function testDelegateRetriesOnConnectionError(): void
    {
        $callCount = 0;
        $adapter = $this->createStubAdapter(function () use (&$callCount) {
            $callCount++;
            if ($callCount === 1) {
                throw new \PDOException(
                    'SQLSTATE[HY000]: General error: 9001 Max connect timeout reached while reaching hostgroup 10 after 10000ms'
                );
            }
            return new Document(['$id' => 'test']);
        });

        $poolAdapter = $this->createPoolAdapter($adapter);
        $collection = new Document(['$id' => 'test_collection']);
        $result = $poolAdapter->getDocument($collection, 'test');

        $this->assertEquals('test', $result->getId());
        $this->assertEquals(2, $callCount, 'getDocument should have been called twice (1 failure + 1 success)');
    }

    /**
     * Test that non-connection PDOExceptions are NOT retried.
     */
    public function testDelegateDoesNotRetryNonConnectionErrors(): void
    {
        $callCount = 0;
        $adapter = $this->createStubAdapter(function () use (&$callCount) {
            $callCount++;
            throw new \PDOException('SQLSTATE[42S02]: Base table or view not found');
        });

        $poolAdapter = $this->createPoolAdapter($adapter);

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('Base table or view not found');

        $collection = new Document(['$id' => 'test_collection']);
        try {
            $poolAdapter->getDocument($collection, 'test');
        } finally {
            $this->assertEquals(1, $callCount, 'getDocument should only be called once for non-connection errors');
        }
    }

    /**
     * Test that connection errors eventually throw after max retries.
     */
    public function testDelegateThrowsAfterMaxRetries(): void
    {
        $callCount = 0;
        $adapter = $this->createStubAdapter(function () use (&$callCount) {
            $callCount++;
            throw new \PDOException(
                'SQLSTATE[HY000]: General error: 9001 Max connect timeout reached while reaching hostgroup 10 after 10000ms'
            );
        });

        $poolAdapter = $this->createPoolAdapter($adapter);

        try {
            $collection = new Document(['$id' => 'test_collection']);
            $poolAdapter->getDocument($collection, 'test');
            $this->fail('Expected PDOException was not thrown');
        } catch (\PDOException $e) {
            $this->assertStringContainsString('Max connect timeout reached', $e->getMessage());
            $this->assertGreaterThan(1, $callCount, 'Should have retried at least once before giving up');
        }
    }
}
