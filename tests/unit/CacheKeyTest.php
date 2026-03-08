<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Query;

class CacheKeyTest extends TestCase
{
    /**
     * @param array<string, array{encode: callable, decode: callable}> $instanceFilters
     */
    private function createDatabase(array $instanceFilters = []): Database
    {
        $adapter = $this->createMock(Adapter::class);
        $adapter->method('getSupportForHostname')->willReturn(false);
        $adapter->method('getTenant')->willReturn(null);
        $adapter->method('getNamespace')->willReturn('test');

        return new Database($adapter, new Cache(new None()), $instanceFilters);
    }

    private function getHashKey(Database $db, string $collection = 'col', string $docId = 'doc1'): string
    {
        [, , $hashKey] = $db->getCacheKeys($collection, $docId);
        return $hashKey;
    }

    public function testSameConfigProducesSameCacheKey(): void
    {
        $db1 = $this->createDatabase();
        $db2 = $this->createDatabase();

        $this->assertEquals($this->getHashKey($db1), $this->getHashKey($db2));
    }

    public function testDifferentSelectsProduceDifferentCacheKeys(): void
    {
        $db = $this->createDatabase();

        [, , $hashA] = $db->getCacheKeys('col', 'doc1', ['name']);
        [, , $hashB] = $db->getCacheKeys('col', 'doc1', ['email']);

        $this->assertNotEquals($hashA, $hashB);
    }

    public function testSelectOrderDoesNotAffectCacheKey(): void
    {
        $db = $this->createDatabase();


        [, , $hashA] = $db->getCacheKeys('col', 'doc1', [
            Query::select('email'),
            Query::select('name'),
        ]);

        [, , $hashB] = $db->getCacheKeys('col', 'doc1', [
            Query::select('name'),
            Query::select('email'),
        ]);

        $this->assertEquals($hashA, $hashB);
    }

    public function testInstanceFilterOverrideProducesDifferentCacheKey(): void
    {
        $noop = function (mixed $value) {
            return $value;
        };

        $dbDefault = $this->createDatabase();
        $dbOverride = $this->createDatabase([
            'json' => [
                'encode' => $noop,
                'decode' => $noop,
            ],
        ]);

        $this->assertNotEquals(
            $this->getHashKey($dbDefault),
            $this->getHashKey($dbOverride)
        );
    }

    public function testDifferentInstanceFilterCallablesProduceDifferentCacheKeys(): void
    {
        $noopA = function (mixed $value) {
            return $value;
        };
        $noopB = function (mixed $value) {
            return $value;
        };

        $dbA = $this->createDatabase([
            'myFilter' => [
                'encode' => $noopA,
                'decode' => $noopA,
            ],
        ]);
        $dbB = $this->createDatabase([
            'myFilter' => [
                'encode' => $noopB,
                'decode' => $noopB,
            ],
        ]);

        $this->assertNotEquals(
            $this->getHashKey($dbA),
            $this->getHashKey($dbB)
        );
    }

    public function testDisabledFiltersProduceDifferentCacheKey(): void
    {
        $db = $this->createDatabase();

        $hashEnabled = $this->getHashKey($db);

        $hashDisabled = $db->skipFilters(function () use ($db) {
            return $this->getHashKey($db);
        }, ['json']);

        $this->assertNotEquals($hashEnabled, $hashDisabled);
    }

    public function testFiltersDisabledEntirelyProducesDifferentCacheKey(): void
    {
        $db = $this->createDatabase();

        $hashEnabled = $this->getHashKey($db);

        $db->disableFilters();
        $hashDisabled = $this->getHashKey($db);
        $db->enableFilters();

        $this->assertNotEquals($hashEnabled, $hashDisabled);
    }
}
