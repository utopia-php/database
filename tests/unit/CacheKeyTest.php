<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Query;

class CacheKeyTest extends TestCase
{
    /**
     * @param array<string, array{encode: callable, decode: callable}> $instanceFilters
     */
    private function createDatabase(array $instanceFilters = [], string $database = 'test'): Database
    {
        $adapter = $this->createMock(Adapter::class);
        $adapter->method('getSupportForHostname')->willReturn(false);
        $adapter->method('getTenant')->willReturn(null);
        $adapter->method('getNamespace')->willReturn('test');
        $adapter->method('getDatabase')->willReturn($database);

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

        [, , $hashA] = $db->getCacheKeys('col', 'doc1', ['name', 'email']);
        [, , $hashB] = $db->getCacheKeys('col', 'doc1', ['email', 'name']);

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

    public function testDifferentFindQueriesProduceDifferentCacheKeys(): void
    {
        $db = $this->createDatabase();

        [, $fieldA] = $db->getCachedFindKeys('col', [Query::equal('status', ['active'])]);
        [, $fieldB] = $db->getCachedFindKeys('col', [Query::equal('status', ['paused'])]);

        $this->assertNotEquals($fieldA, $fieldB);
    }

    public function testCallerFindCacheKeyIgnoresQueryVariation(): void
    {
        $db = $this->createDatabase();

        [, $fieldA] = $db->getCachedFindKeys('col', [Query::equal('status', ['active'])], 'domain-key');
        [, $fieldB] = $db->getCachedFindKeys('col', [Query::equal('status', ['paused'])], 'domain-key');

        $this->assertEquals($fieldA, $fieldB);
    }

    public function testDifferentFindDatabasesProduceDifferentCacheKeys(): void
    {
        $dbPlatform = $this->createDatabase(database: 'platform');
        $dbProject = $this->createDatabase(database: 'project');

        [, $fieldA] = $dbPlatform->getCachedFindKeys('col', [Query::limit(10)]);
        [, $fieldB] = $dbProject->getCachedFindKeys('col', [Query::limit(10)]);

        $this->assertNotEquals($fieldA, $fieldB);
    }

    public function testDifferentFindSchemasProduceDifferentCacheKeys(): void
    {
        $db = $this->createDatabase();

        $collectionA = new Document([
            '$id' => 'col',
            'attributes' => [
                new Document(['$id' => 'name', 'type' => Database::VAR_STRING]),
            ],
            'indexes' => [],
        ]);
        $collectionB = new Document([
            '$id' => 'col',
            'attributes' => [
                new Document(['$id' => 'name', 'type' => Database::VAR_STRING]),
                new Document(['$id' => 'status', 'type' => Database::VAR_STRING]),
            ],
            'indexes' => [],
        ]);

        [, $fieldA] = $db->getCachedFindKeys('col', [Query::limit(10)], collection: $collectionA);
        [, $fieldB] = $db->getCachedFindKeys('col', [Query::limit(10)], collection: $collectionB);

        $this->assertNotEquals($fieldA, $fieldB);
    }

    public function testFindRelationshipModeProducesDifferentCacheKeys(): void
    {
        $db = $this->createDatabase();

        [, $fieldA] = $db->getCachedFindKeys('col', [Query::limit(10)]);
        [, $fieldB] = $db->skipRelationships(fn () => $db->getCachedFindKeys('col', [Query::limit(10)]));

        $this->assertNotEquals($fieldA, $fieldB);
    }

    public function testFindCursorDocumentValuesProduceDifferentCacheKeys(): void
    {
        $db = $this->createDatabase();

        [, $fieldA] = $db->getCachedFindKeys('col', [
            Query::orderAsc('name'),
            Query::cursorAfter(new Document([
                '$id' => 'cursor',
                'name' => 'alpha',
            ])),
        ]);
        [, $fieldB] = $db->getCachedFindKeys('col', [
            Query::orderAsc('name'),
            Query::cursorAfter(new Document([
                '$id' => 'cursor',
                'name' => 'beta',
            ])),
        ]);

        $this->assertNotEquals($fieldA, $fieldB);
    }

    public function testParseHostname(): void
    {
        $hostname = 'database_db_nyc3_self_hosted_0_0';

        $adapter = $this->createMock(Adapter::class);
        $adapter->method('getSupportForHostname')->willReturn(true);
        $adapter->method('getHostname')->willReturn($hostname);
        $adapter->method('getTenant')->willReturn(999);
        $adapter->method('getSharedTables')->willReturn(true);
        $adapter->method('getNamespace')->willReturn('_ns');

        $db = new Database($adapter, new Cache(new None()), []);

        /**
         * Check DSN is parsed correctly
         */
        [$collectionKey, $documentKey] = $db->getCacheKeys('users');
        $this->assertEquals('default-cache-database_db_nyc3_self_hosted_0_0:_ns:999:collection:users', $collectionKey);
        $this->assertEquals('', $documentKey);

        $db->setGlobalCollections(['users']);
        $this->assertEquals(['users'], $db->getGlobalCollections());

        /**
         * Check that tenant 999 exists
         */

        [$collectionKey, $documentKey] = $db->getCacheKeys(Database::METADATA, 'audit');
        $this->assertEquals('default-cache-database_db_nyc3_self_hosted_0_0:_ns:999:collection:_metadata', $collectionKey);
        $this->assertEquals('default-cache-database_db_nyc3_self_hosted_0_0:_ns:999:collection:_metadata:audit', $documentKey);

        /**
         * Check that tenant 999 was removed
         */
        [$collectionKey, $documentKey] = $db->getCacheKeys(Database::METADATA, 'users');
        $this->assertEquals('default-cache-database_db_nyc3_self_hosted_0_0:_ns::collection:_metadata', $collectionKey);
        $this->assertEquals('default-cache-database_db_nyc3_self_hosted_0_0:_ns::collection:_metadata:users', $documentKey);
    }
}
