<?php

namespace Tests\Unit\Cache;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Cache;
use Utopia\Database\Cache\CacheInvalidator;
use Utopia\Database\Cache\CacheRegion;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Query;

class QueryCacheTest extends TestCase
{
    private QueryCache $queryCache;

    private Cache $cache;

    protected function setUp(): void
    {
        $this->cache = self::createStub(Cache::class);
        $this->queryCache = new QueryCache($this->cache);
    }

    public function testConstructorWithDefaults(): void
    {
        $cache = self::createStub(Cache::class);
        $queryCache = new QueryCache($cache);
        $this->assertTrue($queryCache->isEnabled('any_collection'));
    }

    public function testConstructorWithCustomName(): void
    {
        $cache = self::createStub(Cache::class);
        $queryCache = new QueryCache($cache, 'custom');
        $key = $queryCache->buildQueryKey('users', [], 'ns', null);
        $this->assertStringStartsWith('custom:', $key);
    }

    public function testSetRegionAndGetRegion(): void
    {
        $region = new CacheRegion(ttl: 600, enabled: false);
        $this->queryCache->setRegion('users', $region);
        $retrieved = $this->queryCache->getRegion('users');
        $this->assertSame($region, $retrieved);
    }

    public function testGetRegionReturnsDefaultForUnknownCollection(): void
    {
        $region = $this->queryCache->getRegion('unknown');
        $this->assertInstanceOf(CacheRegion::class, $region);
        $this->assertEquals(3600, $region->ttl);
        $this->assertTrue($region->enabled);
    }

    public function testBuildQueryKeyGeneratesConsistentKeys(): void
    {
        $queries = [Query::equal('status', ['active'])];
        $key1 = $this->queryCache->buildQueryKey('users', $queries, 'ns', 1);
        $key2 = $this->queryCache->buildQueryKey('users', $queries, 'ns', 1);
        $this->assertEquals($key1, $key2);
    }

    public function testBuildQueryKeyIncludesNamespaceAndTenant(): void
    {
        $key = $this->queryCache->buildQueryKey('users', [], 'myns', 42);
        $this->assertStringContainsString('myns', $key);
        $this->assertStringContainsString('42', $key);
    }

    public function testBuildQueryKeyDifferentQueriesProduceDifferentKeys(): void
    {
        $key1 = $this->queryCache->buildQueryKey('users', [Query::equal('a', [1])], 'ns', null);
        $key2 = $this->queryCache->buildQueryKey('users', [Query::equal('b', [2])], 'ns', null);
        $this->assertNotEquals($key1, $key2);
    }

    public function testBuildQueryKeyDifferentCollectionsProduceDifferentKeys(): void
    {
        $key1 = $this->queryCache->buildQueryKey('users', [], 'ns', null);
        $key2 = $this->queryCache->buildQueryKey('posts', [], 'ns', null);
        $this->assertNotEquals($key1, $key2);
    }

    public function testGetReturnsNullForCacheMiss(): void
    {
        $this->cache->method('load')->willReturn(false);
        $result = $this->queryCache->get('some-key');
        $this->assertNull($result);
    }

    public function testGetReturnsNullForNullData(): void
    {
        $this->cache->method('load')->willReturn(null);
        $result = $this->queryCache->get('some-key');
        $this->assertNull($result);
    }

    public function testGetReturnsDocumentArrayForCacheHit(): void
    {
        $this->cache->method('load')->willReturn([
            ['$id' => 'doc1', 'name' => 'Alice'],
            ['$id' => 'doc2', 'name' => 'Bob'],
        ]);

        $result = $this->queryCache->get('some-key');

        $this->assertNotNull($result);
        $this->assertCount(2, $result);
        $this->assertInstanceOf(Document::class, $result[0]);
        $this->assertEquals('doc1', $result[0]->getId());
    }

    public function testGetHandlesDocumentObjectsInCache(): void
    {
        $doc = new Document(['$id' => 'doc1', 'name' => 'Alice']);
        $this->cache->method('load')->willReturn([$doc]);

        $result = $this->queryCache->get('some-key');

        $this->assertNotNull($result);
        $this->assertCount(1, $result);
        $this->assertSame($doc, $result[0]);
    }

    public function testGetReturnsNullForNonArrayData(): void
    {
        $this->cache->method('load')->willReturn('not-an-array');
        $result = $this->queryCache->get('some-key');
        $this->assertNull($result);
    }

    public function testSetSerializesDocuments(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);

        $docs = [
            new Document(['$id' => 'doc1', 'name' => 'Alice']),
        ];

        $cache->expects($this->once())
            ->method('save')
            ->with(
                'cache-key',
                $this->callback(function (array $data) {
                    return \is_array($data[0]) && $data[0]['$id'] === 'doc1';
                })
            );

        $queryCache->set('cache-key', $docs);
    }

    public function testInvalidateCollectionCallsPurge(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);

        $cache->expects($this->once())
            ->method('purge')
            ->with($this->stringContains('users'));

        $queryCache->invalidateCollection('users');
    }

    public function testIsEnabledReturnsTrueByDefault(): void
    {
        $this->assertTrue($this->queryCache->isEnabled('any'));
    }

    public function testIsEnabledReturnsFalseWhenRegionDisabled(): void
    {
        $this->queryCache->setRegion('users', new CacheRegion(enabled: false));
        $this->assertFalse($this->queryCache->isEnabled('users'));
    }

    public function testFlushDelegatesToCacheFlush(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);

        $cache->expects($this->once())
            ->method('flush');

        $queryCache->flush();
    }

    public function testCacheRegionDefaults(): void
    {
        $region = new CacheRegion();
        $this->assertEquals(3600, $region->ttl);
        $this->assertTrue($region->enabled);
    }

    public function testCacheRegionCustomValues(): void
    {
        $region = new CacheRegion(ttl: 120, enabled: false);
        $this->assertEquals(120, $region->ttl);
        $this->assertFalse($region->enabled);
    }

    public function testCacheInvalidatorInvalidatesOnDocumentCreate(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $doc = new Document(['$id' => 'doc1', '$collection' => 'users']);

        $cache->expects($this->once())->method('purge');
        $invalidator->handle(Event::DocumentCreate, $doc);
    }

    public function testCacheInvalidatorInvalidatesOnDocumentUpdate(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $doc = new Document(['$id' => 'doc1', '$collection' => 'posts']);

        $cache->expects($this->once())->method('purge');
        $invalidator->handle(Event::DocumentUpdate, $doc);
    }

    public function testCacheInvalidatorInvalidatesOnDocumentDelete(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $doc = new Document(['$id' => 'doc1', '$collection' => 'users']);

        $cache->expects($this->once())->method('purge');
        $invalidator->handle(Event::DocumentDelete, $doc);
    }

    public function testCacheInvalidatorIgnoresNonWriteEvents(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $doc = new Document(['$id' => 'doc1', '$collection' => 'users']);

        $cache->expects($this->never())->method('purge');
        $invalidator->handle(Event::DocumentFind, $doc);
    }

    public function testCacheInvalidatorExtractsCollectionFromDocument(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $cache->expects($this->once())
            ->method('purge')
            ->with($this->stringContains('orders'));

        $doc = new Document(['$id' => 'doc1', '$collection' => 'orders']);
        $invalidator->handle(Event::DocumentCreate, $doc);
    }

    public function testCacheInvalidatorHandlesStringData(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $cache->expects($this->once())
            ->method('purge')
            ->with($this->stringContains('products'));

        $invalidator->handle(Event::DocumentCreate, 'products');
    }

    public function testCacheInvalidatorIgnoresEmptyCollection(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $doc = new Document(['$id' => 'doc1']);

        $cache->expects($this->never())->method('purge');
        $invalidator->handle(Event::DocumentCreate, $doc);
    }
}
