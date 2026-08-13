<?php

namespace Tests\Unit\Cache;

use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;
use Utopia\Database\Cache\CacheInvalidator;
use Utopia\Database\Cache\CacheRegion;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Database;
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
        $differentNamespace = $this->queryCache->buildQueryKey('users', [], 'other', 42);
        $differentTenant = $this->queryCache->buildQueryKey('users', [], 'myns', 43);

        $this->assertNotSame($key, $differentNamespace);
        $this->assertNotSame($key, $differentTenant);
    }

    public function testBuildQueryKeyPreservesTenantType(): void
    {
        $integer = $this->queryCache->buildQueryKey('users', [], 'ns', 1);
        $string = $this->queryCache->buildQueryKey('users', [], 'ns', '1');

        $this->assertNotSame($integer, $string);
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
        $this->cache->method('load')->willReturn(
            'epoch',
            [
                'version' => 1,
                'documents' => [
                    ['$id' => 'doc1', 'name' => 'Alice'],
                    ['$id' => 'doc2', 'name' => 'Bob'],
                ],
            ],
        );

        $result = $this->queryCache->get('some-key');

        $this->assertNotNull($result);
        $this->assertCount(2, $result);
        $this->assertInstanceOf(Document::class, $result[0]);
        $this->assertEquals('doc1', $result[0]->getId());
    }

    public function testGetHandlesDocumentObjectsInCache(): void
    {
        $doc = new Document(['$id' => 'doc1', 'name' => 'Alice']);
        $this->cache->method('load')->willReturn(
            'epoch',
            [
                'version' => 1,
                'documents' => [$doc],
            ],
        );

        $result = $this->queryCache->get('some-key');

        $this->assertNotNull($result);
        $this->assertCount(1, $result);
        $this->assertSame($doc, $result[0]);
    }

    public function testGetPropagatesMalformedPayloadPurgeFailure(): void
    {
        $this->cache->method('load')->willReturn('epoch', 'not-an-array');

        $this->expectException(\RuntimeException::class);
        $this->queryCache->get('some-key');
    }

    public function testGetUsesCollectionRegionTtlAndQueryHash(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $queryCache->setRegion('users', new CacheRegion(ttl: 120));
        $key = $queryCache->buildQueryKey('users', [Query::limit(10)], 'ns', null);
        [, $hash] = \explode('#', $key, 2);

        $cache->expects($this->exactly(2))
            ->method('load')
            ->willReturnCallback(function (string $cacheKey, int $ttl) use ($hash): string|false {
                $this->assertSame(120, $ttl);

                return match ($cacheKey) {
                    'default:qcache:users#epoch' => 'epoch',
                    'default:qcache:users#epoch:'.$hash => false,
                    default => throw new \LogicException("Unexpected cache key '{$cacheKey}'"),
                };
            });

        $this->assertNull($queryCache->get($key));
    }

    public function testGetPurgesMalformedPayload(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $key = $queryCache->buildQueryKey('users', [], 'ns', null);
        [, $hash] = \explode('#', $key, 2);

        $cache->method('load')->willReturnOnConsecutiveCalls(
            'epoch',
            ['version' => 1, 'documents' => ['invalid']],
        );
        $cache->expects($this->once())
            ->method('purge')
            ->with('default:qcache:users#epoch:'.$hash)
            ->willReturn(true);

        $this->assertNull($queryCache->get($key));
    }

    public function testSetSerializesDocuments(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $cache->method('load')->willReturn('epoch');
        $cache->method('getGeneration')->willReturn('0');

        $docs = [
            new Document(['$id' => 'doc1', 'name' => 'Alice']),
        ];

        $cache->expects($this->once())
            ->method('saveWithLease')
            ->with(
                'cache-key#epoch:',
                $this->callback(function (array $data): bool {
                    $documents = $data['documents'] ?? null;

                    return ($data['version'] ?? null) === 1
                        && \is_array($documents)
                        && \is_array($documents[0] ?? null)
                        && ($documents[0]['$id'] ?? null) === 'doc1';
                }),
                '',
                '0',
            );

        $queryCache->set('cache-key', $docs);
    }

    public function testInvalidateCollectionCallsPurge(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $cache->method('load')->willReturn('epoch');

        $cache->expects($this->once())
            ->method('purge')
            ->with('default:qcache:users#epoch')
            ->willReturn(true);
        $cache->expects($this->once())
            ->method('save')
            ->with('default:qcache:users#epoch', $this->isString())
            ->willReturnArgument(1);

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
            ->method('flush')
            ->willReturn(true);

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
        $this->configureInvalidationCache($cache);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $doc = new Document(['$id' => 'doc1', '$collection' => 'users']);

        $cache->expects($this->once())->method('purge')->willReturn(true);
        $invalidator->handle(Event::DocumentCreate, $doc);
    }

    public function testCacheInvalidatorInvalidatesOnDocumentUpdate(): void
    {
        $cache = $this->createMock(Cache::class);
        $this->configureInvalidationCache($cache);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $doc = new Document(['$id' => 'doc1', '$collection' => 'posts']);

        $cache->expects($this->once())->method('purge')->willReturn(true);
        $invalidator->handle(Event::DocumentUpdate, $doc);
    }

    public function testCacheInvalidatorInvalidatesOnDocumentDelete(): void
    {
        $cache = $this->createMock(Cache::class);
        $this->configureInvalidationCache($cache);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $doc = new Document(['$id' => 'doc1', '$collection' => 'users']);

        $cache->expects($this->once())->method('purge')->willReturn(true);
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
        $this->configureInvalidationCache($cache);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $cache->expects($this->once())
            ->method('purge')
            ->with($this->stringContains('orders'))
            ->willReturn(true);

        $doc = new Document(['$id' => 'doc1', '$collection' => 'orders']);
        $invalidator->handle(Event::DocumentCreate, $doc);
    }

    public function testCacheInvalidatorHandlesStringData(): void
    {
        $cache = $this->createMock(Cache::class);
        $this->configureInvalidationCache($cache);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $cache->expects($this->once())
            ->method('purge')
            ->with($this->stringContains('products'))
            ->willReturn(true);

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

    public function testCacheInvalidatorInvalidatesBothRelationshipCollections(): void
    {
        $cache = $this->createMock(Cache::class);
        $this->configureInvalidationCache($cache);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $purged = [];
        $cache->expects($this->exactly(2))
            ->method('purge')
            ->willReturnCallback(function (string $key) use (&$purged): bool {
                $purged[] = $key;

                return true;
            });

        $invalidator->handle(Event::AttributeCreate, new Document([
            '$collection' => 'posts',
            'options' => [
                'relatedCollection' => 'authors',
            ],
        ]));

        $this->assertSame([
            'default:qcache:posts#epoch',
            'default:qcache:authors#epoch',
        ], $purged);
    }

    public function testCacheInvalidatorUsesCollectionIdentityForCollectionMutations(): void
    {
        $cache = $this->createMock(Cache::class);
        $this->configureInvalidationCache($cache);
        $queryCache = new QueryCache($cache);
        $invalidator = new CacheInvalidator($queryCache);

        $cache->expects($this->once())
            ->method('purge')
            ->with('default:qcache:users#epoch')
            ->willReturn(true);

        $invalidator->handle(Event::CollectionUpdate, new Document([
            '$id' => 'users',
            '$collection' => Database::METADATA,
        ]));
    }

    public function testMemoryAdapterKeepsPhysicalVariantsIsolated(): void
    {
        $queryCache = new QueryCache(new Cache(new Memory()));
        $firstKey = $queryCache->buildQueryKey('users', [['limit' => 1]], 'ns', null, 'role:user-a');
        $secondKey = $queryCache->buildQueryKey('users', [['limit' => 2]], 'ns', null, 'role:user-b');

        $firstGeneration = $queryCache->getGeneration($firstKey);
        $secondGeneration = $queryCache->getGeneration($secondKey);

        $this->assertTrue($queryCache->set($firstKey, [new Document(['$id' => 'private-a'])], $firstGeneration));
        $this->assertTrue($queryCache->set($secondKey, [new Document(['$id' => 'private-b'])], $secondGeneration));
        $first = $queryCache->get($firstKey);
        $second = $queryCache->get($secondKey);
        $this->assertNotNull($first);
        $this->assertNotNull($second);
        $this->assertSame('private-a', $first[0]->getId());
        $this->assertSame('private-b', $second[0]->getId());
    }

    public function testInvalidationPropagatesPurgeFailure(): void
    {
        $cache = $this->createMock(Cache::class);
        $cache->method('load')->willReturn('epoch');
        $cache->expects($this->once())
            ->method('purge')
            ->with('default:qcache:users#epoch')
            ->willReturn(false);

        $this->expectException(\RuntimeException::class);
        (new QueryCache($cache))->invalidateCollection('users');
    }

    private function configureInvalidationCache(Cache&MockObject $cache): void
    {
        $cache->method('load')->willReturn('epoch');
        $cache->method('save')->willReturnCallback(
            static fn (string $key, array|string $data): array|string => $data,
        );
    }
}
