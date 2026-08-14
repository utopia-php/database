<?php

namespace Tests\Unit\Cache;

use Closure;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter as CacheAdapter;
use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;
use Utopia\Cache\Feature\Leasable;
use Utopia\Database\Cache\Invalidator;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Cache\Region;
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
        $region = new Region(ttl: 600, enabled: false);
        $this->queryCache->setRegion('users', $region);
        $retrieved = $this->queryCache->getRegion('users');
        $this->assertSame($region, $retrieved);
    }

    public function testGetRegionReturnsDefaultForUnknownCollection(): void
    {
        $region = $this->queryCache->getRegion('unknown');
        $this->assertInstanceOf(Region::class, $region);
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
            'active:epoch',
            [
                'version' => 1,
                'documents' => [
                    ['$id' => 'doc1', 'name' => 'Alice'],
                    ['$id' => 'doc2', 'name' => 'Bob'],
                ],
            ],
            'active:epoch',
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
            'active:epoch',
            [
                'version' => 1,
                'documents' => [$doc],
            ],
            'active:epoch',
        );

        $result = $this->queryCache->get('some-key');

        $this->assertNotNull($result);
        $this->assertCount(1, $result);
        $this->assertSame($doc, $result[0]);
    }

    public function testGetPropagatesMalformedPayloadPurgeFailure(): void
    {
        $this->cache->method('load')->willReturn('active:epoch', 'not-an-array', 'active:epoch');

        $this->expectException(\RuntimeException::class);
        $this->queryCache->get('some-key');
    }

    public function testGetUsesCollectionRegionTtlAndQueryHash(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $queryCache->setRegion('users', new Region(ttl: 120));
        $key = $queryCache->buildQueryKey('users', [Query::limit(10)], 'ns', null);
        [, $hash] = \explode('#', $key, 2);

        $cache->expects($this->exactly(2))
            ->method('load')
            ->willReturnCallback(function (string $cacheKey, int $ttl) use ($hash): string|false {
                $this->assertSame(120, $ttl);

                return match ($cacheKey) {
                    'default:qcache:users#epoch' => 'active:epoch',
                    'default:qcache:users#active:epoch:'.$hash => false,
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
            'active:epoch',
            ['version' => 1, 'documents' => ['invalid']],
            'active:epoch',
        );
        $cache->expects($this->once())
            ->method('purge')
            ->with('default:qcache:users#active:epoch:'.$hash)
            ->willReturn(true);

        $this->assertNull($queryCache->get($key));
    }

    public function testSetSerializesDocuments(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $cache->method('load')->willReturn('active:epoch');
        $cache->method('getGeneration')->willReturn('0');

        $docs = [
            new Document(['$id' => 'doc1', 'name' => 'Alice']),
        ];

        $cache->expects($this->once())
            ->method('saveWithLease')
            ->with(
                'cache-key#active:epoch:',
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
        $cache = new InvalidationCache();
        $queryCache = new QueryCache($cache);

        $queryCache->invalidateCollection('users');

        $this->assertSame(1, $cache->getPurges('default:qcache:users#epoch'));
    }

    public function testIsEnabledReturnsTrueByDefault(): void
    {
        $this->assertTrue($this->queryCache->isEnabled('any'));
    }

    public function testIsEnabledReturnsFalseWhenRegionDisabled(): void
    {
        $this->queryCache->setRegion('users', new Region(enabled: false));
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

    public function testRegionDefaults(): void
    {
        $region = new Region();
        $this->assertEquals(3600, $region->ttl);
        $this->assertTrue($region->enabled);
    }

    public function testRegionCustomValues(): void
    {
        $region = new Region(ttl: 120, enabled: false);
        $this->assertEquals(120, $region->ttl);
        $this->assertFalse($region->enabled);
    }

    public function testInvalidatorInvalidatesOnDocumentCreate(): void
    {
        $cache = new InvalidationCache();
        $queryCache = new QueryCache($cache);
        $invalidator = new Invalidator($queryCache);

        $doc = new Document(['$id' => 'doc1', '$collection' => 'users']);

        $invalidator->handle(Event::DocumentCreate, $doc);
        $this->assertSame(1, $cache->getPurges('default:qcache:users#epoch'));
    }

    public function testInvalidatorInvalidatesOnDocumentUpdate(): void
    {
        $cache = new InvalidationCache();
        $queryCache = new QueryCache($cache);
        $invalidator = new Invalidator($queryCache);

        $doc = new Document(['$id' => 'doc1', '$collection' => 'posts']);

        $invalidator->handle(Event::DocumentUpdate, $doc);
        $this->assertSame(1, $cache->getPurges('default:qcache:posts#epoch'));
    }

    public function testInvalidatorInvalidatesOnDocumentDelete(): void
    {
        $cache = new InvalidationCache();
        $queryCache = new QueryCache($cache);
        $invalidator = new Invalidator($queryCache);

        $doc = new Document(['$id' => 'doc1', '$collection' => 'users']);

        $invalidator->handle(Event::DocumentDelete, $doc);
        $this->assertSame(1, $cache->getPurges('default:qcache:users#epoch'));
    }

    public function testInvalidatorIgnoresNonWriteEvents(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $invalidator = new Invalidator($queryCache);

        $doc = new Document(['$id' => 'doc1', '$collection' => 'users']);

        $cache->expects($this->never())->method('purge');
        $invalidator->handle(Event::DocumentFind, $doc);
    }

    public function testInvalidatorExtractsCollectionFromDocument(): void
    {
        $cache = new InvalidationCache();
        $queryCache = new QueryCache($cache);
        $invalidator = new Invalidator($queryCache);

        $doc = new Document(['$id' => 'doc1', '$collection' => 'orders']);
        $invalidator->handle(Event::DocumentCreate, $doc);
        $this->assertSame(1, $cache->getPurges('default:qcache:orders#epoch'));
    }

    public function testInvalidatorHandlesStringData(): void
    {
        $cache = new InvalidationCache();
        $queryCache = new QueryCache($cache);
        $invalidator = new Invalidator($queryCache);

        $invalidator->handle(Event::DocumentCreate, 'products');
        $this->assertSame(1, $cache->getPurges('default:qcache:products#epoch'));
    }

    public function testInvalidatorIgnoresEmptyCollection(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $invalidator = new Invalidator($queryCache);

        $doc = new Document(['$id' => 'doc1']);

        $cache->expects($this->never())->method('purge');
        $invalidator->handle(Event::DocumentCreate, $doc);
    }

    public function testInvalidatorInvalidatesBothRelationshipCollections(): void
    {
        $cache = new InvalidationCache();
        $queryCache = new QueryCache($cache);
        $invalidator = new Invalidator($queryCache);

        $invalidator->handle(Event::AttributeCreate, new Document([
            '$collection' => 'posts',
            'options' => [
                'relatedCollection' => 'authors',
            ],
        ]));

        $this->assertSame(1, $cache->getPurges('default:qcache:posts#epoch'));
        $this->assertSame(1, $cache->getPurges('default:qcache:authors#epoch'));
    }

    public function testInvalidatorUsesCollectionIdentityForCollectionMutations(): void
    {
        $cache = new InvalidationCache();
        $queryCache = new QueryCache($cache);
        $invalidator = new Invalidator($queryCache);

        $invalidator->handle(Event::CollectionUpdate, new Document([
            '$id' => 'users',
            '$collection' => Database::METADATA,
        ]));
        $this->assertSame(1, $cache->getPurges('default:qcache:users#epoch'));
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

    public function testMemoryAdapterStaysBlockedAfterInvalidation(): void
    {
        $queryCache = new QueryCache(new Cache(new Memory()));
        $key = $queryCache->buildQueryKey('users', [], 'ns', null);
        $generation = $queryCache->getGeneration($key);

        $this->assertTrue($queryCache->set($key, [new Document(['$id' => 'old'])], $generation));
        $documents = $queryCache->get($key);
        $this->assertNotNull($documents);
        $this->assertSame('old', $documents[0]->getId());

        $queryCache->invalidateCollection('users');

        $this->assertFalse($queryCache->isEnabled('users'));
        $this->assertNull($queryCache->get($key));
        $this->assertFalse($queryCache->set($key, [new Document(['$id' => 'new'])]));
    }

    public function testConcurrentOwnersCannotEnableCacheEarly(): void
    {
        for ($iteration = 0; $iteration < 10; $iteration++) {
            $adapter = new OwnershipCache();
            $first = new QueryCache(new Cache($adapter));
            $second = new QueryCache(new Cache($adapter));
            $reader = new QueryCache(new Cache($adapter));
            $key = $reader->buildQueryKey('users', [], 'ns', null);
            $firstToken = 'first-'.$iteration;
            $secondToken = 'second-'.$iteration;

            $generation = $reader->getGeneration($key);
            $this->assertTrue($reader->set($key, [new Document(['$id' => 'old'])], $generation));
            $documents = (new QueryCache(new Cache($adapter)))->get($key);
            $this->assertNotNull($documents);
            $this->assertSame('old', $documents[0]->getId());

            $first->blockCollection('users', $firstToken);
            $adapter->pauseNextActivation(function () use ($adapter, $reader, $second, $secondToken): void {
                $second->blockCollection('users', $secondToken);

                $this->assertFalse($reader->isEnabled('users'));
                $this->assertNull($reader->get($reader->buildQueryKey('users', [], 'ns', null)));
                $this->assertTrue($adapter->has('default:qcache:users#owner:'.$secondToken));
            });

            $first->activateCollection('users', $firstToken);

            $this->assertFalse($adapter->has('default:qcache:users#owner:'.$firstToken));
            $this->assertTrue($adapter->has('default:qcache:users#owner:'.$secondToken));
            $this->assertFalse($reader->isEnabled('users'));
            $this->assertNull($reader->get($key));

            $second->activateCollection('users', $secondToken);

            $this->assertFalse($adapter->has('default:qcache:users#owner:'.$secondToken));
            $this->assertTrue($reader->isEnabled('users'));
            $this->assertNull($reader->get($key));
            $generation = $reader->getGeneration($key);
            $this->assertTrue($reader->set($key, [new Document(['$id' => 'fresh'])], $generation));
            $this->assertSame(
                ['fresh'],
                \array_map(
                    static fn (Document $document): string => (string) $document->getId(),
                    (new QueryCache(new Cache($adapter)))->get($key) ?? [],
                ),
            );
        }
    }

    public function testInvalidationPropagatesPurgeFailure(): void
    {
        $cache = new InvalidationCache();
        $cache->fail('default:qcache:users#epoch');

        $this->expectException(\RuntimeException::class);
        (new QueryCache($cache))->invalidateCollection('users');
    }

}

final class InvalidationCache extends Cache
{
    /** @var array<string, string> */
    public array $values = [];

    /** @var array<string, int> */
    public array $generations = [];

    /** @var array<string, int> */
    public array $purges = [];

    /** @var array<string, true> */
    public array $failures = [];

    public function __construct()
    {
        parent::__construct(new Memory());
    }

    #[\Override]
    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        if (isset($this->values[$key])) {
            return $this->values[$key];
        }

        return \str_ends_with($key, '#epoch') ? 'active:epoch' : false;
    }

    #[\Override]
    public function save(string $key, mixed $data, string $hash = ''): string|array
    {
        if (\is_string($data)) {
            $this->values[$key] = $data;
        }

        return $data;
    }

    #[\Override]
    public function getGeneration(string $key): string
    {
        return (string) ($this->generations[$key] ?? 0);
    }

    #[\Override]
    public function purge(string $key, string $hash = ''): bool
    {
        $this->purges[$key] = ($this->purges[$key] ?? 0) + 1;
        if (isset($this->failures[$key])) {
            return false;
        }

        $this->generations[$key] = ($this->generations[$key] ?? 0) + 1;
        unset($this->values[$key]);

        return true;
    }

    public function fail(string $key): void
    {
        $this->failures[$key] = true;
    }

    public function getPurges(string $key): int
    {
        return $this->purges[$key] ?? 0;
    }
}

final class OwnershipCache implements CacheAdapter, Leasable
{
    /** @var array<string, array{time: int, data: array<int|string, mixed>|string}> */
    private array $store = [];

    /** @var array<string, int> */
    private array $generations = [];

    private ?Closure $activation = null;

    public function load(string $key, int $ttl, string $hash = ''): mixed
    {
        $saved = $this->store[$key] ?? null;

        return $saved !== null && $saved['time'] + $ttl > \time() ? $saved['data'] : false;
    }

    public function save(string $key, array|string $data, string $hash = ''): bool|string|array
    {
        if ($key === '') {
            return false;
        }

        if (
            $this->activation !== null
            && \str_ends_with($key, '#epoch')
            && \is_string($data)
            && \str_starts_with($data, 'active:')
        ) {
            $activation = $this->activation;
            $this->activation = null;
            $activation();
        }

        $this->store[$key] = ['time' => \time(), 'data' => $data];

        return $data;
    }

    public function getGeneration(string $key): string
    {
        return (string) ($this->generations[$key] ?? 0);
    }

    public function saveWithLease(string $key, array|string $data, string $hash, string $generation): bool|string|array
    {
        if ($this->getGeneration($key) !== $generation) {
            return false;
        }

        return $this->save($key, $data, $hash);
    }

    public function touch(string $key, string $hash = ''): bool
    {
        if (! isset($this->store[$key])) {
            return false;
        }

        $this->store[$key]['time'] = \time();

        return true;
    }

    /** @return array<string> */
    public function list(string $key): array
    {
        return [];
    }

    public function purge(string $key, string $hash = ''): bool
    {
        $this->generations[$key] = ($this->generations[$key] ?? 0) + 1;
        unset($this->store[$key]);

        return true;
    }

    public function flush(): bool
    {
        $this->store = [];
        $this->generations = [];

        return true;
    }

    public function ping(): bool
    {
        return true;
    }

    public function getSize(): int
    {
        return \count($this->store);
    }

    public function getName(?string $key = null): string
    {
        return 'ownership';
    }

    public function has(string $key): bool
    {
        return isset($this->store[$key]);
    }

    public function pauseNextActivation(Closure $activation): void
    {
        $this->activation = $activation;
    }
}
