<?php

namespace Tests\Unit\Cache;

use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;
use Utopia\Database\Cache\Entry;
use Utopia\Database\Cache\Invalidator;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Cache\Region;
use Utopia\Database\Cache\Scope;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Query;

class QueryCacheTest extends TestCase
{
    private QueryCache $queryCache;

    private Cache&Stub $cache;

    protected function setUp(): void
    {
        $this->cache = self::createCache();
        $this->queryCache = new QueryCache($this->cache);
    }

    public function testConstructorWithDefaults(): void
    {
        $queryCache = new QueryCache(self::createCache());

        $this->assertNotNull($queryCache->getEntry(new Scope(), 'any_collection', []));
    }

    public function testConstructorWithCustomName(): void
    {
        $queryCache = new QueryCache(self::createCache(), 'custom');

        $entry = $queryCache->getEntry(new Scope(), 'users', []);

        $this->assertNotNull($entry);
        $this->assertStringStartsWith('custom:', $entry->key);
    }

    public function testSetRegionAndGetRegion(): void
    {
        $region = new Region(ttl: 600, enabled: false);
        $this->queryCache->setRegion('users', $region);

        $this->assertSame($region, $this->queryCache->getRegion('users'));
    }

    public function testGetRegionReturnsDefaultForUnknownCollection(): void
    {
        $region = $this->queryCache->getRegion('unknown');

        $this->assertSame(3600, $region->ttl);
        $this->assertTrue($region->enabled);
    }

    public function testEntryKeysAreStable(): void
    {
        $queries = [Query::equal('status', ['active'])];
        $scope = new Scope(namespace: 'ns', tenant: 1);

        $first = $this->queryCache->getEntry($scope, 'users', $queries);
        $second = $this->queryCache->getEntry($scope, 'users', $queries);

        $this->assertNotNull($first);
        $this->assertNotNull($second);
        $this->assertSame($first->key, $second->key);
    }

    public function testCollectionKeysSeparateEveryScopeField(): void
    {
        $key = $this->queryCache->getCollectionKey(new Scope('host', 'database', 'namespace', 1), 'users');

        foreach ([
            new Scope('other', 'database', 'namespace', 1),
            new Scope('host', 'other', 'namespace', 1),
            new Scope('host', 'database', 'other', 1),
            new Scope('host', 'database', 'namespace', 2),
            new Scope('host', 'database', 'namespace', null),
        ] as $scope) {
            $this->assertNotSame($key, $this->queryCache->getCollectionKey($scope, 'users'));
        }
    }

    public function testCollectionKeysPreserveTenantType(): void
    {
        $this->assertNotSame(
            $this->queryCache->getCollectionKey(new Scope(tenant: 1), 'users'),
            $this->queryCache->getCollectionKey(new Scope(tenant: '1'), 'users'),
        );
    }

    public function testCollectionKeysSeparateCollectionsThatDifferOnlyInCase(): void
    {
        $this->assertNotSame(
            \strtolower($this->queryCache->getCollectionKey(new Scope(), 'Users')),
            \strtolower($this->queryCache->getCollectionKey(new Scope(), 'users')),
            'Cache keys are case-insensitive by default, so the collection must be part of the scope hash',
        );
    }

    public function testDifferentQueriesProduceDifferentEntries(): void
    {
        $first = $this->queryCache->getEntry(new Scope(), 'users', [Query::equal('a', [1])]);
        $second = $this->queryCache->getEntry(new Scope(), 'users', [Query::equal('b', [2])]);

        $this->assertNotNull($first);
        $this->assertNotNull($second);
        $this->assertNotSame($first->key, $second->key);
    }

    public function testDifferentCollectionsProduceDifferentEntries(): void
    {
        $users = $this->queryCache->getEntry(new Scope(), 'users', []);
        $posts = $this->queryCache->getEntry(new Scope(), 'posts', []);

        $this->assertNotNull($users);
        $this->assertNotNull($posts);
        $this->assertNotSame($users->key, $posts->key);
    }

    public function testGetReturnsNullForCacheMiss(): void
    {
        $this->cache->method('load')->willReturn(false);

        $this->assertNull($this->queryCache->get(new Entry('some-key', 'users')));
    }

    public function testGetReturnsNullForNullData(): void
    {
        $this->cache->method('load')->willReturn(null);

        $this->assertNull($this->queryCache->get(new Entry('some-key', 'users')));
    }

    public function testGetReturnsDocumentArrayForCacheHit(): void
    {
        $this->cache->method('load')->willReturn([
            'version' => 1,
            'documents' => [
                ['$id' => 'doc1', 'name' => 'Alice'],
                ['$id' => 'doc2', 'name' => 'Bob'],
            ],
        ]);

        $result = $this->queryCache->get(new Entry('some-key', 'users'));

        $this->assertNotNull($result);
        $this->assertCount(2, $result);
        $this->assertSame('doc1', $result[0]->getId());
    }

    public function testGetHandlesDocumentObjectsInCache(): void
    {
        $document = new Document(['$id' => 'doc1', 'name' => 'Alice']);
        $this->cache->method('load')->willReturn([
            'version' => 1,
            'documents' => [$document],
        ]);

        $result = $this->queryCache->get(new Entry('some-key', 'users'));

        $this->assertNotNull($result);
        $this->assertCount(1, $result);
        $this->assertSame($document, $result[0]);
    }

    public function testGetPropagatesMalformedPayloadPurgeFailure(): void
    {
        $this->cache->method('load')->willReturn('not-an-array');

        $this->expectException(\RuntimeException::class);
        $this->queryCache->get(new Entry('some-key', 'users'));
    }

    public function testEntriesExpireWithTheRegionButEpochsNeverDo(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);
        $queryCache->setRegion('users', new Region(ttl: 120));
        $scope = new Scope(namespace: 'ns');
        $key = $queryCache->getCollectionKey($scope, 'users');

        $cache->expects($this->exactly(2))
            ->method('load')
            ->willReturnCallback(function (string $cacheKey, int $ttl) use ($key): string|false {
                return match (true) {
                    $cacheKey === $key.'#epoch' && $ttl === \PHP_INT_MAX => 'active:epoch@0',
                    \str_starts_with($cacheKey, $key.'#active:epoch:') && $ttl === 120 => false,
                    default => throw new \LogicException("Unexpected load of '{$cacheKey}' for {$ttl} seconds"),
                };
            });
        $cache->method('getGeneration')->willReturn('0');

        $entry = $queryCache->getEntry($scope, 'users', [Query::limit(10)]);

        $this->assertNotNull($entry);
        $this->assertStringStartsWith($key.'#active:epoch:', $entry->key);
        $this->assertNull($queryCache->get($entry));
    }

    public function testGetPurgesMalformedPayload(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);

        $cache->method('load')->willReturn(['version' => 1, 'documents' => ['invalid']]);
        $cache->expects($this->once())
            ->method('purge')
            ->with('entry-key')
            ->willReturn(true);

        $this->assertNull($queryCache->get(new Entry('entry-key', 'users')));
    }

    public function testSetSerializesDocuments(): void
    {
        $cache = $this->createMock(Cache::class);
        $queryCache = new QueryCache($cache);

        $cache->expects($this->once())
            ->method('saveWithLease')
            ->with(
                'entry-key',
                $this->callback(function (array $data): bool {
                    $documents = $data['documents'] ?? null;

                    return ($data['version'] ?? null) === 1
                        && \is_array($documents)
                        && \is_array($documents[0] ?? null)
                        && ($documents[0]['$id'] ?? null) === 'doc1';
                }),
                '',
                '7',
            )
            ->willReturnArgument(1);

        $this->assertTrue($queryCache->set(
            new Entry('entry-key', 'users'),
            [new Document(['$id' => 'doc1', 'name' => 'Alice'])],
            '7',
        ));
    }

    public function testInvalidateCollectionBlocksThenPublishesAFreshEpoch(): void
    {
        $cache = new InvalidationCache();
        $queryCache = new QueryCache($cache);

        $queryCache->invalidateCollection(new Scope(), 'users');

        $this->assertInvalidated($cache, $queryCache->getCollectionKey(new Scope(), 'users'));
    }

    public function testEntriesResolveByDefault(): void
    {
        $this->assertNotNull($this->queryCache->getEntry(new Scope(), 'any', []));
    }

    public function testEntriesDoNotResolveWhenTheRegionIsDisabled(): void
    {
        $this->queryCache->setRegion('users', new Region(enabled: false));

        $this->assertNull($this->queryCache->getEntry(new Scope(), 'users', []));
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

        $this->assertSame(3600, $region->ttl);
        $this->assertTrue($region->enabled);
    }

    public function testRegionCustomValues(): void
    {
        $region = new Region(ttl: 120, enabled: false);

        $this->assertSame(120, $region->ttl);
        $this->assertFalse($region->enabled);
    }

    public function testInvalidatorInvalidatesOnDocumentCreate(): void
    {
        $this->assertInvalidatorInvalidates(Event::DocumentCreate, new Document(['$id' => 'doc1', '$collection' => 'users']), ['users']);
    }

    public function testInvalidatorInvalidatesOnDocumentUpdate(): void
    {
        $this->assertInvalidatorInvalidates(Event::DocumentUpdate, new Document(['$id' => 'doc1', '$collection' => 'posts']), ['posts']);
    }

    public function testInvalidatorInvalidatesOnDocumentDelete(): void
    {
        $this->assertInvalidatorInvalidates(Event::DocumentDelete, new Document(['$id' => 'doc1', '$collection' => 'users']), ['users']);
    }

    public function testInvalidatorIgnoresNonWriteEvents(): void
    {
        $cache = $this->createMock(Cache::class);
        $invalidator = new Invalidator(new QueryCache($cache));

        $cache->expects($this->never())->method('purge');
        $invalidator->handle(Event::DocumentFind, new Document(['$id' => 'doc1', '$collection' => 'users']));
    }

    public function testInvalidatorExtractsCollectionFromDocument(): void
    {
        $this->assertInvalidatorInvalidates(Event::DocumentCreate, new Document(['$id' => 'doc1', '$collection' => 'orders']), ['orders']);
    }

    public function testInvalidatorHandlesStringData(): void
    {
        $this->assertInvalidatorInvalidates(Event::DocumentCreate, 'products', ['products']);
    }

    public function testInvalidatorIgnoresEmptyCollection(): void
    {
        $cache = $this->createMock(Cache::class);
        $invalidator = new Invalidator(new QueryCache($cache));

        $cache->expects($this->never())->method('purge');
        $invalidator->handle(Event::DocumentCreate, new Document(['$id' => 'doc1']));
    }

    public function testInvalidatorInvalidatesBothRelationshipCollections(): void
    {
        $this->assertInvalidatorInvalidates(Event::AttributeCreate, new Document([
            '$collection' => 'posts',
            'options' => [
                'relatedCollection' => 'authors',
            ],
        ]), ['posts', 'authors']);
    }

    public function testInvalidatorUsesCollectionIdentityForCollectionMutations(): void
    {
        $this->assertInvalidatorInvalidates(Event::CollectionUpdate, new Document([
            '$id' => 'users',
            '$collection' => Database::METADATA,
        ]), ['users']);
    }

    public function testInvalidatorInvalidatesTheScopeItIsGiven(): void
    {
        $cache = new InvalidationCache();
        $queryCache = new QueryCache($cache);
        $scope = new Scope('host', 'database', 'namespace', 7);

        (new Invalidator($queryCache))->invalidate(Event::DocumentCreate, 'users', $scope);

        $this->assertInvalidated($cache, $queryCache->getCollectionKey($scope, 'users'));
        $this->assertSame(0, $cache->getPurges($queryCache->getCollectionKey(new Scope(), 'users').'#started'));
    }

    public function testInvalidatorHandlesEventsInTheScopeItWasGiven(): void
    {
        $cache = new InvalidationCache();
        $queryCache = new QueryCache($cache);
        $scope = new Scope(namespace: 'namespace', tenant: 'tenant');

        (new Invalidator($queryCache, $scope))->handle(Event::DocumentCreate, 'users');

        $this->assertInvalidated($cache, $queryCache->getCollectionKey($scope, 'users'));
    }

    public function testInvalidatorKeysTokensByTheScopedCollection(): void
    {
        $queryCache = new QueryCache(new InvalidationCache());
        $scope = new Scope(namespace: 'namespace', tenant: 7);

        $tokens = (new Invalidator($queryCache))->tokens(Event::DocumentCreate, 'users', $scope);

        $this->assertSame([$queryCache->getCollectionKey($scope, 'users')], \array_keys($tokens));
    }

    public function testMemoryAdapterKeepsPhysicalVariantsIsolated(): void
    {
        $queryCache = new QueryCache(new Cache(new Memory()));
        $first = $queryCache->getEntry(new Scope(namespace: 'ns'), 'users', [['limit' => 1]], 'role:user-a');
        $second = $queryCache->getEntry(new Scope(namespace: 'ns'), 'users', [['limit' => 2]], 'role:user-b');
        $this->assertNotNull($first);
        $this->assertNotNull($second);

        $this->assertTrue($queryCache->set($first, [new Document(['$id' => 'private-a'])], $queryCache->getGeneration($first)));
        $this->assertTrue($queryCache->set($second, [new Document(['$id' => 'private-b'])], $queryCache->getGeneration($second)));

        $this->assertSame(['private-a'], $this->ids($queryCache->get($first) ?? []));
        $this->assertSame(['private-b'], $this->ids($queryCache->get($second) ?? []));
    }

    public function testMemoryAdapterStaysBlockedAfterInvalidation(): void
    {
        $queryCache = new QueryCache(new Cache(new Memory()));
        $scope = new Scope(namespace: 'ns');
        $entry = $queryCache->getEntry($scope, 'users', []);
        $this->assertNotNull($entry);
        $this->assertTrue($queryCache->set($entry, [new Document(['$id' => 'old'])], $queryCache->getGeneration($entry)));
        $this->assertSame(['old'], $this->ids($queryCache->get($entry) ?? []));

        $queryCache->invalidateCollection($scope, 'users');

        $this->assertNull(
            $queryCache->getEntry($scope, 'users', []),
            'A cache without generations cannot prove a new epoch fresh, so it stays blocked',
        );
    }

    public function testConcurrentOwnersCannotEnableCacheEarly(): void
    {
        $scope = new Scope(namespace: 'ns');

        for ($iteration = 0; $iteration < 10; $iteration++) {
            $adapter = new OwnershipCache();
            $first = new QueryCache(new Cache($adapter));
            $second = new QueryCache(new Cache($adapter));
            $reader = new QueryCache(new Cache($adapter));
            $key = $reader->getCollectionKey($scope, 'users');
            $firstToken = 'first-'.$iteration;
            $secondToken = 'second-'.$iteration;

            $entry = $reader->getEntry($scope, 'users', []);
            $this->assertNotNull($entry);
            $this->assertTrue($reader->set($entry, [new Document(['$id' => 'old'])], $reader->getGeneration($entry)));
            $this->assertSame(['old'], $this->ids((new QueryCache(new Cache($adapter)))->get($entry) ?? []));

            $first->blockCollection($key, $firstToken);
            $adapter->pauseNextActivation(function () use ($adapter, $reader, $second, $scope, $key, $secondToken): void {
                $second->blockCollection($key, $secondToken);

                $this->assertNull($reader->getEntry($scope, 'users', []));
                $this->assertTrue($adapter->has($key.'#owner:'.$secondToken));
            });

            $first->activateCollection($key, $firstToken);

            $this->assertFalse($adapter->has($key.'#owner:'.$firstToken));
            $this->assertTrue($adapter->has($key.'#owner:'.$secondToken));
            $this->assertNull($reader->getEntry($scope, 'users', []));

            $second->activateCollection($key, $secondToken);

            $this->assertFalse($adapter->has($key.'#owner:'.$secondToken));
            $fresh = $reader->getEntry($scope, 'users', []);
            $this->assertNotNull($fresh);
            $this->assertNull($reader->get($fresh));
            $this->assertTrue($reader->set($fresh, [new Document(['$id' => 'fresh'])], $reader->getGeneration($fresh)));
            $this->assertSame(['fresh'], $this->ids((new QueryCache(new Cache($adapter)))->get($fresh) ?? []));
        }
    }

    public function testAnEpochPublishedAfterALaterFinishStaysUsable(): void
    {
        $adapter = new OwnershipCache();
        $queryCache = new QueryCache(new Cache($adapter));
        $scope = new Scope(namespace: 'ns');
        $key = $queryCache->getCollectionKey($scope, 'users');

        $queryCache->blockCollection($key, 'first');
        $adapter->pauseNextActivation(function () use ($queryCache, $key): void {
            $queryCache->blockCollection($key, 'second');
            $queryCache->activateCollection($key, 'second');
        });
        $queryCache->activateCollection($key, 'first');

        $entry = $queryCache->getEntry($scope, 'users', []);
        $this->assertNotNull($entry, 'Once every writer has finished, an epoch published late must still be usable');
        $this->assertTrue($queryCache->set($entry, [new Document(['$id' => 'fresh'])], $queryCache->getGeneration($entry)));
        $this->assertSame(['fresh'], $this->ids($queryCache->get($entry) ?? []));
    }

    public function testATombstoneOlderThanItsRegionStillBlocksWhileItsWriterIsInFlight(): void
    {
        $adapter = new OwnershipCache();
        $queryCache = new QueryCache(new Cache($adapter));
        $queryCache->setRegion('users', new Region(ttl: 0));
        $key = $queryCache->getCollectionKey(new Scope(), 'users');

        $queryCache->blockCollection($key, 'writer');

        $this->assertNull(
            $queryCache->getEntry(new Scope(), 'users', []),
            'A transaction that outlives the region TTL must keep readers off the cache until it activates',
        );

        $queryCache->activateCollection($key, 'writer');

        $this->assertStringStartsWith('active:', $this->epochOf($adapter, $key), 'The writer must still own its tombstone and publish a fresh epoch');
    }

    public function testATombstoneOnACacheWithoutGenerationsLapsesWithItsRegion(): void
    {
        $queryCache = new QueryCache(new Cache(new Memory()));
        $queryCache->setRegion('users', new Region(ttl: 0));

        $queryCache->invalidateCollection(new Scope(), 'users');

        $this->assertNotNull(
            $queryCache->getEntry(new Scope(), 'users', []),
            'Without generations a tombstone is the only guard, and it must lapse with its region',
        );
    }

    public function testCacheFlushDuringActivationDoesNotFailInvalidation(): void
    {
        $adapter = new OwnershipCache();
        $queryCache = new QueryCache(new Cache($adapter));
        $key = $queryCache->getCollectionKey(new Scope(), 'users');
        $queryCache->blockCollection($key, 'owner');
        $adapter->flushDuringActivation();

        $queryCache->activateCollection($key, 'owner');

        $this->assertNotNull($queryCache->getEntry(new Scope(), 'users', []));
    }

    public function testCacheFlushBeforeActivationDoesNotFailInvalidation(): void
    {
        $adapter = new OwnershipCache();
        $queryCache = new QueryCache(new Cache($adapter));
        $key = $queryCache->getCollectionKey(new Scope(), 'users');
        $queryCache->blockCollection($key, 'owner');
        $this->assertTrue($adapter->flush());

        $queryCache->activateCollection($key, 'owner');

        $this->assertNotNull($queryCache->getEntry(new Scope(), 'users', []));
    }

    public function testActivationPurgeFailureStillPropagates(): void
    {
        $adapter = new OwnershipCache();
        $queryCache = new QueryCache(new Cache($adapter));
        $key = $queryCache->getCollectionKey(new Scope(), 'users');
        $queryCache->blockCollection($key, 'owner');
        $adapter->failDuringActivation();

        try {
            $queryCache->activateCollection($key, 'owner');
            $this->fail('Query cache activation purge failure was not propagated');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('finish query cache invalidation', $error->getMessage());
        }

        $this->assertNull($queryCache->getEntry(new Scope(), 'users', []));
    }

    public function testInvalidationPropagatesACacheWriteFailure(): void
    {
        $cache = new InvalidationCache();
        $queryCache = new QueryCache($cache);
        $cache->fail($queryCache->getCollectionKey(new Scope(), 'users').'#epoch');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Failed to block query cache epoch');
        $queryCache->invalidateCollection(new Scope(), 'users');
    }

    /**
     * @param  array<string>  $collections
     */
    private function assertInvalidatorInvalidates(Event $event, mixed $data, array $collections): void
    {
        $cache = new InvalidationCache();
        $queryCache = new QueryCache($cache);

        (new Invalidator($queryCache))->handle($event, $data);

        foreach ($collections as $collection) {
            $this->assertInvalidated($cache, $queryCache->getCollectionKey(new Scope(), $collection));
        }
    }

    private function assertInvalidated(InvalidationCache $cache, string $key): void
    {
        $this->assertSame(1, $cache->getPurges($key.'#started'), 'The invalidation must block the collection once');
        $this->assertSame(1, $cache->getPurges($key.'#finished'), 'The invalidation must finish what it blocked');
        $this->assertStringStartsWith('active:', $cache->values[$key.'#epoch'] ?? '', 'The invalidation must publish a fresh epoch');
    }

    private static function createCache(): Cache&Stub
    {
        $cache = self::createStub(Cache::class);
        $cache->method('getGeneration')->willReturn('0');

        return $cache;
    }

    private function epochOf(OwnershipCache $adapter, string $key): string
    {
        $epoch = $adapter->load($key.'#epoch', \PHP_INT_MAX);
        $this->assertIsString($epoch);

        return $epoch;
    }

    /**
     * @param  array<Document>  $documents
     * @return array<string>
     */
    private function ids(array $documents): array
    {
        return \array_map(
            static fn (Document $document): string => $document->getId(),
            $documents,
        );
    }
}
