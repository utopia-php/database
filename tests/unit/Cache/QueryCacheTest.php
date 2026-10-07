<?php

namespace Tests\Unit\Cache;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\Entry;
use Utopia\Database\Cache\Invalidator;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Cache\Region;
use Utopia\Database\Cache\Scope;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;

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

    public function testCacheNamesKeepTheirResultsApartOnTheSameCache(): void
    {
        $adapter = new RedisLeasableCache();
        $default = new QueryCache(new Cache($adapter));
        $custom = self::attached(new QueryCache(new Cache($adapter)), name: 'custom');
        $entry = $custom->getEntry(new Scope(), 'users', []);
        $this->assertNotNull($entry);
        $this->assertTrue($custom->set($entry, [new Document(['$id' => 'custom'])], $custom->getGeneration($entry)));

        $other = $default->getEntry(new Scope(), 'users', []);
        $this->assertNotNull($other);
        $this->assertNull($default->get($other), 'A query cache must not serve what a query cache of another name filled');

        $default->invalidateCollection(new Scope(), 'users');

        $after = $custom->getEntry(new Scope(), 'users', []);
        $this->assertNotNull($after);
        $this->assertSame(['custom'], $this->ids($custom->get($after) ?? []), 'Invalidating one query cache must not retire what a query cache of another name filled');
    }

    public function testTheNameFollowsTheDatabaseItIsAttachedTo(): void
    {
        $adapter = new RedisLeasableCache();
        $renamed = new QueryCache(new Cache($adapter));
        $database = (new Database(new DatabaseMemory(), new Cache(new None())))->setQueryCache($renamed);
        $custom = self::attached(new QueryCache(new Cache($adapter)), name: 'custom');
        $entry = $custom->getEntry(new Scope(), 'users', []);
        $this->assertNotNull($entry);
        $this->assertTrue($custom->set($entry, [new Document(['$id' => 'custom'])], $custom->getGeneration($entry)));

        $database->setCacheName('custom');

        $shared = $renamed->getEntry(new Scope(), 'users', []);
        $this->assertNotNull($shared);
        $this->assertSame(['custom'], $this->ids($renamed->get($shared) ?? []), 'A query cache must read under the name its database has now');
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
        $this->assertNotSame($first->field, $second->field);
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

    public function testFilledResultsAreServedByAnotherQueryCacheOnTheSameCache(): void
    {
        $adapter = new RedisLeasableCache();
        $writer = new QueryCache(new Cache($adapter));
        $reader = new QueryCache(new Cache($adapter));
        $scope = new Scope(namespace: 'ns');
        $entry = $writer->getEntry($scope, 'users', [Query::limit(2)]);
        $this->assertNotNull($entry);

        $this->assertTrue($writer->set($entry, [
            new Document(['$id' => 'doc1', 'name' => 'Alice']),
            new Document(['$id' => 'doc2', 'name' => 'Bob']),
        ], $writer->getGeneration($entry)));

        $served = $reader->getEntry($scope, 'users', [Query::limit(2)]);
        $this->assertNotNull($served);
        $result = $reader->get($served);
        $this->assertNotNull($result);
        $this->assertSame(['doc1', 'doc2'], $this->ids($result));
        $this->assertSame(['Alice', 'Bob'], \array_map(
            static fn (Document $document): mixed => $document->getAttribute('name'),
            $result,
        ));
    }

    public function testAFillThatStartedBeforeAnInvalidationIsRejected(): void
    {
        $queryCache = new QueryCache(new Cache(new RedisLeasableCache()));
        $scope = new Scope(namespace: 'ns');
        $entry = $queryCache->getEntry($scope, 'users', []);
        $this->assertNotNull($entry);
        $generation = $queryCache->getGeneration($entry);

        $queryCache->invalidateCollection($scope, 'users');

        $this->assertFalse($queryCache->set($entry, [new Document(['$id' => 'stale'])], $generation), 'A fill must not land once a write has started since its read');
        $fresh = $queryCache->getEntry($scope, 'users', []);
        $this->assertNotNull($fresh);
        $this->assertNull($queryCache->get($fresh));
    }

    public function testGetHandlesDocumentObjectsInCache(): void
    {
        $document = new Document(['$id' => 'doc1', 'name' => 'Alice']);
        $this->cache->method('load')->willReturn([
            'version' => 2,
            'epoch' => '',
            'field' => '',
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
        $queryCache = new QueryCache(new Cache(new RedisLeasableCache()));
        $scope = new Scope(namespace: 'ns');
        $queryCache->setRegion('users', new Region(ttl: 0));
        $queryCache->invalidateCollection($scope, 'users');

        $entry = $queryCache->getEntry($scope, 'users', [Query::limit(10)]);
        $this->assertNotNull($entry, 'The epoch a write published must stay usable after the region TTL has passed');
        $this->assertTrue($queryCache->set($entry, [new Document(['$id' => 'filled'])], $queryCache->getGeneration($entry)));
        $this->assertNull($queryCache->get($entry), 'A result older than the region TTL must miss');

        $queryCache->setRegion('users', new Region(ttl: 60));

        $this->assertSame(['filled'], $this->ids($queryCache->get($entry) ?? []), 'The same result is served within a longer region TTL, so the miss was its expiry');
    }

    /**
     * @return iterable<string, array{\Closure(Entry): (string|array<int|string, mixed>)}>
     */
    public static function malformedPayloads(): iterable
    {
        yield 'a string' => [static fn (Entry $entry): string => 'not-an-array'];
        yield 'an array of another shape' => [static fn (Entry $entry): array => ['foreign' => 'payload']];
        yield 'documents that are not documents' => [static fn (Entry $entry): array => [
            'version' => 2,
            'epoch' => $entry->epoch,
            'field' => $entry->field,
            'documents' => ['invalid'],
        ]];
    }

    /**
     * @param  \Closure(Entry): (string|array<int|string, mixed>)  $payload
     */
    #[DataProvider('malformedPayloads')]
    public function testAMalformedResultMissesAndIsReplacedByTheNextFill(\Closure $payload): void
    {
        $cache = new Cache(new RedisLeasableCache());
        $queryCache = new QueryCache($cache);
        $entry = $queryCache->getEntry(new Scope(namespace: 'ns'), 'users', []);
        $this->assertNotNull($entry);
        $cache->save($entry->key, $payload($entry), $entry->slot);

        $this->assertNull($queryCache->get($entry), 'A result the query cache cannot read must be a miss, not an error');
        $this->assertTrue($queryCache->set($entry, [new Document(['$id' => 'fresh'])], $queryCache->getGeneration($entry)));
        $this->assertSame(['fresh'], $this->ids($queryCache->get($entry) ?? []));
    }

    public function testInvalidateCollectionBlocksThenPublishesAFreshEpoch(): void
    {
        $queryCache = new QueryCache(new Cache(new RedisLeasableCache()));

        $this->assertRetired($queryCache, new Scope(), 'users', function () use ($queryCache): void {
            $queryCache->invalidateCollection(new Scope(), 'users');
        });
    }

    public function testAnInvalidEpochIsAMiss(): void
    {
        $cache = new Cache(new Memory());
        $queryCache = new QueryCache($cache);
        $scope = new Scope(namespace: 'ns');
        $cache->save($queryCache->getCollectionKey($scope, 'users').'#epoch', ['not' => 'an epoch']);

        $this->assertNull($queryCache->getEntry($scope, 'users', []), 'An epoch value the query cache did not write must disable the cache for that read, not fail it');
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

    public function testFlushDropsEveryCachedResult(): void
    {
        $queryCache = new QueryCache(new Cache(new RedisLeasableCache()));
        $scope = new Scope(namespace: 'ns');
        $users = $queryCache->getEntry($scope, 'users', []);
        $posts = $queryCache->getEntry($scope, 'posts', []);
        $this->assertNotNull($users);
        $this->assertNotNull($posts);
        $this->assertTrue($queryCache->set($users, [new Document(['$id' => 'user'])], $queryCache->getGeneration($users)));
        $this->assertTrue($queryCache->set($posts, [new Document(['$id' => 'post'])], $queryCache->getGeneration($posts)));

        $queryCache->flush();

        foreach (['users', 'posts'] as $collection) {
            $entry = $queryCache->getEntry($scope, $collection, []);
            $this->assertNotNull($entry, "A flush must leave '{$collection}' usable");
            $this->assertNull($queryCache->get($entry), "A flush must drop what '{$collection}' filled before it");
        }
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
        $queryCache = new QueryCache(new Cache(new RedisLeasableCache()));

        $this->assertKept($queryCache, ['users'], function () use ($queryCache): void {
            (new Invalidator($queryCache))->handle(Event::DocumentFind, new Document(['$id' => 'doc1', '$collection' => 'users']));
        });
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
        $queryCache = new QueryCache(new Cache(new RedisLeasableCache()));

        $this->assertKept($queryCache, ['users', ''], function () use ($queryCache): void {
            (new Invalidator($queryCache))->handle(Event::DocumentCreate, new Document(['$id' => 'doc1']));
        });
    }

    public function testInvalidatorInvalidatesBothRelationshipCollections(): void
    {
        $this->assertInvalidatorInvalidates(
            Event::AttributeCreate,
            Attribute::relationship('author', Relationship::manyToOne('authors'), RelationshipSide::Parent)
                ->toDocument()
                ->setAttribute('$collection', 'posts'),
            ['posts', 'authors'],
        );
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
        $queryCache = new QueryCache(new Cache(new RedisLeasableCache()));
        $scope = new Scope('host', 'database', 'namespace', 7);
        $untouched = $queryCache->getEntry(new Scope(), 'users', []);
        $this->assertNotNull($untouched);
        $this->assertTrue($queryCache->set($untouched, [new Document(['$id' => 'untouched'])], $queryCache->getGeneration($untouched)));

        $this->assertRetired($queryCache, $scope, 'users', function () use ($queryCache, $scope): void {
            (new Invalidator($queryCache))->invalidate(Event::DocumentCreate, 'users', $scope);
        });

        $other = $queryCache->getEntry(new Scope(), 'users', []);
        $this->assertNotNull($other);
        $this->assertSame(['untouched'], $this->ids($queryCache->get($other) ?? []), 'Invalidating one scope must not retire what another scope filled');
    }

    public function testInvalidatorHandlesEventsInTheScopeItWasGiven(): void
    {
        $queryCache = new QueryCache(new Cache(new RedisLeasableCache()));
        $scope = new Scope(namespace: 'namespace', tenant: 'tenant');

        $this->assertRetired($queryCache, $scope, 'users', function () use ($queryCache, $scope): void {
            (new Invalidator($queryCache, $scope))->handle(Event::DocumentCreate, 'users');
        });
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

        $this->assertNull($queryCache->get($first), 'A cache without fields holds one result per collection: the second query took the slot, so the first must miss instead of being served its rows');
        $this->assertSame(['private-b'], $this->ids($queryCache->get($second) ?? []));

        $this->assertTrue($queryCache->set($first, [new Document(['$id' => 'private-a'])], $queryCache->getGeneration($first)));

        $this->assertSame(['private-a'], $this->ids($queryCache->get($first) ?? []));
        $this->assertNull($queryCache->get($second));
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
            $adapter->pauseNextActivation(function () use ($reader, $second, $scope, $key, $secondToken): void {
                $second->blockCollection($key, $secondToken);

                $this->assertNull($reader->getEntry($scope, 'users', []));
            });

            $first->activateCollection($key, $firstToken);

            $this->assertNull($reader->getEntry($scope, 'users', []));

            $second->activateCollection($key, $secondToken);

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
        $queryCache = new QueryCache(new Cache(new OwnershipCache()));
        $queryCache->setRegion('users', new Region(ttl: 0));
        $key = $queryCache->getCollectionKey(new Scope(), 'users');
        $stale = $queryCache->getEntry(new Scope(), 'users', []);
        $this->assertNotNull($stale);
        $this->assertTrue($queryCache->set($stale, [new Document(['$id' => 'stale'])], $queryCache->getGeneration($stale)));

        $queryCache->blockCollection($key, 'writer');

        $this->assertNull(
            $queryCache->getEntry(new Scope(), 'users', []),
            'A transaction that outlives the region TTL must keep readers off the cache until it activates',
        );

        $queryCache->activateCollection($key, 'writer');
        $queryCache->setRegion('users', new Region());

        $fresh = $queryCache->getEntry(new Scope(), 'users', []);
        $this->assertNotNull($fresh, 'The writer must still own its tombstone and publish a fresh epoch');
        $this->assertNull($queryCache->get($fresh), 'The fresh epoch must not serve what was filled before the write');
        $this->assertTrue($queryCache->set($fresh, [new Document(['$id' => 'fresh'])], $queryCache->getGeneration($fresh)));
        $this->assertSame(['fresh'], $this->ids($queryCache->get($fresh) ?? []));
    }

    public function testAKilledWriterDoesNotDisableTheQueryCacheForever(): void
    {
        $adapter = new RedisLeasableCache();
        $queryCache = self::attached(new QueryCache(new Cache($adapter)), writerTimeout: 0);
        $scope = new Scope(namespace: 'ns');
        $key = $queryCache->getCollectionKey($scope, 'users');
        $before = $queryCache->getEntry($scope, 'users', []);
        $this->assertNotNull($before);

        $queryCache->blockCollection($key, $queryCache->createToken());
        $this->assertTrue($queryCache->set($before, [new Document(['$id' => 'stale'])], $queryCache->getGeneration($before)));

        $entry = $queryCache->getEntry($scope, 'users', []);
        $this->assertNotNull($entry, 'A tombstone whose writer never activates must lapse once it is older than the writer timeout');
        $this->assertNull($queryCache->get($entry), 'The lapse must not serve a result filled under the epoch the killed writer retired');
        $this->assertTrue($queryCache->set($entry, [new Document(['$id' => 'fresh'])], $queryCache->getGeneration($entry)));
        $this->assertSame(['fresh'], $this->ids($queryCache->get($entry) ?? []));
    }

    public function testAWriteAfterAKilledWriterReenablesTheQueryCache(): void
    {
        $adapter = new RedisLeasableCache();
        $killed = new QueryCache(new Cache($adapter));
        $writer = self::attached(new QueryCache(new Cache($adapter)), writerTimeout: 0);
        $reader = new QueryCache(new Cache($adapter));
        $scope = new Scope(namespace: 'ns');
        $killed->blockCollection($killed->getCollectionKey($scope, 'users'), $killed->createToken());
        $this->assertNull($reader->getEntry($scope, 'users', []));

        $writer->invalidateCollection($scope, 'users');

        $entry = $reader->getEntry($scope, 'users', []);
        $this->assertNotNull($entry, 'The next write must reconcile a writer whose registration is older than the writer timeout');
        $this->assertTrue($reader->set($entry, [new Document(['$id' => 'fresh'])], $reader->getGeneration($entry)));
        $this->assertSame(['fresh'], $this->ids($reader->get($entry) ?? []));

        $writer->invalidateCollection($scope, 'users');

        $this->assertNotNull($reader->getEntry($scope, 'users', []), 'Later writes must not be held back by the killed writer either');
    }

    public function testAWriteDoesNotReenableTheQueryCacheWhileAnotherWriterIsLive(): void
    {
        $adapter = new RedisLeasableCache();
        $live = new QueryCache(new Cache($adapter));
        $writer = new QueryCache(new Cache($adapter));
        $scope = new Scope(namespace: 'ns');
        $key = $live->getCollectionKey($scope, 'users');
        $token = $live->createToken();
        $live->blockCollection($key, $token);

        $writer->invalidateCollection($scope, 'users');

        $this->assertNull($writer->getEntry($scope, 'users', []), 'A writer registered within the writer timeout is still in flight');

        $live->activateCollection($key, $token);

        $this->assertNotNull($writer->getEntry($scope, 'users', []));
    }

    public function testAWriterWhoseTokenHasNoCreationTimeCountsAsLive(): void
    {
        $adapter = new RedisLeasableCache();
        $live = new QueryCache(new Cache($adapter));
        $writer = self::attached(new QueryCache(new Cache($adapter)), writerTimeout: 0);
        $reader = new QueryCache(new Cache($adapter));
        $scope = new Scope(namespace: 'ns');
        $live->blockCollection($live->getCollectionKey($scope, 'users'), 'token-without-a-time');

        $writer->invalidateCollection($scope, 'users');

        $this->assertNull($reader->getEntry($scope, 'users', []), 'Without a creation time a registration cannot be judged abandoned');
    }

    public function testAWriterPastTheTimeoutRetiresWhatReadersFilledWhileItRan(): void
    {
        $adapter = new RedisLeasableCache();
        $queryCache = self::attached(new QueryCache(new Cache($adapter)), writerTimeout: 0);
        $scope = new Scope(namespace: 'ns');
        $key = $queryCache->getCollectionKey($scope, 'users');
        $token = $queryCache->createToken();
        $queryCache->blockCollection($key, $token);
        $during = $queryCache->getEntry($scope, 'users', []);
        $this->assertNotNull($during);
        $this->assertTrue($queryCache->set($during, [new Document(['$id' => 'before-commit'])], $queryCache->getGeneration($during)));

        $queryCache->activateCollection($key, $token);

        $after = $queryCache->getEntry($scope, 'users', []);
        $this->assertNotNull($after);
        $this->assertNull($queryCache->get($after), 'A result filled while the writer ran must not be served once it activates');
    }

    public function testAWriterJudgedAbandonedStillRetiresWhatReadersFilledWhileItRan(): void
    {
        $adapter = new RedisLeasableCache();
        $slow = new QueryCache(new Cache($adapter));
        $writer = self::attached(new QueryCache(new Cache($adapter)), writerTimeout: 0);
        $scope = new Scope(namespace: 'ns');
        $key = $slow->getCollectionKey($scope, 'users');
        $token = $slow->createToken();
        $slow->blockCollection($key, $token);
        $writer->invalidateCollection($scope, 'users');
        $during = $slow->getEntry($scope, 'users', []);
        $this->assertNotNull($during);
        $this->assertTrue($slow->set($during, [new Document(['$id' => 'before-commit'])], $slow->getGeneration($during)));

        $slow->activateCollection($key, $token);

        $after = $slow->getEntry($scope, 'users', []);
        $this->assertNotNull($after);
        $this->assertNull($slow->get($after), 'A writer whose registration was reconciled away must still retire what readers filled before its commit');
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

    public function testFillsAndInvalidationsKeepTheCacheSizeBounded(): void
    {
        $slots = 4;
        $adapter = new RedisLeasableCache();
        $queryCache = new QueryCache(new Cache($adapter), slots: $slots);
        $scope = new Scope(namespace: 'ns');
        $cycle = function (int $query) use ($queryCache, $scope): void {
            $entry = $queryCache->getEntry($scope, 'users', [Query::limit($query)]);
            $this->assertNotNull($entry);
            $this->assertNull($queryCache->get($entry));
            $this->assertTrue($queryCache->set($entry, [new Document(['$id' => 'query-'.$query])], $queryCache->getGeneration($entry)));
            $this->assertSame(['query-'.$query], $this->ids($queryCache->get($entry) ?? []));
            $queryCache->invalidateCollection($scope, 'users');
        };

        for ($query = 1; $query <= 20; $query++) {
            $cycle($query);
        }
        $keys = $adapter->getSize();
        $values = $adapter->countValues();

        for ($query = 21; $query <= 100; $query++) {
            $cycle($query);
        }

        $this->assertLessThanOrEqual($keys + $slots, $adapter->getSize(), 'Redis keeps no expiry on these keys and a purge leaves its key behind, so 80 more fills and writes must not add a key each');
        $this->assertLessThanOrEqual($values + $slots, $adapter->countValues(), 'Redis keeps no expiry on cached results, so the slot count, not the number of distinct queries, must bound what fills leave behind');
    }

    public function testAnInvalidationRetiresEveryCachedResultOfTheScope(): void
    {
        $queryCache = new QueryCache(new Cache(new RedisLeasableCache()));
        $scope = new Scope(namespace: 'ns');
        for ($query = 1; $query <= 50; $query++) {
            $entry = $queryCache->getEntry($scope, 'users', [Query::limit($query)]);
            $this->assertNotNull($entry);
            $this->assertTrue($queryCache->set($entry, [new Document(['$id' => 'old-'.$query])], $queryCache->getGeneration($entry)));
            $this->assertSame(['old-'.$query], $this->ids($queryCache->get($entry) ?? []));
        }

        $queryCache->invalidateCollection($scope, 'users');

        for ($query = 1; $query <= 50; $query++) {
            $entry = $queryCache->getEntry($scope, 'users', [Query::limit($query)]);
            $this->assertNotNull($entry, 'The invalidation must leave the scope usable');
            $this->assertNull($queryCache->get($entry), 'The invalidation must retire every result filled before it');
            $this->assertTrue($queryCache->set($entry, [new Document(['$id' => 'new-'.$query])], $queryCache->getGeneration($entry)));
            $this->assertSame(['new-'.$query], $this->ids($queryCache->get($entry) ?? []), 'A fill after the invalidation must be served');
        }
    }

    public function testQueriesSharingASlotNeverServeEachOther(): void
    {
        $adapter = new RedisLeasableCache();
        $queryCache = new QueryCache(new Cache($adapter), slots: 1);
        $scope = new Scope(namespace: 'ns');
        $first = $queryCache->getEntry($scope, 'users', [Query::limit(1)], 'role:user-a');
        $second = $queryCache->getEntry($scope, 'users', [Query::limit(2)], 'role:user-b');
        $this->assertNotNull($first);
        $this->assertNotNull($second);

        $this->assertTrue($queryCache->set($first, [new Document(['$id' => 'private-a'])], $queryCache->getGeneration($first)));
        $this->assertTrue($queryCache->set($second, [new Document(['$id' => 'private-b'])], $queryCache->getGeneration($second)));

        $this->assertNull($queryCache->get($first), 'The second query took the only slot, so the first must miss instead of being served its rows');
        $this->assertSame(['private-b'], $this->ids($queryCache->get($second) ?? []));
    }

    public function testAResultFilledBeforeTheFirstWriteIsNeverServedAfterTheEpochIsLost(): void
    {
        $adapter = new RedisLeasableCache();
        $queryCache = new QueryCache(new Cache($adapter));
        $scope = new Scope(namespace: 'ns');
        $key = $queryCache->getCollectionKey($scope, 'users');
        $before = $queryCache->getEntry($scope, 'users', []);
        $this->assertNotNull($before);
        $this->assertTrue($queryCache->set($before, [new Document(['$id' => 'stale'])], $queryCache->getGeneration($before)));

        $queryCache->invalidateCollection($scope, 'users');
        $adapter->evict($key.'#epoch');
        $adapter->evict($key.'#started');

        $entry = $queryCache->getEntry($scope, 'users', []);
        $this->assertNull($entry === null ? null : $queryCache->get($entry), 'An evicted epoch must not bring back the initial epoch a result was filled under before the first write');
    }

    public function testAFillUnderARetiredEpochIsNeverServed(): void
    {
        $adapter = new RedisLeasableCache();
        $queryCache = new QueryCache(new Cache($adapter));
        $scope = new Scope(namespace: 'ns');
        $stale = $queryCache->getEntry($scope, 'users', []);
        $this->assertNotNull($stale);

        $queryCache->invalidateCollection($scope, 'users');
        $this->assertTrue($queryCache->set($stale, [new Document(['$id' => 'stale'])], $queryCache->getGeneration($stale)));

        $fresh = $queryCache->getEntry($scope, 'users', []);
        $this->assertNotNull($fresh);
        $this->assertSame($stale->field, $fresh->field);
        $this->assertNull($queryCache->get($fresh), 'A reader that resolved its entry before an invalidation fills the old epoch, which the new one must not serve');
    }

    public function testQueriesOfOneCollectionKeepTheirOwnResultsOnACacheWithFields(): void
    {
        $queryCache = new QueryCache(new Cache(new RedisLeasableCache()));
        $scope = new Scope(namespace: 'ns');
        $first = $queryCache->getEntry($scope, 'users', [Query::limit(1)], 'role:user-a');
        $second = $queryCache->getEntry($scope, 'users', [Query::limit(2)], 'role:user-b');
        $this->assertNotNull($first);
        $this->assertNotNull($second);

        $this->assertTrue($queryCache->set($first, [new Document(['$id' => 'private-a'])], $queryCache->getGeneration($first)));
        $this->assertTrue($queryCache->set($second, [new Document(['$id' => 'private-b'])], $queryCache->getGeneration($second)));

        $this->assertSame(['private-a'], $this->ids($queryCache->get($first) ?? []));
        $this->assertSame(['private-b'], $this->ids($queryCache->get($second) ?? []));
    }

    public function testOverlappingInvalidationsSucceedOnACacheWithoutFields(): void
    {
        $queryCache = new QueryCache(new Cache(new Memory()));
        $key = $queryCache->getCollectionKey(new Scope(), 'users');

        $queryCache->blockCollection($key, 'first');
        $queryCache->blockCollection($key, 'second');
        $queryCache->activateCollection($key, 'first');
        $this->assertNull($queryCache->getEntry(new Scope(), 'users', []), 'The second writer is still in flight');
        $queryCache->activateCollection($key, 'second');

        $this->assertNull(
            $queryCache->getEntry(new Scope(), 'users', []),
            'A cache without generations cannot prove a new epoch fresh, so it stays blocked',
        );
    }

    public function testActivationRejectsACorruptedOwnerRegistration(): void
    {
        $adapter = new RedisLeasableCache();
        $queryCache = new QueryCache(new Cache($adapter));
        $key = $queryCache->getCollectionKey(new Scope(), 'users');
        $adapter->corruptFieldWrites();
        $queryCache->blockCollection($key, 'owner');

        try {
            $queryCache->activateCollection($key, 'owner');
            $this->fail('A corrupted owner registration was accepted');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('Invalid query cache owner', $error->getMessage());
        }

        $this->assertNull($queryCache->getEntry(new Scope(), 'users', []));
    }

    public function testActivationPropagatesAnOwnerReleaseFailure(): void
    {
        $adapter = new RedisLeasableCache();
        $queryCache = new QueryCache(new Cache($adapter));
        $key = $queryCache->getCollectionKey(new Scope(), 'users');
        $queryCache->blockCollection($key, 'owner');
        $adapter->failFieldPurges();

        try {
            $queryCache->activateCollection($key, 'owner');
            $this->fail('An owner release failure was not propagated');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('Failed to release query cache owner', $error->getMessage());
        }

        $this->assertNull($queryCache->getEntry(new Scope(), 'users', []));
    }

    public function testInvalidationPropagatesACacheWriteFailure(): void
    {
        $queryCache = new QueryCache(new class (new RedisLeasableCache()) extends Cache {
            #[\Override]
            public function save(string $key, mixed $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                return $hash === '' ? false : parent::save($key, $data, $hash);
            }
        });

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Failed to block query cache epoch');
        $queryCache->invalidateCollection(new Scope(), 'users');
    }

    public function testAFlushedWriterLeavesAnotherWritersTombstoneInPlace(): void
    {
        $adapter = new OwnershipCache();
        $queryCache = new QueryCache(new Cache($adapter));
        $scope = new Scope(namespace: 'ns');
        $key = $queryCache->getCollectionKey($scope, 'users');

        $queryCache->blockCollection($key, 'first');
        $this->assertTrue($adapter->flush());
        $queryCache->blockCollection($key, 'second');

        $queryCache->activateCollection($key, 'first');

        $this->assertNull($queryCache->getEntry($scope, 'users', []), 'A writer whose registration was flushed away must not enable the cache while another writer is in flight');

        $queryCache->activateCollection($key, 'second');

        $this->assertNotNull($queryCache->getEntry($scope, 'users', []));
    }

    public function testAnOwnerReleasedByAConcurrentFlushIsNotReported(): void
    {
        $cache = new class (new RedisLeasableCache()) extends Cache {
            private bool $armed = false;

            public function flushOnNextFieldPurge(): void
            {
                $this->armed = true;
            }

            #[\Override]
            public function purge(string $key, string $hash = ''): bool
            {
                if ($this->armed && $hash !== '') {
                    $this->armed = false;
                    $this->flush();
                }

                return parent::purge($key, $hash);
            }
        };
        $queryCache = new QueryCache($cache);
        $scope = new Scope(namespace: 'ns');
        $key = $queryCache->getCollectionKey($scope, 'users');
        $queryCache->blockCollection($key, 'owner');
        $cache->flushOnNextFieldPurge();

        $queryCache->activateCollection($key, 'owner');

        $entry = $queryCache->getEntry($scope, 'users', []);
        $this->assertNotNull($entry, 'An owner that a flush removed before its release must still publish a fresh epoch');
        $this->assertTrue($queryCache->set($entry, [new Document(['$id' => 'fresh'])], $queryCache->getGeneration($entry)));
        $this->assertSame(['fresh'], $this->ids($queryCache->get($entry) ?? []));
    }

    public function testInvalidationPropagatesAnOwnerRegistrationFailure(): void
    {
        $queryCache = new QueryCache(new class (new RedisLeasableCache()) extends Cache {
            #[\Override]
            public function save(string $key, mixed $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                return $hash === '' ? parent::save($key, $data, $hash) : false;
            }
        });
        $before = $queryCache->getEntry(new Scope(), 'users', []);
        $this->assertNotNull($before);
        $this->assertTrue($queryCache->set($before, [new Document(['$id' => 'cached'])], $queryCache->getGeneration($before)));

        try {
            $queryCache->invalidateCollection(new Scope(), 'users');
            $this->fail('An owner registration failure was not propagated');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('Failed to register query cache owner', $error->getMessage());
        }

        $after = $queryCache->getEntry(new Scope(), 'users', []);
        $this->assertNotNull($after, 'A write whose owner was not registered must not block the epoch');
        $this->assertSame(['cached'], $this->ids($queryCache->get($after) ?? []));
    }

    public function testFlushFailureIsReported(): void
    {
        $cache = self::createStub(Cache::class);
        $cache->method('flush')->willReturn(false);
        $queryCache = new QueryCache($cache);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Failed to flush query cache');
        $queryCache->flush();
    }

    /**
     * @param  array<string>  $collections
     */
    private function assertInvalidatorInvalidates(Event $event, mixed $data, array $collections): void
    {
        $queryCache = new QueryCache(new Cache(new RedisLeasableCache()));
        $action = function () use ($queryCache, $event, $data): void {
            (new Invalidator($queryCache))->handle($event, $data);
        };

        foreach ($collections as $collection) {
            $action = function () use ($queryCache, $collection, $action): void {
                $this->assertRetired($queryCache, new Scope(), $collection, $action);
            };
        }

        $action();
    }

    private function assertRetired(QueryCache $queryCache, Scope $scope, string $collection, callable $action): void
    {
        $before = $queryCache->getEntry($scope, $collection, []);
        $this->assertNotNull($before);
        $this->assertTrue($queryCache->set($before, [new Document(['$id' => 'stale'])], $queryCache->getGeneration($before)));
        $this->assertSame(['stale'], $this->ids($queryCache->get($before) ?? []));

        $action();

        $after = $queryCache->getEntry($scope, $collection, []);
        $this->assertNotNull($after, "The invalidation must leave '{$collection}' usable");
        $this->assertNull($queryCache->get($after), "The invalidation must retire what '{$collection}' filled before it");
        $this->assertTrue($queryCache->set($after, [new Document(['$id' => 'fresh'])], $queryCache->getGeneration($after)));
        $this->assertSame(['fresh'], $this->ids($queryCache->get($after) ?? []), "The invalidation must publish a fresh epoch for '{$collection}'");
    }

    /**
     * @param  array<string>  $collections
     */
    private function assertKept(QueryCache $queryCache, array $collections, callable $action): void
    {
        foreach ($collections as $collection) {
            $before = $queryCache->getEntry(new Scope(), $collection, []);
            $this->assertNotNull($before);
            $this->assertTrue($queryCache->set($before, [new Document(['$id' => 'cached'])], $queryCache->getGeneration($before)));
        }

        $action();

        foreach ($collections as $collection) {
            $after = $queryCache->getEntry(new Scope(), $collection, []);
            $this->assertNotNull($after, "The event must leave '{$collection}' usable");
            $this->assertSame(['cached'], $this->ids($queryCache->get($after) ?? []), "The event must not retire what '{$collection}' filled");
        }
    }

    private static function attached(QueryCache $queryCache, string $name = 'default', int $writerTimeout = 3600): QueryCache
    {
        (new Database(new DatabaseMemory(), new Cache(new None())))
            ->setCacheName($name)
            ->setCacheWriterTimeout($writerTimeout)
            ->setQueryCache($queryCache);

        return $queryCache;
    }

    private static function createCache(): Cache&Stub
    {
        $cache = self::createStub(Cache::class);
        $cache->method('getGeneration')->willReturn('0');

        return $cache;
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
