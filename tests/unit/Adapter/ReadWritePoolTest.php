<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Runtime;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\ReadWritePool;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Write;
use Utopia\Database\Index;
use Utopia\Database\Profiler\QueryProfiler;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

use function Swoole\Coroutine\run;

class ReadWritePoolTest extends TestCase
{
    /** @var UtopiaPool<Adapter>&Stub */
    private UtopiaPool $writePool;

    /** @var UtopiaPool<Adapter>&Stub */
    private UtopiaPool $readPool;

    private ReadWritePool $pool;

    /** @var FeatureAdapterStub&Stub */
    private Adapter $writeAdapter;

    /** @var FeatureAdapterStub&Stub */
    private Adapter $readAdapter;

    protected function setUp(): void
    {
        $this->writeAdapter = self::createStub(FeatureAdapterStub::class);
        $this->readAdapter = self::createStub(FeatureAdapterStub::class);

        $this->writePool = self::createStub(UtopiaPool::class);
        $this->readPool = self::createStub(UtopiaPool::class);

        $this->writePool->method('use')->willReturnCallback(function (callable $callback) {
            return $callback($this->writeAdapter);
        });

        $this->readPool->method('use')->willReturnCallback(function (callable $callback) {
            return $callback($this->readAdapter);
        });

        $this->pool = new ReadWritePool($this->writePool, $this->readPool);
        $this->pool->setAuthorization(new Authorization());
    }

    private function mockWriteAdapter(): FeatureAdapterStub&MockObject
    {
        $adapter = $this->createMock(FeatureAdapterStub::class);
        $this->writeAdapter = $adapter;

        return $adapter;
    }

    private function mockReadAdapter(): FeatureAdapterStub&MockObject
    {
        $adapter = $this->createMock(FeatureAdapterStub::class);
        $this->readAdapter = $adapter;

        return $adapter;
    }

    public function testReadMethodsRouteToReadPool(): void
    {
        $readAdapter = $this->mockReadAdapter();
        $readMethods = [
            'find',
            'getDocument',
            'count',
            'sum',
            'exists',
            'list',
            'getSchemaAttributes',
            'getSchemaIndexes',
            'builder',
            'schema',
            'getColumnType',
            'encode',
            'decode',
            'getSizeOfCollection',
            'getSizeOfCollectionOnDisk',
            'ping',
            'getConnectionId',
            'getDocumentSizeLimit',
            'getAttributeWidth',
            'getCountOfAttributes',
            'getCountOfIndexes',
            'getCountOfDefaultAttributes',
            'getCountOfDefaultIndexes',
            'getLimitForString',
            'getLimitForInt',
            'getLimitForBigInt',
            'getLimitForAttributes',
            'getLimitForIndexes',
            'getMaxIndexLength',
            'getMaxVarcharLength',
            'getMaxUIDLength',
            'getMinDateTime',
            'getIdAttributeType',
            'getKeywords',
            'getInternalIndexesKeys',
            'supports',
            'capabilities',
            'hasFeature',
        ];

        foreach ($readMethods as $method) {
            $readAdapter->expects($this->atLeastOnce())
                ->method($method)
                ->willReturn($this->getDefaultReturnForMethod($method));
        }

        foreach ($readMethods as $method) {
            $args = $this->getDefaultArgsForMethod($method);
            $this->pool->delegate($method, $args);
        }
    }

    public function testWriteMethodRoutesToWritePool(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $writeAdapter->expects($this->once())
            ->method('createDocument')
            ->willReturn(new Document());

        $this->pool->delegate('createDocument', [new Document(), new Document()]);
    }

    public function testDeleteDocumentRoutesToWritePool(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $writeAdapter->expects($this->once())
            ->method('deleteDocument')
            ->willReturn(true);

        $this->pool->delegate('deleteDocument', ['collection', 'id']);
    }

    public function testUpdateDocumentRoutesToWritePool(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $writeAdapter->expects($this->once())
            ->method('updateDocument')
            ->willReturn(new Document());

        $this->pool->delegate('updateDocument', [new Document(), 'id', new Document(), false]);
    }

    public function testCreateCollectionRoutesToWritePool(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $writeAdapter->expects($this->once())
            ->method('createCollection')
            ->willReturn(true);

        $this->pool->delegate('createCollection', ['testCollection', [], []]);
    }

    public function testStickyModeRoutesReadsToWritePoolAfterWrite(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $this->pool->setSticky(true);
        $this->pool->setStickyDuration(5000);

        $writeAdapter->expects($this->once())
            ->method('createDocument')
            ->willReturn(new Document());

        $this->pool->delegate('createDocument', [new Document(), new Document()]);

        $writeAdapter->expects($this->once())
            ->method('find')
            ->willReturn([]);

        $result = $this->pool->delegate('find', [new Document(), [], 25, 0, [], [], [], \Utopia\Query\CursorDirection::After, \Utopia\Database\PermissionType::Read]);
        $this->assertSame([], $result);
    }

    public function testStickyDurationExpiry(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $readAdapter = $this->mockReadAdapter();
        $this->pool->setSticky(true);
        $this->pool->setStickyDuration(1);

        $writeAdapter->expects($this->once())
            ->method('createDocument')
            ->willReturn(new Document());

        $this->pool->delegate('createDocument', [new Document(), new Document()]);

        usleep(2000);

        $readAdapter->expects($this->once())
            ->method('ping')
            ->willReturn(true);

        $result = $this->pool->delegate('ping', []);
        $this->assertTrue($result);
    }

    public function testReadReplicaReceivesWriteHooks(): void
    {
        // The permission side-table hook is a write hook, but Mongo decides whether to
        // apply its read-side permission filter by asking the adapter whether that hook
        // is present. A replica that never receives it answers no and reads unfiltered.
        $hook = new Permissions();
        $this->pool->addWriteHook($hook);

        $received = [];
        $this->readAdapter->method('getWriteHooks')->willReturnCallback(fn (): array => $received);
        $this->readAdapter->method('addWriteHook')->willReturnCallback(
            function (Write $hook) use (&$received): Adapter {
                $received[] = $hook;

                return $this->readAdapter;
            }
        );

        $this->pool->setSticky(false);
        $this->pool->find(new Document(['$id' => 'posts']), []);

        $this->assertSame([$hook], $received, 'read replica did not receive the pool\'s write hooks');
    }

    public function testStickyDisabledRoutesReadNormally(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $readAdapter = $this->mockReadAdapter();
        $this->pool->setSticky(false);

        $writeAdapter->expects($this->once())
            ->method('createDocument')
            ->willReturn(new Document());

        $this->pool->delegate('createDocument', [new Document(), new Document()]);

        $readAdapter->expects($this->once())
            ->method('ping')
            ->willReturn(true);

        $result = $this->pool->delegate('ping', []);
        $this->assertTrue($result);
    }

    public function testSetStickyDurationIsChainable(): void
    {
        $result = $this->pool->setStickyDuration(3000);
        $this->assertSame($this->pool, $result);
    }

    public function testSetStickyIsChainable(): void
    {
        $result = $this->pool->setSticky(true);
        $this->assertSame($this->pool, $result);
    }

    public function testReadAfterMultipleWritesStaysSticky(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $this->pool->setSticky(true);
        $this->pool->setStickyDuration(5000);

        $this->writeAdapter->method('createDocument')
            ->willReturn(new Document());
        $this->writeAdapter->method('deleteDocument')
            ->willReturn(true);

        $this->pool->delegate('createDocument', [new Document(), new Document()]);
        $this->pool->delegate('deleteDocument', ['collection', 'id']);

        $writeAdapter->expects($this->once())
            ->method('ping')
            ->willReturn(true);

        $result = $this->pool->delegate('ping', []);
        $this->assertTrue($result);
    }

    public function testReadBeforeAnyWriteGoesToReadPool(): void
    {
        $readAdapter = $this->mockReadAdapter();
        $this->pool->setSticky(true);
        $this->pool->setStickyDuration(5000);

        $readAdapter->expects($this->once())
            ->method('ping')
            ->willReturn(true);

        $result = $this->pool->delegate('ping', []);
        $this->assertTrue($result);
    }

    public function testPinnedAdapterResyncsTenantAndDatabaseBeforeDelegatedCall(): void
    {
        $writeAdapter = new Memory();
        $writeAdapter->setDatabase('old_db');
        $writeAdapter->setNamespace('old_ns');
        $writeAdapter->setTenant(1);

        $readAdapter = new Memory();

        $writePool = self::createStub(UtopiaPool::class);
        $writePool->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($writeAdapter),
        );

        $readPool = self::createStub(UtopiaPool::class);
        $readPool->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($readAdapter),
        );

        $pool = new ReadWritePool($writePool, $readPool);
        $pool->setAuthorization(new Authorization());
        $pool->setDatabase('old_db');
        $pool->setNamespace('old_ns');
        $pool->setTenant(1);

        $pool->withTransaction(function () use ($pool, $writeAdapter): void {
            $pool->setDatabase('new_db');
            $pool->setNamespace('new_ns');
            $pool->setTenant(2);

            $this->assertTrue($pool->ping());
            $this->assertSame('new_db', $writeAdapter->getDatabase());
            $this->assertSame('new_ns', $writeAdapter->getNamespace());
            $this->assertSame(2, $writeAdapter->getTenant());
        });
    }

    public function testReadAdapterClearsStaleTimeout(): void
    {
        /** @var Adapter&Feature\Timeouts&MockObject $readAdapter */
        $readAdapter = $this->createMock(FeatureAdapterStub::class);
        $readAdapter->expects($this->once())
            ->method('clearTimeout');
        $readAdapter->expects($this->once())
            ->method('ping')
            ->willReturn(true);

        $readPool = self::createStub(UtopiaPool::class);
        $readPool->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($readAdapter),
        );

        $pool = new ReadWritePool($this->writePool, $readPool);
        $pool->setAuthorization(new Authorization());

        $this->assertTrue($pool->delegate('ping', []));
    }

    public function testNonReadNonStandardMethodGoesToWritePool(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $writeAdapter->expects($this->once())
            ->method('createAttribute')
            ->willReturn(true);

        $attr = Attribute::string(key: 'test', size: 128);
        $this->pool->delegate('createAttribute', ['collection', $attr]);
    }

    public function testCreateIndexRoutesToWritePool(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $writeAdapter->expects($this->once())
            ->method('createIndex')
            ->willReturn(true);

        $index = Index::key(key: 'idx', attributes: ['col']);
        $this->pool->delegate('createIndex', ['collection', $index, [], []]);
    }

    public function testDeleteCollectionRoutesToWritePool(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $writeAdapter->expects($this->once())
            ->method('deleteCollection')
            ->willReturn(true);

        $this->pool->delegate('deleteCollection', ['collection']);
    }

    public function testRawMutationRoutesToWritePool(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $writeAdapter->expects($this->once())
            ->method('rawMutation')
            ->willReturn(1);

        $this->pool->delegate('rawMutation', ['UPDATE t SET a = 1', []]);
    }

    public function testReadAfterTransactionalWriteRoutesToWritePool(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $readAdapter = $this->mockReadAdapter();
        $this->writeAdapter->method('withTransaction')->willReturnCallback(
            static fn (callable $callback): mixed => $callback(),
        );
        $this->writeAdapter->method('withTenant')->willReturnCallback(
            static fn (int|string|null $tenant, callable $callback): mixed => $callback(),
        );
        $this->writeAdapter->method('createDocument')->willReturn(new Document());
        $writeAdapter->expects($this->once())->method('find')->willReturn([]);
        $readAdapter->expects($this->never())->method('find');

        $this->pool->withTransaction(fn (): Document => $this->pool->createDocument(new Document(), new Document()));

        $this->pool->find(new Document());
    }

    public function testStickinessRunsFromTheCommitRatherThanTheWrite(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $readAdapter = $this->mockReadAdapter();
        $this->pool->setStickyDuration(200);

        $this->writeAdapter->method('withTransaction')->willReturnCallback(
            static fn (callable $callback): mixed => $callback(),
        );
        $this->writeAdapter->method('withTenant')->willReturnCallback(
            static fn (int|string|null $tenant, callable $callback): mixed => $callback(),
        );
        $this->writeAdapter->method('createDocument')->willReturn(new Document());
        $writeAdapter->expects($this->once())->method('find')->willReturn([]);
        $readAdapter->expects($this->never())->method('find');

        $this->pool->withTransaction(function (): void {
            $this->pool->createDocument(new Document(), new Document());
            \usleep(250_000);
        });

        $this->pool->find(new Document());
    }

    public function testStickinessRunsFromTheEndOfAWrite(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $readAdapter = $this->mockReadAdapter();
        $this->pool->setStickyDuration(200);

        $this->writeAdapter->method('createDocument')->willReturnCallback(static function (): Document {
            \usleep(250_000);

            return new Document();
        });
        $writeAdapter->expects($this->once())->method('find')->willReturn([]);
        $readAdapter->expects($this->never())->method('find');

        $this->pool->createDocument(new Document(), new Document());

        $this->pool->find(new Document());
    }

    public function testGetDocumentForUpdateRoutesToWritePool(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $readAdapter = $this->mockReadAdapter();
        $writeAdapter->expects($this->exactly(3))->method('getDocument')->willReturn(new Document());
        $readAdapter->expects($this->never())->method('getDocument');

        $this->pool->setSticky(false);
        $this->pool->getDocument(new Document(), 'id', [], true);
        $this->pool->getDocument(new Document(), 'id', forUpdate: true);
        $this->pool->delegate('getDocument', ['collection' => new Document(), 'id' => 'id', 'forUpdate' => true]);
    }

    public function testGetDocumentWithoutLockRoutesToReadPool(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $readAdapter = $this->mockReadAdapter();
        $readAdapter->expects($this->once())->method('getDocument')->willReturn(new Document());
        $writeAdapter->expects($this->never())->method('getDocument');

        $this->pool->getDocument(new Document(), 'id', [], false);
    }

    public function testRawQueryRoutesToWritePool(): void
    {
        $writeAdapter = $this->mockWriteAdapter();
        $readAdapter = $this->mockReadAdapter();
        $writeAdapter->expects($this->once())->method('rawQuery')->willReturn([]);
        $readAdapter->expects($this->never())->method('rawQuery');

        $this->pool->setSticky(false);
        $this->pool->rawQuery('UPDATE posts SET title = ?', ['draft']);
    }

    public function testDocumentWrittenThroughDatabaseIsReadBackFromThePrimary(): void
    {
        $primary = new Memory();
        $replica = new Memory();
        $this->createSchema($primary);
        $this->createSchema($replica);

        $database = $this->createReplicatedDatabase($primary, $replica);
        $database->createDocument('posts', new Document(['$id' => 'post']));

        $this->assertFalse(
            $database->getDocument('posts', 'post')->isEmpty(),
            'A read straight after a committed write was served by a replica that has not received the row',
        );
    }

    public function testLockingReadThroughDatabaseIsServedByThePrimary(): void
    {
        $primary = new Memory();
        $replica = new Memory();
        $this->createSchema($primary)->createDocument('posts', new Document(['$id' => 'post']));
        $this->createSchema($replica);

        $database = $this->createReplicatedDatabase($primary, $replica);

        $this->assertFalse(
            $database->getDocument('posts', 'post', forUpdate: true)->isEmpty(),
            'A locking read was served by a replica, where the lock protects nothing',
        );
    }

    public function testReplicaDoesNotKeepTheProfilerAfterARead(): void
    {
        $replica = new ProfilerProbeAdapter();
        $pool = new ReadWritePool($this->createConnections(new Memory()), $this->createConnections($replica));
        $pool->setAuthorization(new Authorization());
        $profiler = new QueryProfiler();
        $pool->setProfiler($profiler);

        $this->assertTrue($pool->ping());

        $this->assertSame($profiler, $replica->profiled, 'The replica must profile the read it served');
        $this->assertNull($replica->getProfiler(), 'The replica kept the profiler of the handle that borrowed it');
    }

    private function createSchema(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new NoCache()));
        $database
            ->setDatabase('replication')
            ->setNamespace('replication')
            ->setAuthorization(new Authorization());
        $database->create();
        $database->createCollection(Collection::create(
            id: 'posts',
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
            documentSecurity: false,
        ));

        return $database;
    }

    private function createReplicatedDatabase(Adapter $primary, Adapter $replica): Database
    {
        $pool = new ReadWritePool($this->createConnections($primary), $this->createConnections($replica));

        $database = new Database($pool, new Cache(new NoCache()));
        $database
            ->setDatabase('replication')
            ->setNamespace('replication')
            ->setAuthorization(new Authorization());

        return $database;
    }

    /**
     * @return UtopiaPool<Adapter>
     */
    private function createConnections(Adapter $adapter): UtopiaPool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($adapter),
        );

        return $connections;
    }

    /**
     * @return mixed
     */
    private function getDefaultReturnForMethod(string $method): mixed
    {
        return match ($method) {
            'find', 'list' => [],
            'getDocument' => new Document(),
            'count', 'sum', 'getSizeOfCollection', 'getSizeOfCollectionOnDisk',
            'getDocumentSizeLimit', 'getAttributeWidth', 'getCountOfAttributes',
            'getCountOfIndexes', 'getCountOfDefaultAttributes', 'getCountOfDefaultIndexes',
            'getLimitForString', 'getLimitForInt', 'getLimitForBigInt',
            'getLimitForAttributes', 'getLimitForIndexes', 'getMaxIndexLength',
            'getMaxVarcharLength', 'getMaxUIDLength' => 0,
            'exists', 'ping', 'supports', 'hasFeature' => true,
            'getConnectionId', 'getIdAttributeType' => 'string',
            'getMinDateTime' => new \DateTime(),
            'getSchemaAttributes', 'getSchemaIndexes', 'getKeywords',
            'getInternalIndexesKeys', 'capabilities', 'decode' => [],
            'encode' => 'POINT(1 2)',
            'builder' => $this->createStub(\Utopia\Query\Builder::class),
            'schema' => $this->createStub(\Utopia\Query\Schema::class),
            'getColumnType' => 'VARCHAR(255)',
            default => null,
        };
    }

    /**
     * @return array<mixed>
     */
    private function getDefaultArgsForMethod(string $method): array
    {
        return match ($method) {
            'find' => [new Document(), [], 25, 0, [], [], [], \Utopia\Query\CursorDirection::After, \Utopia\Database\PermissionType::Read],
            'getDocument' => [new Document(), 'id', [], false],
            'count' => [new Document(), [], null],
            'sum' => [new Document(), 'attr', [], null],
            'exists' => ['db', null],
            'list' => [],
            'getSizeOfCollection', 'getSizeOfCollectionOnDisk' => ['collection'],
            'ping' => [],
            'getConnectionId' => [],
            'getDocumentSizeLimit' => [],
            'getAttributeWidth' => [new Document()],
            'getCountOfAttributes' => [new Document()],
            'getCountOfIndexes' => [new Document()],
            'getLimitForString', 'getLimitForInt', 'getLimitForBigInt',
            'getLimitForAttributes', 'getLimitForIndexes',
            'getMaxIndexLength', 'getMaxVarcharLength',
            'getMaxUIDLength' => [],
            'getMinDateTime' => [],
            'getIdAttributeType' => [],
            'supports' => [\Utopia\Database\Capability::Index],
            'hasFeature' => [Feature\Spatial::class],
            'getSchemaAttributes', 'getSchemaIndexes', 'builder' => ['collection'],
            'getColumnType' => [\Utopia\Database\Attribute::string(key: 'title', size: 255)],
            'encode' => [[1, 2], \Utopia\Query\Schema\ColumnType::Point],
            'decode' => ['wkb', \Utopia\Query\Schema\ColumnType::Point],
            default => [],
        };
    }

    public function testAReplicaReadFillsNoCache(): void
    {
        $primary = new Memory();
        $replica = new Memory();
        $this->createArticles($primary, 'v2');
        $replication = $this->createArticles($replica, 'v1');

        $pool = new ReadWritePool($this->createConnections($primary), $this->createConnections($replica));
        $database = new Database($pool, new Cache(new MemoryCache()));
        $database
            ->setDatabase('replication')
            ->setNamespace('replication')
            ->setAuthorization(new Authorization());
        $database->setQueryCache(new QueryCache(new Cache(new MemoryCache())));
        $titles = static fn (): array => \array_map(
            static fn (Document $article): mixed => $article->getAttribute('title'),
            $database->find('articles'),
        );

        $this->assertSame('v1', $database->getDocument('articles', 'article')->getAttribute('title'));
        $this->assertSame(['v1'], $titles());
        $this->assertTrue($database->getDocument('articles', 'created')->isEmpty());

        $replication->updateDocument('articles', 'article', new Document(['title' => 'v2']));
        $replication->createDocument('articles', new Document(['$id' => 'created', 'title' => 'v2']));

        $this->assertSame('v2', $database->getDocument('articles', 'article')->getAttribute('title'), 'A document a lagging replica served must not be cached for later reads');
        $this->assertSame(['v2', 'v2'], $titles(), 'A query result a lagging replica served must not be cached for later reads');
        $this->assertFalse($database->getDocument('articles', 'created')->isEmpty(), 'A miss a lagging replica served must not be cached for later reads');
    }

    public function testEachCoroutineReportsWhereItsOwnReadWasServed(): void
    {
        $this->readAdapter->method('getDocument')->willReturn(new Document());
        $this->writeAdapter->method('getDocument')->willReturn(new Document());
        $pool = $this->pool;
        $observed = null;
        $hookFlags = Runtime::getHookFlags();

        try {
            run(static function () use ($pool, &$observed): void {
                $read = new Channel(1);
                $locked = new Channel(1);

                Coroutine::create(static function () use ($pool, $read, $locked, &$observed): void {
                    $pool->getDocument(new Document(), 'id', [], false);
                    $read->push(true);
                    $locked->pop();
                    $observed = $pool->servedByReplica();
                });

                Coroutine::create(static function () use ($pool, $read, $locked): void {
                    $read->pop();
                    $pool->getDocument(new Document(), 'id', [], true);
                    $locked->push(true);
                });
            });
        } finally {
            Runtime::setHookFlags($hookFlags);
        }

        $this->assertTrue($observed, "A locking read on another coroutine must not change where this coroutine's read was served");
    }

    private function createArticles(Adapter $adapter, string $title): Database
    {
        $database = new Database($adapter, new Cache(new NoCache()));
        $database
            ->setDatabase('replication')
            ->setNamespace('replication')
            ->setAuthorization(new Authorization());
        $database->create();
        $database->createCollection(Collection::create(
            id: 'articles',
            attributes: [Attribute::string(key: 'title')],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: false,
        ));
        $database->createDocument('articles', new Document([
            '$id' => 'article',
            'title' => $title,
        ]));

        return $database;
    }
}
