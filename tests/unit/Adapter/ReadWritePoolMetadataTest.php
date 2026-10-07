<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\ReadWritePool;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;
use Utopia\Query\Schema\ColumnType;

final class ReadWritePoolMetadataTest extends TestCase
{
    private const string DATABASE = 'replication';

    private const string NAMESPACE = 'replication';

    private const int STICKY_MILLISECONDS = 200;

    /**
     * @return iterable<string, array{non-empty-string, array<mixed>}>
     */
    public static function metadataCalls(): iterable
    {
        $calls = [
            'supports' => [Capability::IndexKey],
            'capabilities' => [],
            'getAttributeWidth' => [new Document()],
            'getCountOfAttributes' => [new Document()],
            'getCountOfIndexes' => [new Document()],
            'builder' => ['posts'],
            'schema' => [],
            'getColumnType' => [Attribute::string(key: 'title', size: 255)],
            'encode' => [[1, 2], ColumnType::Point],
            'decode' => ['POINT(1 2)', ColumnType::Point],
            'castBefore' => [new Document(), new Document()],
            'castAfter' => [new Document(), [new Document()]],
            'castDatetime' => ['2026-09-23 00:00:00'],
        ];

        foreach ($calls as $method => $args) {
            yield $method => [$method, $args];
        }
    }

    /**
     * @return iterable<string, array{non-empty-string, array<mixed>}>
     */
    public static function callsThatNeedThePrimary(): iterable
    {
        $calls = [
            'getDriver' => [],
            'getSequences' => [new Document(['$id' => 'posts']), []],
            'analyzeCollection' => ['posts'],
            'startTransaction' => [],
            'commitTransaction' => [],
            'rollbackTransaction' => [],
        ];

        foreach ($calls as $method => $args) {
            yield $method => [$method, $args];
        }
    }

    /**
     * @param  non-empty-string  $method
     * @param  array<mixed>  $args
     */
    #[DataProvider('metadataCalls')]
    public function testMetadataCallIsAnsweredWhereReadsGoWithoutOpeningTheStickyWindow(string $method, array $args): void
    {
        $primary = $this->createMock(CastingAdapterStub::class);
        $replica = $this->createMock(CastingAdapterStub::class);
        $pool = $this->createPool($primary, $replica);

        $replica->expects($this->once())->method($method);
        $primary->expects($this->never())->method($method);
        $replica->expects($this->once())->method('ping')->willReturn(true);
        $primary->expects($this->never())->method('ping');

        $pool->delegate($method, $args);

        $this->assertTrue($pool->ping(), "{$method}() sent the next read to the primary");
    }

    /**
     * The pool asks each connection it borrows whether it takes timeouts, so the delegated question is told apart
     * from that probe by the feature it names.
     */
    public function testFeatureQuestionIsAnsweredWhereReadsGoWithoutOpeningTheStickyWindow(): void
    {
        $primary = $this->createMock(CastingAdapterStub::class);
        $replica = $this->createMock(CastingAdapterStub::class);
        $pool = $this->createPool($primary, $replica);

        $asked = [];
        $replica->method('hasFeature')->willReturnCallback(static function (string $feature) use (&$asked): bool {
            $asked[] = $feature;

            return false;
        });
        $primary->expects($this->never())->method('hasFeature');
        $replica->expects($this->once())->method('ping')->willReturn(true);
        $primary->expects($this->never())->method('ping');

        $pool->delegate('hasFeature', [Feature\Spatial::class]);

        $this->assertTrue($pool->ping(), 'hasFeature() sent the next read to the primary');
        $this->assertSame([Feature\Spatial::class], \array_values(\array_filter(
            $asked,
            static fn (string $feature): bool => $feature !== Feature\Timeouts::class,
        )));
    }

    /**
     * @param  non-empty-string  $method
     * @param  array<mixed>  $args
     */
    #[DataProvider('callsThatNeedThePrimary')]
    public function testCallThatNeedsThePrimaryRunsThereAndOpensTheStickyWindow(string $method, array $args): void
    {
        $primary = $this->createMock(CastingAdapterStub::class);
        $replica = $this->createMock(CastingAdapterStub::class);
        $pool = $this->createPool($primary, $replica);

        $primary->expects($this->once())->method($method);
        $replica->expects($this->never())->method($method);
        $primary->expects($this->once())->method('ping')->willReturn(true);
        $replica->expects($this->never())->method('ping');

        $pool->delegate($method, $args);

        $this->assertTrue($pool->ping(), "{$method}() did not keep the next read on the primary");
    }

    public function testHostnameComesFromTheWritePoolWithoutOpeningTheStickyWindow(): void
    {
        $primary = $this->createMock(CastingAdapterStub::class);
        $replica = $this->createMock(CastingAdapterStub::class);

        $primary->expects($this->exactly(3))->method('hostname')->willReturn('primary');
        $replica->expects($this->never())->method('hostname');
        $primary->method('createDocument')->willReturn(new Document());
        $primary->method('withTransaction')->willReturnCallback(
            static fn (callable $callback): mixed => $callback(),
        );
        $primary->method('withTenant')->willReturnCallback(
            static fn (int|string|null $tenant, callable $callback): mixed => $callback(),
        );
        $replica->expects($this->once())->method('ping')->willReturn(true);
        $primary->expects($this->never())->method('ping');

        $outside = $this->createPool($primary, $replica);
        $this->assertSame('primary', $outside->hostname());
        $this->assertTrue($outside->ping(), 'Naming the host sent the next read to the primary');

        $inside = $this->createPool($primary, $replica);
        $inside->createDocument(new Document(), new Document());
        $this->assertSame('primary', $inside->hostname(), 'Inside the sticky window the hostname must still name the write pool');

        $pinned = $this->createPool($primary, $replica);
        $this->assertSame('primary', $pinned->withTransaction(static fn (): string => $pinned->hostname()));
    }

    public function testHostnameIsLookedUpOncePerHandle(): void
    {
        $primary = $this->createMock(CastingAdapterStub::class);
        $replica = $this->createMock(CastingAdapterStub::class);
        $pool = $this->createPool($primary, $replica);

        $primary->expects($this->once())->method('hostname')->willReturn('primary');
        $replica->expects($this->never())->method('hostname');

        $this->assertSame('primary', $pool->hostname());
        $this->assertSame('primary', $pool->hostname());
    }

    public function testReadsReachTheReplicaAfterTheStickyWindowWhileCacheKeysNameTheHost(): void
    {
        $database = $this->createReplicatedDatabase(new HostnameSQLite('primary'), new HostnameSQLite('replica'));
        $database->setQueryCache(new QueryCache(new Cache(new MemoryCache())));

        $this->assertSame(
            'primary',
            $database->getHostname(),
            'The cache keys must name the host, as they do on MariaDB, MySQL, PostgreSQL and MongoDB',
        );

        $database->createDocument('posts', new Document(['$id' => 'draft', 'server' => 'primary']));

        $this->assertFalse(
            $database->getDocument('posts', 'draft')->isEmpty(),
            'A read inside the sticky window was served by a replica that has not received the write',
        );

        \usleep((self::STICKY_MILLISECONDS + 50) * 1000);

        $this->assertSame(
            'replica',
            $database->getDocument('posts', 'post')->getAttribute('server'),
            'getDocument() computed its cache key through getHostname(), which kept the read on the primary',
        );
        $this->assertSame(
            ['replica', 'replica'],
            $this->servers($database->find('posts')),
            'find() computed its cache key through getHostname(), which kept the read on the primary',
        );
    }

    public function testLimitsAreAnsweredWhereReadsGoWithoutOpeningTheStickyWindow(): void
    {
        $primary = $this->createMock(CastingAdapterStub::class);
        $replica = $this->createMock(CastingAdapterStub::class);
        $pool = $this->createPool($primary, $replica);
        $limits = (new Memory())->limits();

        $replica->expects($this->once())->method('limits')->willReturn($limits);
        $primary->expects($this->never())->method('limits');
        $replica->expects($this->once())->method('ping')->willReturn(true);
        $primary->expects($this->never())->method('ping');

        $this->assertSame($limits, $pool->limits());
        $this->assertTrue($pool->ping(), 'limits() sent the next read to the primary');
    }

    public function testConfiguringTheSchemalessModeKeepsReadsOnTheReplica(): void
    {
        $database = $this->createReplicatedDatabase($this->schemalessMemory(), $this->schemalessMemory());

        $database->setSchemaless(false);

        $this->assertSame(
            'replica',
            $database->getDocument('posts', 'post')->getAttribute('server'),
            'setSchemaless() sent the next read to the primary',
        );
    }

    private function schemalessMemory(): Memory
    {
        return new class () extends Memory implements Feature\Schemaless {
            private bool $schemaless = false;

            public function setSchemaless(bool $schemaless): static
            {
                $this->schemaless = $schemaless;

                return $this;
            }

            public function isSchemaless(): bool
            {
                return $this->schemaless;
            }
        };
    }

    public function testCastingKeepsReadsOnTheReplica(): void
    {
        $database = $this->createReplicatedDatabase(new CastingMemory(), new CastingMemory());

        $post = $database->getDocument('posts', 'post');

        $this->assertSame('replica', $post->getAttribute('server'), 'castAfter() sent the read to the primary');
        $this->assertSame(
            ['replica', 'replica'],
            $this->servers($database->find('posts', [Query::greaterThan('$createdAt', '2000-01-01T00:00:00.000+00:00')])),
            'castDatetime() sent the read to the primary',
        );
        $this->assertSame(
            ['replica'],
            $this->servers($database->find('posts', [Query::cursorAfter($post)])),
            'castBefore() sent the read to the primary',
        );
    }

    private function createReplicatedDatabase(Adapter $primary, Adapter $replica): Database
    {
        $this->seed($primary, 'primary');
        $this->seed($replica, 'replica');

        $pool = new ReadWritePool($this->createConnections($primary), $this->createConnections($replica));
        $pool->setStickyDuration(self::STICKY_MILLISECONDS);

        $database = new Database($pool, new Cache(new MemoryCache()));
        $database
            ->setDatabase(self::DATABASE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());

        return $database;
    }

    private function seed(Adapter $adapter, string $server): void
    {
        $database = new Database($adapter, new Cache(new NoCache()));
        $database
            ->setDatabase(self::DATABASE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $database->create();
        $database->createCollection(Collection::create(
            id: 'posts',
            attributes: [Attribute::string(key: 'server', size: 32)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
            documentSecurity: false,
        ));

        foreach (['post', 'page'] as $id) {
            $database->createDocument('posts', new Document(['$id' => $id, 'server' => $server]));
        }
    }

    /**
     * @param  CastingAdapterStub&MockObject  $primary
     * @param  CastingAdapterStub&MockObject  $replica
     */
    private function createPool(CastingAdapterStub $primary, CastingAdapterStub $replica): ReadWritePool
    {
        $pool = new ReadWritePool($this->createConnections($primary), $this->createConnections($replica));
        $pool->setAuthorization(new Authorization());

        return $pool;
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
     * @param  array<Document>  $documents
     * @return list<mixed>
     */
    private function servers(array $documents): array
    {
        return \array_map(
            static fn (Document $document): mixed => $document->getAttribute('server'),
            \array_values($documents),
        );
    }
}
