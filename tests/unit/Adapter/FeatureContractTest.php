<?php

namespace Tests\Unit\Adapter;

use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\Redis;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class FeatureContractTest extends TestCase
{
    public function testMemoryDoesNotAdvertiseOptionalEngineMethods(): void
    {
        $adapter = $this->instance(new Memory());

        foreach ([
            'rawQuery',
            'rawMutation',
            'getBuilder',
            'getSchema',
            'decodePoint',
            'getColumnType',
            'getSchemaAttributes',
            'castingBefore',
            'setUTCDatetime',
            'setTimeout',
            'upsertDocuments',
            'getConnectionId',
        ] as $method) {
            $this->assertSame(false, \method_exists($adapter, $method), $method);
        }

        foreach (['createRelationship'] as $method) {
            $this->assertSame(true, \method_exists($adapter, $method), $method);
        }

        $this->assertSame(true, $adapter instanceof Feature\Relationships);
        $this->assertSame(false, $adapter instanceof Feature\Upserts);
    }

    public function testMemoryHasFeatureReportsRelationshipsButNotSpatial(): void
    {
        $adapter = $this->instance(new Memory());

        $this->assertSame(false, $adapter->hasFeature(Feature\Spatial::class));
        $this->assertSame(true, $adapter->hasFeature(Feature\Relationships::class));
    }

    public function testMemoryPoolHasFeatureDelegatesToInnerAdapter(): void
    {
        $adapter = new Memory();

        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($adapter),
        );

        $pool = new Pool($connections);
        $pool->setAuthorization(new Authorization());

        $this->assertSame(false, $pool->hasFeature(Feature\Spatial::class));
        $this->assertSame(true, $pool->hasFeature(Feature\Relationships::class));
    }

    public function testRedisAdvertisesUpsertsConnectionIdAndRelationships(): void
    {
        $class = Redis::class;

        foreach ([
            'rawQuery',
            'rawMutation',
            'getBuilder',
            'getSchema',
            'decodePoint',
            'getColumnType',
            'getSchemaAttributes',
            'castingBefore',
            'setUTCDatetime',
            'setTimeout',
        ] as $method) {
            $this->assertSame(false, \method_exists($class, $method), $method);
        }

        foreach (['upsertDocuments', 'getConnectionId'] as $method) {
            $this->assertSame(true, \method_exists($class, $method), $method);
        }

        $implements = $this->interfaces($class);
        $this->assertArrayHasKey(Feature\Upserts::class, $implements);
        $this->assertArrayHasKey(Feature\ConnectionId::class, $implements);
        $this->assertArrayHasKey(Feature\Relationships::class, $implements);
    }

    public function testSQLiteImplementsSqlFeaturesButNotSpatialTimeoutsOrConnectionId(): void
    {
        $implements = $this->interfaces(SQLite::class);
        $this->assertArrayHasKey(Feature\Upserts::class, $implements);
        $this->assertArrayHasKey(Feature\Relationships::class, $implements);
        $this->assertArrayHasKey(Feature\SchemaAttributes::class, $implements);
        $this->assertArrayHasKey(Feature\SchemaIndexes::class, $implements);
        $this->assertArrayHasKey(Feature\RawQuery::class, $implements);
        $this->assertArrayHasKey(Feature\QueryBuilder::class, $implements);
        $this->assertArrayHasKey(Feature\ColumnTypes::class, $implements);
        $this->assertArrayNotHasKey(Feature\Spatial::class, $implements);
        $this->assertArrayNotHasKey(Feature\Timeouts::class, $implements);
        $this->assertArrayNotHasKey(Feature\ConnectionId::class, $implements);
    }

    public function testMariaDBImplementsSpatialTimeoutsConnectionIdAndSchemaIndexes(): void
    {
        $implements = $this->interfaces(MariaDB::class);
        $this->assertArrayHasKey(Feature\Spatial::class, $implements);
        $this->assertArrayHasKey(Feature\Timeouts::class, $implements);
        $this->assertArrayHasKey(Feature\ConnectionId::class, $implements);
        $this->assertArrayHasKey(Feature\SchemaIndexes::class, $implements);
    }

    public function testPostgresImplementsSpatialTimeoutsAndConnectionIdWithoutSchemaIntrospection(): void
    {
        $implements = $this->interfaces(Postgres::class);
        $this->assertArrayHasKey(Feature\Spatial::class, $implements);
        $this->assertArrayHasKey(Feature\Timeouts::class, $implements);
        $this->assertArrayHasKey(Feature\ConnectionId::class, $implements);
        $this->assertArrayNotHasKey(Feature\SchemaAttributes::class, $implements);
        $this->assertArrayNotHasKey(Feature\SchemaIndexes::class, $implements);
    }

    /**
     * @param class-string $class
     * @return array<string, string>
     */
    private function interfaces(string $class): array
    {
        $implements = \class_implements($class);

        return $implements === false ? [] : $implements;
    }

    private function instance(Adapter $adapter): Adapter
    {
        return $adapter;
    }
}
