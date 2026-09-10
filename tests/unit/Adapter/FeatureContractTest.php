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
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;

final class FeatureContractTest extends TestCase
{
    public function testMemoryPoolHasFeatureDelegatesToInnerAdapter(): void
    {
        $pool = $this->pool(new Memory());

        $this->assertSame(false, $pool->hasFeature(Feature\Spatial::class));
        $this->assertSame(false, $pool->hasFeature(Feature\Upserts::class));
        $this->assertSame(true, $pool->hasFeature(Feature\Relationships::class));
    }

    /**
     * Pool carries every optional Feature method so it can forward whichever
     * adapter the pool hands out, but it must not implement the interfaces:
     * a caller type-checking the facade would be told the pooled engine
     * supports something it does not. Support is answered by hasFeature(),
     * and a call the inner adapter cannot serve is refused at the facade.
     */
    public function testPoolRefusesAFeatureTheInnerAdapterLacks(): void
    {
        $implements = $this->interfaces(Pool::class);

        foreach ([
            Feature\ConnectionId::class,
            Feature\InternalCasting::class,
            Feature\Relationships::class,
            Feature\SchemaAttributes::class,
            Feature\SchemaIndexes::class,
            Feature\Spatial::class,
            Feature\Timeouts::class,
            Feature\Upserts::class,
            Feature\UTCCasting::class,
            Feature\RawQuery::class,
            Feature\QueryBuilder::class,
            Feature\ColumnTypes::class,
        ] as $feature) {
            $this->assertArrayNotHasKey($feature, $implements, $feature);
        }

        $pool = $this->pool(new Memory());

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support upserts');
        $pool->upsertDocuments(new Document(['$id' => 'any']), '', []);
    }

    public function testRedisAdvertisesUpsertsConnectionIdAndRelationships(): void
    {
        $implements = $this->interfaces(Redis::class);
        $this->assertArrayHasKey(Feature\Upserts::class, $implements);
        $this->assertArrayHasKey(Feature\ConnectionId::class, $implements);
        $this->assertArrayHasKey(Feature\Relationships::class, $implements);
        $this->assertArrayNotHasKey(Feature\Spatial::class, $implements);
        $this->assertArrayNotHasKey(Feature\RawQuery::class, $implements);
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

    private function pool(Adapter $adapter): Pool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(
            static fn (callable $callback): mixed => $callback($adapter),
        );

        $pool = new Pool($connections);
        $pool->setAuthorization(new Authorization());

        return $pool;
    }
}
