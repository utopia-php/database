<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\MySQL;
use Utopia\Database\Adapter\Pool;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Validator\Authorization;
use Utopia\Pools\Pool as UtopiaPool;
use Utopia\Query\Schema\ColumnType;

final class SpatialCodecTest extends TestCase
{
    /**
     * @return iterable<string, array{Adapter&Feature\Spatial}>
     */
    public static function spatialAdapters(): iterable
    {
        yield 'MariaDB' => [new MariaDB(new stdClass())];
        yield 'MySQL' => [new MySQL(new stdClass())];
        yield 'Postgres' => [new Postgres(new stdClass())];
    }

    /**
     * @return iterable<string, array{ColumnType, array<mixed>, string}>
     */
    public static function geometries(): iterable
    {
        yield 'point' => [ColumnType::Point, [1.5, 2], 'POINT(1.5 2)'];
        yield 'linestring' => [ColumnType::Linestring, [[0, 0], [1, 1], [2, 0.5]], 'LINESTRING(0 0, 1 1, 2 0.5)'];
        yield 'polygon with a hole' => [
            ColumnType::Polygon,
            [[[0, 0], [0, 4], [4, 4], [4, 0], [0, 0]], [[1, 1], [1, 2], [2, 2], [1, 1]]],
            'POLYGON((0 0, 0 4, 4 4, 4 0, 0 0), (1 1, 1 2, 2 2, 1 1))',
        ];
        yield 'polygon given as one ring' => [ColumnType::Polygon, [[0, 0], [0, 1], [1, 1], [0, 0]], 'POLYGON((0 0, 0 1, 1 1, 0 0))'];
    }

    /**
     * @return iterable<string, array{Adapter&Feature\Spatial, ColumnType, array<mixed>, string}>
     */
    public static function adaptersAndGeometries(): iterable
    {
        foreach (self::spatialAdapters() as $engine => [$adapter]) {
            foreach (self::geometries() as $shape => [$type, $value, $text]) {
                yield $shape.' on '.$engine => [$adapter, $type, $value, $text];
            }
        }
    }

    /**
     * @param  array<mixed>  $value
     */
    #[DataProvider('adaptersAndGeometries')]
    public function testAGeometryIsEncodedAsWellKnownText(Adapter&Feature\Spatial $adapter, ColumnType $type, array $value, string $text): void
    {
        $this->assertSame($text, $adapter->encode($value, $type));
    }

    /**
     * @param  array<mixed>  $value
     */
    #[DataProvider('adaptersAndGeometries')]
    public function testAnEncodedGeometryDecodesToItsCoordinates(Adapter&Feature\Spatial $adapter, ColumnType $type, array $value, string $text): void
    {
        $expected = $type === ColumnType::Polygon && \is_numeric($value[0][0] ?? null) ? [$value] : $value;

        $this->assertSame(self::floats($expected), $adapter->decode($adapter->encode($value, $type), $type));
    }

    #[DataProvider('spatialAdapters')]
    public function testAnInvalidGeometryIsRefused(Adapter&Feature\Spatial $adapter): void
    {
        $this->expectException(StructureException::class);

        $adapter->encode([1], ColumnType::Point);
    }

    #[DataProvider('spatialAdapters')]
    public function testANonSpatialTypeIsRefused(Adapter&Feature\Spatial $adapter): void
    {
        $this->expectException(DatabaseException::class);

        $adapter->decode('POINT(1 2)', ColumnType::String);
    }

    public function testAPoolCodesThroughItsConnection(): void
    {
        $pool = $this->pool(new MariaDB(new stdClass()));

        $this->assertTrue($pool->hasFeature(Feature\Spatial::class));
        $this->assertSame('POINT(3 4)', $pool->encode([3, 4], ColumnType::Point));
        $this->assertSame([3.0, 4.0], $pool->decode('POINT(3 4)', ColumnType::Point));
    }

    public function testAPoolOverAnAdapterWithoutSpatialRefusesToCode(): void
    {
        $pool = $this->pool(new SQLite(new PDO('sqlite::memory:')));

        $this->assertFalse($pool->hasFeature(Feature\Spatial::class));
        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Adapter does not support spatial');

        $pool->encode([3, 4], ColumnType::Point);
    }

    public function testTheDatabaseEncodesASpatialAttributeThroughItsAdapter(): void
    {
        $database = new Database(new MariaDB(new stdClass()), new Cache(new None()));
        $collection = Collection::create(id: 'places', attributes: [Attribute::point(key: 'location')]);

        $encoded = $database->encode($collection, new Document(['$id' => 'home', 'location' => [5, 6]]));

        $this->assertSame('POINT(5 6)', $encoded->getAttribute('location'));
    }

    public function testWithoutSpatialTheDatabaseLeavesTheValueAsGiven(): void
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $collection = Collection::create(id: 'places', attributes: [Attribute::point(key: 'location')]);

        $encoded = $database->encode($collection, new Document(['$id' => 'home', 'location' => [5, 6]]));

        $this->assertSame([5, 6], $encoded->getAttribute('location'));
    }

    /**
     * @param  array<mixed>  $value
     * @return array<mixed>
     */
    private static function floats(array $value): array
    {
        return \array_map(static fn (mixed $node): mixed => \is_array($node) ? self::floats($node) : (float) $node, $value);
    }

    private function pool(Adapter $adapter): Pool
    {
        /** @var UtopiaPool<Adapter>&Stub $connections */
        $connections = self::createStub(UtopiaPool::class);
        $connections->method('use')->willReturnCallback(static fn (callable $callback): mixed => $callback($adapter));

        $pool = new Pool($connections);
        $pool->setAuthorization(new Authorization());

        return $pool;
    }
}
