<?php

namespace Tests\Unit\Builder;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Builder\Postgres;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;

final class PostgresDistanceTest extends TestCase
{
    public function testDistanceLessThanInDegreesLeadsWithDWithin(): void
    {
        $condition = (new Postgres())->compileFilters([self::typed(Query::distanceLessThan('location', [10, 20], 5), ColumnType::Point)]);

        $this->assertSame('(ST_DWithin("location", ST_GeomFromText(?, 4326), ?) AND ST_Distance("location", ST_GeomFromText(?, 4326)) < ?)', $condition->expression);
        $this->assertSame(['POINT(10 20)', 5.0, 'POINT(10 20)', 5.0], $condition->bindings);
    }

    public function testDistanceLessThanInMetersFromAPointLeadsWithADegreeBox(): void
    {
        $condition = (new Postgres())->compileFilters([self::typed(Query::distanceLessThan('location', [10, 20], 1000, true), ColumnType::Point)]);

        $this->assertSame('("location" && ST_Expand(ST_GeomFromText(?, 4326), ?, ?) AND ST_Distance(("location"::geography), ST_SetSRID(ST_GeomFromText(?), 4326)::geography) < ?)', $condition->expression);
        $this->assertCount(5, $condition->bindings);
        $this->assertSame('POINT(10 20)', $condition->bindings[0]);
        $this->assertSame('POINT(10 20)', $condition->bindings[3]);
        $this->assertSame(1000.0, $condition->bindings[4]);

        $latitudeDegrees = $condition->bindings[2];
        $longitudeDegrees = $condition->bindings[1];
        $this->assertIsFloat($latitudeDegrees);
        $this->assertIsFloat($longitudeDegrees);
        $this->assertEqualsWithDelta(1000 / 110574, $latitudeDegrees, 1e-12);
        $this->assertGreaterThan(1000 / 111319, $longitudeDegrees, 'Away from the equator a degree of longitude is shorter, so the box must be wider');
    }

    /**
     * @return array<string, array{Query}>
     */
    public static function exactOnly(): array
    {
        return [
            'a line column' => [self::typed(Query::distanceLessThan('location', [10, 20], 1000, true), ColumnType::Linestring)],
            'a polygon value' => [self::typed(Query::distanceLessThan('location', [[[0, 0], [0, 1], [1, 1], [0, 0]]], 1000, true), ColumnType::Point)],
            'a box reaching a pole' => [self::typed(Query::distanceLessThan('location', [10, 89.99], 5000, true), ColumnType::Point)],
            'a box reaching the antimeridian' => [self::typed(Query::distanceLessThan('location', [179.99, 0], 5000, true), ColumnType::Point)],
            'a distance that is not a number' => [self::typed(Query::distanceLessThan('location', [10, 20], NAN, true), ColumnType::Point)],
            'an infinite distance' => [self::typed(Query::distanceLessThan('location', [10, 20], INF, true), ColumnType::Point)],
            'a negatively infinite distance' => [self::typed(Query::distanceLessThan('location', [10, 20], -INF, true), ColumnType::Point)],
        ];
    }

    #[DataProvider('exactOnly')]
    public function testDistanceLessThanInMetersKeepsTheExactCheckOnlyWhenNoBoxHoldsTheRange(Query $query): void
    {
        $condition = (new Postgres())->compileFilters([$query]);

        $this->assertSame('ST_Distance(("location"::geography), ST_SetSRID(ST_GeomFromText(?), 4326)::geography) < ?', $condition->expression);
        $this->assertCount(2, $condition->bindings);
    }

    public function testDistanceGreaterThanIsUnchanged(): void
    {
        $condition = (new Postgres())->compileFilters([self::typed(Query::distanceGreaterThan('location', [10, 20], 5), ColumnType::Point)]);

        $this->assertSame('ST_Distance("location", ST_GeomFromText(?, 4326)) > ?', $condition->expression);
    }

    private static function typed(Query $query, ColumnType $type): Query
    {
        $query->setAttributeType($type->value);

        return $query;
    }
}
