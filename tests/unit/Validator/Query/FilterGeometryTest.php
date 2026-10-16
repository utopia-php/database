<?php

namespace Tests\Unit\Validator\Query;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Query;
use Utopia\Database\Validator\Query\Filter;
use Utopia\Database\Validator\Spatial;
use Utopia\Query\Method;
use Utopia\Query\Schema\ColumnType;

/**
 * The geometry of a spatial filter is held to the rules of a stored value of its shape. Filter only asked for an
 * array, so a geometry nested too deep or out of range failed in the engine, and one of strings read as (0, 0).
 */
final class FilterGeometryTest extends TestCase
{
    private const array SQUARE = [[[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]]];

    private function validator(): Filter
    {
        return new Filter(
            attributes: [
                new Document(['$id' => 'pt', 'key' => 'pt', 'type' => ColumnType::Point->value, 'array' => false]),
                new Document(['$id' => 'pg', 'key' => 'pg', 'type' => ColumnType::Polygon->value, 'array' => false]),
            ],
            idAttributeType: ColumnType::Integer->value,
        );
    }

    /**
     * @return iterable<string, array{Query}>
     */
    public static function malformed(): iterable
    {
        yield 'nested too deep' => [Query::intersects('pg', [[[[[1]]]]])];
        yield 'longitude and latitude out of range' => [Query::intersects('pt', [[999, 999]])];
        yield 'latitude out of range' => [Query::intersects('pt', [10, 91])];
        yield 'coordinates that are not numbers' => [Query::intersects('pt', [['a', 'b']])];
        yield 'a point of three coordinates' => [Query::intersects('pt', [1, 2, 3])];
        yield 'a point given as an object' => [Query::intersects('pt', [['x' => 1, 'y' => 2]])];
        yield 'a ring that is not closed' => [Query::intersects('pg', [[[0, 0], [0, 10], [10, 10], [10, 0]]])];
        yield 'a ring of too few points' => [Query::intersects('pg', [[[0, 0], [0, 10], [0, 0]]])];
        yield 'a linestring with a point of strings' => [Query::crosses('pg', [[0, 0], ['a', 'b']])];
        yield 'a distance from a point out of range' => [Query::distanceLessThan('pt', [999, 999], 10)];
        yield 'a distance from a point of strings' => [Query::distanceLessThan('pt', [['a', 'b']], 10)];
        yield 'a distance from text that is not WKT' => [new Query(Method::DistanceLessThan, 'pt', [['nowhere', 10, false]])];
    }

    #[DataProvider('malformed')]
    public function testAMalformedGeometryIsRefused(Query $query): void
    {
        $this->assertFalse($this->validator()->isValid($query));
    }

    /**
     * @return iterable<string, array{Query}>
     */
    public static function wellFormed(): iterable
    {
        yield 'a point' => [Query::intersects('pt', [1, 2])];
        yield 'a wrapped point' => [Query::intersects('pt', [[1, 2]])];
        yield 'a linestring' => [Query::crosses('pg', [[0, 0], [5, 5]])];
        yield 'a polygon' => [Query::intersects('pt', self::SQUARE)];
        yield 'a polygon on a polygon' => [Query::covers('pg', self::SQUARE)];
        yield 'equal to a stored polygon' => [Query::equal('pg', [self::SQUARE])];
        yield 'a distance from a point' => [Query::distanceLessThan('pt', [1, 2], 10, true)];
        yield 'a distance from WKT' => [new Query(Method::DistanceGreaterThan, 'pt', [['POINT(1 2)', 10, false]])];
        yield 'numeric strings' => [Query::intersects('pt', ['1.5', '2'])];
    }

    #[DataProvider('wellFormed')]
    public function testAWellFormedGeometryIsValid(Query $query): void
    {
        $validator = $this->validator();

        $this->assertTrue($validator->isValid($query), $validator->getDescription());
    }

    public function testTheRefusalNamesWhatIsWrong(): void
    {
        $validator = $this->validator();

        $this->assertFalse($validator->isValid(Query::intersects('pt', [[999, 999]])));
        $this->assertSame(
            'Invalid spatial data for attribute "pt": Value must be a valid point: Longitude (x) must be between -180 and 180, got 999',
            $validator->getDescription(),
        );
    }

    /**
     * @return iterable<string, array{string, array<mixed>}>
     */
    public static function storedObjects(): iterable
    {
        yield 'point' => [ColumnType::Point->value, ['x' => 1, 'y' => 2]];
        yield 'linestring point' => [ColumnType::Linestring->value, [[0, 0], ['x' => 1, 'y' => 2]]];
        yield 'polygon ring' => [ColumnType::Polygon->value, [['a' => [0, 0], 'b' => [0, 1], 'c' => [1, 1], 'd' => [0, 0]]]];
        yield 'polygon point' => [ColumnType::Polygon->value, [[[0, 0], ['x' => 0, 'y' => 1], [1, 1], [0, 0]]]];
        yield 'polygon single ring' => [ColumnType::Polygon->value, [['x' => 0, 'y' => 0], [0, 1], [1, 1], [0, 0]]];
    }

    /**
     * @param  array<mixed>  $value
     */
    #[DataProvider('storedObjects')]
    public function testAStoredCoordinateGivenAsAnObjectIsRefused(string $type, array $value): void
    {
        $this->assertFalse((new Spatial($type))->isValid($value));
    }
}
