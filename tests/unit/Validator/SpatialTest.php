<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Validator\Spatial;
use Utopia\Query\Schema\ColumnType;

class SpatialTest extends TestCase
{
    public function test_valid_point(): void
    {
        $validator = new Spatial(ColumnType::Point->value);

        $this->assertTrue($validator->isValid([10, 20]));
        $this->assertTrue($validator->isValid([0, 0]));
        $this->assertTrue($validator->isValid([-180.0, 90.0]));

        // Invalid cases
        $this->assertFalse($validator->isValid([10])); // Only one coordinate
        $this->assertFalse($validator->isValid([10, 'a'])); // Non-numeric
        $this->assertFalse($validator->isValid([[10, 20]])); // Nested array
    }

    public function test_valid_line_string(): void
    {
        $validator = new Spatial(ColumnType::Linestring->value);

        $this->assertTrue($validator->isValid([[0, 0], [1, 1]]));

        $this->assertTrue($validator->isValid([[10, 10], [20, 20], [30, 30]]));

        // Invalid cases
        $this->assertFalse($validator->isValid([[10, 10]])); // Only one point
        $this->assertFalse($validator->isValid([[10, 10], [20]])); // Malformed point
        $this->assertFalse($validator->isValid([[10, 10], ['x', 'y']])); // Non-numeric
    }

    public function test_valid_polygon(): void
    {
        $validator = new Spatial(ColumnType::Polygon->value);

        // Single ring polygon (closed)
        $this->assertTrue($validator->isValid([
            [0, 0],
            [0, 1],
            [1, 1],
            [1, 0],
            [0, 0],
        ]));

        // Multi-ring polygon
        $this->assertTrue($validator->isValid([
            [   // Outer ring
                [0, 0], [0, 4], [4, 4], [4, 0], [0, 0],
            ],
            [   // Hole
                [1, 1], [1, 2], [2, 2], [2, 1], [1, 1],
            ],
        ]));

        // Invalid polygons
        $this->assertFalse($validator->isValid([])); // Empty
        $this->assertFalse($validator->isValid([
            [0, 0], [1, 1], [2, 2], // Not closed, less than 4 points
        ]));
        $this->assertFalse($validator->isValid([
            [[0, 0], [1, 1], [1, 0]], // Not closed
        ]));
        $this->assertFalse($validator->isValid([
            [[0, 0], [1, 1], [1, 'a'], [0, 0]], // Non-numeric
        ]));
    }

    public function test_wkt_strings(): void
    {
        $this->assertTrue(Spatial::isWktString('POINT(1 2)'));
        $this->assertTrue(Spatial::isWktString('LINESTRING(0 0,1 1)'));
        $this->assertTrue(Spatial::isWktString('POLYGON((0 0,1 0,1 1,0 1,0 0))'));

        $this->assertFalse(Spatial::isWktString('CIRCLE(0 0,1)'));
        $this->assertFalse(Spatial::isWktString('POINT1(1 2)'));
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function malformedWkt(): iterable
    {
        yield 'unclosed point' => ['POINT(1 2'];
        yield 'point of one coordinate' => ['POINT(1)'];
        yield 'point of three coordinates' => ['POINT(1 2 3)'];
        yield 'point of a word' => ['POINT(a 2)'];
        yield 'empty point' => ['POINT EMPTY'];
        yield 'empty parentheses' => ['POINT()'];
        yield 'trailing text' => ['POINT(1 2) junk'];
        yield 'nested point' => ['POINT((1 2))'];
        yield 'linestring with an empty point' => ['LINESTRING(0 0,,1 1)'];
        yield 'polygon without ring parentheses' => ['POLYGON(0 0,1 0,1 1,0 0)'];
        yield 'polygon missing its closing parenthesis' => ['POLYGON((0 0,1 0,1 1,0 0)'];
        yield 'polygon with a dangling comma' => ['POLYGON((0 0,1 0,1 1,0 0),)'];
        yield 'polygon with rings not separated' => ['POLYGON((0 0,1 0,1 1,0 0)(0 0,1 0,1 1,0 0))'];
        yield 'unknown type' => ['CIRCLE(0 0,1)'];
        yield 'prefixed type' => ['POINT1(1 2)'];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('malformedWkt')]
    public function test_malformed_wkt_is_refused(string $wkt): void
    {
        $this->assertFalse(Spatial::isWktString($wkt));
        $this->assertNull(Spatial::parseWkt($wkt));
        foreach ([ColumnType::Point, ColumnType::Linestring, ColumnType::Polygon] as $type) {
            $this->assertFalse((new Spatial($type->value))->isValid($wkt), $type->value);
        }
    }

    public function test_wkt_is_parsed_into_the_coordinates_of_its_type(): void
    {
        $this->assertSame([ColumnType::Point, [1.0, -2.5]], Spatial::parseWkt(" point ( 1\t-2.5 ) "));
        $this->assertSame([ColumnType::Linestring, [[0.0, 0.0], [1.0, 1.0]]], Spatial::parseWkt('LINESTRING(0 0, 1 1)'));
        $this->assertSame(
            [ColumnType::Polygon, [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]], [[0.1, 0.1], [0.2, 0.1], [0.2, 0.2], [0.1, 0.1]]]],
            Spatial::parseWkt('POLYGON((0 0, 1 0, 1 1, 0 0), (0.1 0.1, 0.2 0.1, 0.2 0.2, 0.1 0.1))'),
        );
    }

    public function test_wkt_is_held_to_the_rules_of_its_type(): void
    {
        $point = new Spatial(ColumnType::Point->value);
        $this->assertTrue($point->isValid('POINT(180 -90)'));
        $this->assertTrue($point->isValid('POINT(-180 90)'));
        $this->assertFalse($point->isValid('POINT(180.0001 0)'));
        $this->assertFalse($point->isValid('POINT(0 -90.0001)'));
        $this->assertFalse($point->isValid('POINT(1e308 0)'));
        $this->assertFalse($point->isValid('LINESTRING(0 0, 1 1)'));
        $this->assertStringContainsString('WKT must be a POINT, got a LINESTRING', $point->getDescription());

        $this->assertFalse((new Spatial(ColumnType::Linestring->value))->isValid('LINESTRING(0 0)'));
        $this->assertFalse((new Spatial(ColumnType::Polygon->value))->isValid('POLYGON((0 0, 1 0, 1 1, 1 0))'));
        $this->assertFalse((new Spatial(ColumnType::Polygon->value))->isValid('POLYGON((0 0, 1 0, 0 0))'));
    }

    public function test_coordinates_that_are_not_finite_are_refused(): void
    {
        $point = new Spatial(ColumnType::Point->value);
        foreach ([[\INF, 0], [0, -\INF], [\NAN, 0], [0, \NAN]] as $coordinates) {
            $this->assertFalse($point->isValid($coordinates));
            $this->assertStringContainsString('finite', $point->getDescription());
        }
    }

    public function test_invalid_coordinate(): void
    {
        // Point with invalid longitude
        $validator = new Spatial(ColumnType::Point->value);
        $this->assertFalse($validator->isValid([200, 10])); // longitude > 180
        $this->assertStringContainsString('Longitude', $validator->getDescription());

        // Point with invalid latitude
        $validator = new Spatial(ColumnType::Point->value);
        $this->assertFalse($validator->isValid([10, -100])); // latitude < -90
        $this->assertStringContainsString('Latitude', $validator->getDescription());

        // LineString with invalid coordinates
        $validator = new Spatial(ColumnType::Linestring->value);
        $this->assertFalse($validator->isValid([
            [0, 0],
            [181, 45], // invalid longitude
        ]));
        $this->assertStringContainsString('Invalid coordinates', $validator->getDescription());

        // Polygon with invalid coordinates
        $validator = new Spatial(ColumnType::Polygon->value);
        $this->assertFalse($validator->isValid([
            [[0, 0], [1, 1], [190, 5], [0, 0]], // invalid longitude in ring
        ]));
        $this->assertStringContainsString('Invalid coordinates', $validator->getDescription());
    }
}
