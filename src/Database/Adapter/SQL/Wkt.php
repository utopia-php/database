<?php

namespace Utopia\Database\Adapter\SQL;

use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Validator\Spatial as SpatialValidator;
use Utopia\Query\Schema\ColumnType;

/**
 * The well-known text of a spatial value, which every SQL engine reads alike, so it is built without a connection.
 *
 * @internal
 */
final class Wkt
{
    /**
     * A polygon may be given as one ring.
     *
     * @throws StructureException When the value is not a valid geometry of the type
     * @throws DatabaseException When the type is not spatial
     */
    public static function encode(mixed $value, ColumnType $type): string
    {
        $validator = new SpatialValidator($type->value);
        if (! $validator->isValid($value)) {
            throw new StructureException($validator->getDescription());
        }

        /** @var array<int, mixed> $value */
        switch ($type) {
            case ColumnType::Point:
                /** @var array{0: float|int, 1: float|int} $value */
                return "POINT({$value[0]} {$value[1]})";

            case ColumnType::Linestring:
                /** @var array<int, array{0: float|int, 1: float|int}> $value */
                return 'LINESTRING('.self::points($value).')';

            case ColumnType::Polygon:
                $singleRing = \is_array($value[0] ?? null)
                    && \count($value[0]) === 2
                    && \is_numeric($value[0][0] ?? null)
                    && \is_numeric($value[0][1] ?? null);

                if ($singleRing) {
                    $value = [$value];
                }

                $rings = [];
                /** @var array<int, array<int, array{0: float|int, 1: float|int}>> $value */
                foreach ($value as $ring) {
                    $rings[] = '('.self::points($ring).')';
                }

                return 'POLYGON('.\implode(', ', $rings).')';

            default:
                throw new DatabaseException('Unknown spatial type: '.$type->value);
        }
    }

    /**
     * @param  array<int, array{0: float|int, 1: float|int}>  $points
     */
    private static function points(array $points): string
    {
        $coordinates = [];
        foreach ($points as $point) {
            $coordinates[] = "{$point[0]} {$point[1]}";
        }

        return \implode(', ', $coordinates);
    }
}
