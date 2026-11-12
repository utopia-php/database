<?php

namespace Utopia\Database\Validator;

use Utopia\Query\Schema\ColumnType;
use Utopia\Validator;

/**
 * Validates the geometry a spatial query compares against, by the shape the builders read it as: `[x, y]` and
 * `[[x, y]]` are a point, a deeper list of points a linestring, and a list of rings a polygon. Each is held to the
 * rules a stored value of its type is.
 */
class Geometry extends Validator
{
    private string $message = 'Geometry must be a point [x, y], a list of points or a list of rings of points';

    #[\Override]
    public function getDescription(): string
    {
        return $this->message;
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        if (! \is_array($value) || $value === [] || ! \array_is_list($value)) {
            return false;
        }

        $first = $value[0];
        [$type, $geometry] = match (true) {
            ! \is_array($first) => [ColumnType::Point, $value],
            \is_array($first[0] ?? null) => [ColumnType::Polygon, $value],
            \count($value) === 1 => [ColumnType::Point, $first],
            default => [ColumnType::Linestring, $value],
        };

        $spatial = new Spatial($type->value);
        if (! $spatial->isValid($geometry)) {
            $this->message = $spatial->getDescription();

            return false;
        }

        return true;
    }

    #[\Override]
    public function isArray(): bool
    {
        return false;
    }

    #[\Override]
    public function getType(): string
    {
        return self::TYPE_ARRAY;
    }
}
