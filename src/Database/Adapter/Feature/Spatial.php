<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Query\Schema\ColumnType;

/**
 * Converts spatial values between the coordinate arrays documents carry and the form the engine stores.
 */
interface Spatial
{
    /**
     * Encode a point ([x, y]), linestring ([[x, y], ...]) or polygon ([[[x, y], ...], ...], or one ring) for storage.
     *
     * @throws StructureException When the value is not a valid geometry of the type
     * @throws DatabaseException When the type is not spatial
     */
    public function encode(mixed $value, ColumnType $type): string;

    /**
     * Decode a stored geometry of the type back into coordinates.
     *
     * @return array<mixed> [x, y] for a point, a list of points for a linestring, a list of rings for a polygon
     *
     * @throws DatabaseException When the type is not spatial or the value cannot be decoded
     */
    public function decode(string $value, ColumnType $type): array;
}
