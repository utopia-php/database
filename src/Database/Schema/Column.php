<?php

namespace Utopia\Database\Schema;

/**
 * A column as the engine reports it, read back from its catalog. Engine-only columns (internal or spelled in a type
 * no attribute maps to) are columns too, so introspection never goes through Attribute.
 */
final readonly class Column
{
    /**
     * @param  string  $type  The engine's native type, upper case, with its size, precision and sign: VARCHAR(255),
     *                        INT UNSIGNED, DOUBLE PRECISION, GEOMETRY(POINT,4326)
     * @param  int|null  $length  The character length of a character column
     */
    public function __construct(
        public string $name,
        public string $type,
        public ?int $length,
        public bool $nullable,
    ) {
    }
}
