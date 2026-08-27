<?php

namespace Utopia\Database\ORM\Mapping;

use Utopia\Query\Schema\ColumnType;

#[\Attribute(\Attribute::TARGET_PROPERTY)]
final class Column
{
    /**
     * @param  array<string, mixed>  $formatOptions
     * @param  array<string>  $filters
     */
    public function __construct(
        public ColumnType $type = ColumnType::String,
        public int $size = 0,
        public bool $required = false,
        public mixed $default = null,
        public bool $signed = true,
        public bool $array = false,
        public ?string $format = null,
        public array $formatOptions = [],
        public array $filters = [],
        public ?string $key = null,
    ) {
    }
}
