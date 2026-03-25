<?php

namespace Utopia\Database\ORM\Mapping;

use Utopia\Query\Schema\IndexType;

#[\Attribute(\Attribute::TARGET_CLASS | \Attribute::IS_REPEATABLE)]
final class TableIndex
{
    /**
     * @param  array<string>  $attributes
     * @param  array<int|null>  $lengths
     * @param  array<string|null>  $orders
     */
    public function __construct(
        public string $key,
        public IndexType $type = IndexType::Index,
        public array $attributes = [],
        public array $lengths = [],
        public array $orders = [],
    ) {
    }
}
