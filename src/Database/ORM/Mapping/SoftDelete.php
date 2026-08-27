<?php

namespace Utopia\Database\ORM\Mapping;

use Utopia\Query\Schema\ColumnType;

#[\Attribute(\Attribute::TARGET_CLASS)]
final class SoftDelete
{
    public function __construct(
        public string $column = 'deletedAt',
        public ColumnType $type = ColumnType::Datetime,
    ) {
    }
}
