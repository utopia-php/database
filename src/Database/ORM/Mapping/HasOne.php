<?php

namespace Utopia\Database\ORM\Mapping;

use Utopia\Query\Schema\ForeignKeyAction;

#[\Attribute(\Attribute::TARGET_PROPERTY)]
final class HasOne
{
    public function __construct(
        public string $target,
        public string $key = '',
        public string $twoWayKey = '',
        public bool $twoWay = true,
        public ForeignKeyAction $onDelete = ForeignKeyAction::Restrict,
    ) {
    }
}
