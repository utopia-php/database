<?php

namespace Utopia\Database\ORM;

use Utopia\Database\RelationType;
use Utopia\Query\Schema\ForeignKeyAction;

class RelationshipMapping
{
    public function __construct(
        public readonly string $propertyName,
        public readonly string $documentKey,
        public readonly RelationType $type,
        public readonly string $targetClass,
        public readonly string $twoWayKey,
        public readonly bool $twoWay,
        public readonly ForeignKeyAction $onDelete,
    ) {
    }
}
