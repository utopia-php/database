<?php

namespace Utopia\Database\Relationship;

use Utopia\Database\RelationshipDeleteAction;

final readonly class Update
{
    public function __construct(
        public ?string $key = null,
        public ?string $twoWayKey = null,
        public ?bool $twoWay = null,
        public ?RelationshipDeleteAction $onDelete = null,
    ) {
    }
}
