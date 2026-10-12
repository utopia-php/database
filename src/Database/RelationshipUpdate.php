<?php

namespace Utopia\Database;

final readonly class RelationshipUpdate
{
    public function __construct(
        public ?string $key = null,
        public ?string $twoWayKey = null,
        public ?bool $twoWay = null,
        public ?RelationshipDeleteAction $onDelete = null,
    ) {
    }
}
