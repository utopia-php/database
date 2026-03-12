<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Relationship;

interface Relationships
{
    public function createRelationship(Relationship $relationship): bool;

    public function updateRelationship(Relationship $relationship, ?string $newKey = null, ?string $newTwoWayKey = null): bool;

    public function deleteRelationship(Relationship $relationship): bool;
}
