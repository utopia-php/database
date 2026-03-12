<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Relationship;

interface Relationships
{
    /**
     * @param Relationship $relationship
     * @return bool
     */
    public function createRelationship(Relationship $relationship): bool;

    /**
     * @param Relationship $relationship
     * @param string|null $newKey
     * @param string|null $newTwoWayKey
     * @return bool
     */
    public function updateRelationship(Relationship $relationship, ?string $newKey = null, ?string $newTwoWayKey = null): bool;

    /**
     * @param Relationship $relationship
     * @return bool
     */
    public function deleteRelationship(Relationship $relationship): bool;
}
