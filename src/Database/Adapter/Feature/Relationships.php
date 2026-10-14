<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Relationship;
use Utopia\Database\Relationship\Update;
use Utopia\Database\RelationshipSide;

/**
 * Every relationship is given from the side of $collection, with both keys resolved.
 */
interface Relationships
{
    /**
     * Create the storage of a relationship from its parent side.
     */
    public function createRelationship(string $collection, Relationship $relationship): bool;

    /**
     * Rename the stored keys of a relationship as $update says, from the side of $collection that $side names.
     */
    public function updateRelationship(string $collection, Relationship $relationship, RelationshipSide $side, Update $update): bool;

    /**
     * Drop the storage of a relationship, from the side of $collection that $side names.
     */
    public function deleteRelationship(string $collection, Relationship $relationship, RelationshipSide $side): bool;
}
