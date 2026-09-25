<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Relationship;

/**
 * Defines relationship management operations for a database adapter.
 */
interface Relationships
{
    /**
     * Create a relationship between collections.
     *
     * @param Relationship $relationship The relationship definition.
     * @return bool True on success.
     */
    public function createRelationship(Relationship $relationship): bool;

    /**
     * Update an existing relationship, optionally renaming its keys.
     *
     * @param Relationship $relationship The relationship with updated properties.
     * @param string|null $newKey Optional new key for the relationship.
     * @param string|null $newTwoWayKey Optional new key for the inverse side.
     * @return bool True on success.
     */
    public function updateRelationship(Relationship $relationship, ?string $newKey = null, ?string $newTwoWayKey = null): bool;

    /**
     * Delete a relationship between collections.
     *
     * @param Relationship $relationship The relationship to delete.
     * @return bool True on success.
     */
    public function deleteRelationship(Relationship $relationship): bool;
}
