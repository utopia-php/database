<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Attribute;

/**
 * Defines attribute management operations for a database adapter.
 */
interface Attributes
{
    /**
     * Create a new attribute in a collection.
     *
     * @param string $collection The collection identifier.
     * @param Attribute $attribute The attribute to create.
     * @return bool True on success.
     */
    public function createAttribute(string $collection, Attribute $attribute): bool;

    /**
     * Create multiple attributes in a collection at once.
     *
     * @param string $collection The collection identifier.
     * @param array<Attribute> $attributes The attributes to create.
     * @return bool True on success.
     */
    public function createAttributes(string $collection, array $attributes): bool;

    /**
     * Update an existing attribute in a collection.
     *
     * @param string $collection The collection identifier.
     * @param Attribute $attribute The attribute with updated properties.
     * @param string|null $newKey Optional new key to rename the attribute.
     * @return bool True on success.
     */
    public function updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null): bool;

    /**
     * Delete an attribute from a collection.
     *
     * @param string $collection The collection identifier.
     * @param string $id The attribute identifier to delete.
     * @return bool True on success.
     */
    public function deleteAttribute(string $collection, string $id): bool;

    /**
     * Rename an attribute in a collection.
     *
     * @param string $collection The collection identifier.
     * @param string $old The current attribute key.
     * @param string $new The new attribute key.
     * @return bool True on success.
     */
    public function renameAttribute(string $collection, string $old, string $new): bool;
}
