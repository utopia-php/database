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
     * @param list<Attribute> $attributes The attributes to create.
     * @return bool True on success.
     */
    public function createAttributes(string $collection, array $attributes): bool;

    /**
     * Alter the column stored under $key to match $attribute, renaming it when $attribute->key differs.
     */
    public function updateAttribute(string $collection, string $key, Attribute $attribute): bool;

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
