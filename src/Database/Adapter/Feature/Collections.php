<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Attribute;
use Utopia\Database\Index;

/**
 * Defines collection lifecycle and inspection operations for a database adapter.
 */
interface Collections
{
    /**
     * Create a new collection with optional attributes and indexes.
     *
     * @param string $name The collection name.
     * @param array<Attribute> $attributes Initial attributes for the collection.
     * @param array<Index> $indexes Initial indexes for the collection.
     * @return bool True on success.
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool;

    /**
     * Delete a collection by its identifier.
     *
     * @param string $id The collection identifier.
     * @return bool True on success.
     */
    public function deleteCollection(string $id): bool;

    /**
     * Analyze a collection to update index statistics.
     *
     * @param string $collection The collection identifier.
     * @return bool True on success.
     */
    public function analyzeCollection(string $collection): bool;

    /**
     * Get the logical data size of a collection in bytes.
     *
     * @param string $collection The collection identifier.
     * @return int Size in bytes.
     */
    public function getSizeOfCollection(string $collection): int;

    /**
     * Get the on-disk storage size of a collection in bytes.
     *
     * @param string $collection The collection identifier.
     * @return int Size in bytes.
     */
    public function getSizeOfCollectionOnDisk(string $collection): int;
}
