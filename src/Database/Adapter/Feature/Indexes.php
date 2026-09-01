<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Index;

/**
 * Defines index management operations for a database adapter.
 */
interface Indexes
{
    /**
     * Create an index on a collection.
     *
     * @param string $collection The collection identifier.
     * @param Index $index The index definition.
     * @param array<string,string> $indexAttributeTypes Mapping of attribute names to their types.
     * @param array<string, mixed> $collation Optional collation settings for the index.
     * @return bool True on success.
     */
    public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool;

    /**
     * Delete an index from a collection.
     *
     * @param string $collection The collection identifier.
     * @param string $id The index identifier.
     * @return bool True on success.
     */
    public function deleteIndex(string $collection, string $id): bool;

    /**
     * Rename an index in a collection.
     *
     * @param string $collection The collection identifier.
     * @param string $old The current index name.
     * @param string $new The new index name.
     * @return bool True on success.
     */
    public function renameIndex(string $collection, string $old, string $new): bool;

    /**
     * Get the keys of all internal indexes used by the adapter.
     *
     * @return array<string> The internal index keys.
     */
    public function getInternalIndexesKeys(): array;
}
