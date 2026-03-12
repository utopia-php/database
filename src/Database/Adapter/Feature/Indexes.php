<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Index;

interface Indexes
{
    /**
     * @param string $collection
     * @param Index $index
     * @param array<string,string> $indexAttributeTypes
     * @param array<string, mixed> $collation
     * @return bool
     */
    public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool;

    /**
     * @param string $collection
     * @param string $id
     * @return bool
     */
    public function deleteIndex(string $collection, string $id): bool;

    /**
     * @param string $collection
     * @param string $old
     * @param string $new
     * @return bool
     */
    public function renameIndex(string $collection, string $old, string $new): bool;

    /**
     * @return array<string>
     */
    public function getInternalIndexesKeys(): array;
}
