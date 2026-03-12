<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Index;

interface Indexes
{
    /**
     * @param  array<string,string>  $indexAttributeTypes
     * @param  array<string, mixed>  $collation
     */
    public function createIndex(string $collection, Index $index, array $indexAttributeTypes = [], array $collation = []): bool;

    public function deleteIndex(string $collection, string $id): bool;

    public function renameIndex(string $collection, string $old, string $new): bool;

    /**
     * @return array<string>
     */
    public function getInternalIndexesKeys(): array;
}
