<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Attribute;
use Utopia\Database\Index;

interface Collections
{
    /**
     * @param  array<Attribute>  $attributes
     * @param  array<Index>  $indexes
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool;

    public function deleteCollection(string $id): bool;

    public function analyzeCollection(string $collection): bool;

    public function getSizeOfCollection(string $collection): int;

    public function getSizeOfCollectionOnDisk(string $collection): int;
}
