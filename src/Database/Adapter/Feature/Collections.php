<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Attribute;
use Utopia\Database\Index;

interface Collections
{
    /**
     * @param string $name
     * @param array<Attribute> $attributes
     * @param array<Index> $indexes
     * @return bool
     */
    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool;

    /**
     * @param string $id
     * @return bool
     */
    public function deleteCollection(string $id): bool;

    /**
     * @param string $collection
     * @return bool
     */
    public function analyzeCollection(string $collection): bool;

    /**
     * @param string $collection
     * @return int
     */
    public function getSizeOfCollection(string $collection): int;

    /**
     * @param string $collection
     * @return int
     */
    public function getSizeOfCollectionOnDisk(string $collection): int;
}
