<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Attribute;

interface Attributes
{
    /**
     * @param string $collection
     * @param Attribute $attribute
     * @return bool
     */
    public function createAttribute(string $collection, Attribute $attribute): bool;

    /**
     * @param string $collection
     * @param array<Attribute> $attributes
     * @return bool
     */
    public function createAttributes(string $collection, array $attributes): bool;

    /**
     * @param string $collection
     * @param Attribute $attribute
     * @param string|null $newKey
     * @return bool
     */
    public function updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null): bool;

    /**
     * @param string $collection
     * @param string $id
     * @return bool
     */
    public function deleteAttribute(string $collection, string $id): bool;

    /**
     * @param string $collection
     * @param string $old
     * @param string $new
     * @return bool
     */
    public function renameAttribute(string $collection, string $old, string $new): bool;
}
