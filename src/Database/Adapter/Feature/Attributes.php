<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Attribute;

interface Attributes
{
    public function createAttribute(string $collection, Attribute $attribute): bool;

    /**
     * @param  array<Attribute>  $attributes
     */
    public function createAttributes(string $collection, array $attributes): bool;

    public function updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null): bool;

    public function deleteAttribute(string $collection, string $id): bool;

    public function renameAttribute(string $collection, string $old, string $new): bool;
}
