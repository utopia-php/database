<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Document;

/**
 * Provides the ability to retrieve the schema-level attributes of a collection.
 */
interface SchemaAttributes
{
    /**
     * Get the schema attributes defined on a collection in the underlying database.
     *
     * @param string $collection The collection identifier.
     * @return array<Document> The attribute documents describing the schema.
     */
    public function getSchemaAttributes(string $collection): array;
}
