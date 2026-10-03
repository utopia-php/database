<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Document;

/**
 * Provides the ability to retrieve the schema-level indexes of a collection.
 */
interface SchemaIndexes
{
    /**
     * Get the schema indexes defined on a collection in the underlying database.
     *
     * @param string $collection The collection identifier.
     * @return array<Document> The index documents describing the schema.
     */
    public function getSchemaIndexes(string $collection): array;
}
