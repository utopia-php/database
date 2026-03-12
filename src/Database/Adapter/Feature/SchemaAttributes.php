<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Document;

interface SchemaAttributes
{
    /**
     * @param string $collection
     * @return array<Document>
     */
    public function getSchemaAttributes(string $collection): array;
}
