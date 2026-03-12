<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Document;

interface SchemaAttributes
{
    /**
     * @return array<Document>
     */
    public function getSchemaAttributes(string $collection): array;
}
