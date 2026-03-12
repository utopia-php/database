<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Change;
use Utopia\Database\Document;

interface Upserts
{
    /**
     * @param  array<Change>  $changes
     * @return array<Document>
     */
    public function upsertDocuments(Document $collection, string $attribute, array $changes): array;
}
