<?php

namespace Utopia\Database\Adapter\Feature;

use Utopia\Database\Document;

interface InternalCasting
{
    public function castingBefore(Document $collection, Document $document): Document;

    public function castingAfter(Document $collection, Document $document): Document;
}
