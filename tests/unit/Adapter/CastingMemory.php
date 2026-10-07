<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\DateTime;
use Utopia\Database\Document;

final class CastingMemory extends Memory implements Feature\Casting
{
    public function castBefore(Document $collection, Document $document): Document
    {
        return $document;
    }

    public function castAfter(Document $collection, array $documents): array
    {
        return $documents;
    }

    public function castDatetime(string $value): mixed
    {
        return DateTime::setTimezone($value);
    }
}
