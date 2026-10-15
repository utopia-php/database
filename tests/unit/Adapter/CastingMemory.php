<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\DateTime;
use Utopia\Database\Document;

final class CastingMemory extends Memory implements Feature\Casting
{
    #[\Override]
    public function castBefore(Document $collection, Document $document): Document
    {
        return $document;
    }

    #[\Override]
    public function castAfter(Document $collection, array $documents): array
    {
        return $documents;
    }

    #[\Override]
    public function castDatetime(string $value): mixed
    {
        return DateTime::setTimezone($value);
    }
}
