<?php

namespace Tests\Unit\Adapter;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\DateTime;
use Utopia\Database\Document;

final class CastingMemory extends Memory implements Feature\InternalCasting, Feature\UTCCasting
{
    public function castingBefore(Document $collection, Document $document): Document
    {
        return $document;
    }

    public function castingAfter(Document $collection, Document $document): Document
    {
        return $document;
    }

    public function setUTCDatetime(string $value): mixed
    {
        return DateTime::setTimezone($value);
    }
}
