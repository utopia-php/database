<?php

namespace Tests\Unit\Adapter;

use PDO;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;

final class CastCountingSQLite extends SQLite implements Feature\InternalCasting, Feature\UTCCasting
{
    public int $singles = 0;

    /**
     * @var list<list<string>>
     */
    public array $batches = [];

    public function __construct()
    {
        parent::__construct(new PDO('sqlite::memory:'));
    }

    public function castingBefore(Document $collection, Document $document): Document
    {
        return $document;
    }

    public function castingAfter(Document $collection, Document $document): Document
    {
        if ($collection->getId() !== Database::METADATA) {
            $this->singles++;
        }

        return $document;
    }

    public function castingAfterDocuments(Document $collection, array $documents): array
    {
        $this->batches[] = \array_values(\array_map(fn (Document $document): string => $document->getId(), $documents));

        return $documents;
    }

    public function setUTCDatetime(string $value): mixed
    {
        return DateTime::setTimezone($value);
    }

    public function reset(): void
    {
        $this->singles = 0;
        $this->batches = [];
    }
}
