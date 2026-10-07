<?php

namespace Tests\Unit\Adapter;

use PDO;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;

final class CastCountingSQLite extends SQLite implements Feature\Casting
{
    /**
     * @var list<list<string>> The ids of each batch of collection documents cast after a read
     */
    public array $batches = [];

    public function __construct()
    {
        parent::__construct(new PDO('sqlite::memory:'));
    }

    public function castBefore(Document $collection, Document $document): Document
    {
        return $document;
    }

    public function castAfter(Document $collection, array $documents): array
    {
        if ($collection->getId() !== Database::METADATA) {
            $this->batches[] = \array_values(\array_map(fn (Document $document): string => $document->getId(), $documents));
        }

        return $documents;
    }

    public function castDatetime(string $value): mixed
    {
        return DateTime::setTimezone($value);
    }

    public function reset(): void
    {
        $this->batches = [];
    }
}
