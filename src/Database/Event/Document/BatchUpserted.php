<?php

namespace Utopia\Database\Event\Document;

use Utopia\Database\Event;
use Utopia\Database\Event\Domain;

final readonly class BatchUpserted extends Domain
{
    public int $count;

    public function __construct(
        public string $collection,
        public int $created,
        public int $updated,
    ) {
        parent::__construct(Event::DocumentsUpsert);
        $this->count = $created + $updated;
    }
}
